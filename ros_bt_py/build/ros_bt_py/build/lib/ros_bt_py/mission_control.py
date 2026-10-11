# Copyright 2018-2023 FZI Forschungszentrum Informatik
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the FZI Forschungszentrum Informatik nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""
Module used to implement mission control components.

This includes the management of capabilities and their implementations as well as the forwarding to
other nodes by using an auction protocol.
"""

# pylint: disable=no-name-in-module,import-error

import dataclasses
import json
import threading
from threading import RLock
from typing import Dict, List, Tuple, Optional
import queue

import rclpy
from rclpy.node import Node
import rclpy.time
import std_msgs.msg

from builtin_interfaces.msg import Time
from rclpy.clock import Clock

import rosidl_runtime_py
import rosidl_runtime_py.utilities

from rclpy.action.server import ServerGoalHandle
from rclpy.task import Future
from rclpy.client import Client
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.publisher import Publisher
from rclpy.action import ActionServer
from rclpy.duration import Duration
from actionlib_msgs.msg import GoalStatus
from rclpy.exceptions import ROSInterruptException
from action_msgs.msg import GoalInfo
from unique_identifier_msgs.msg import UUID
from actionlib_msgs.msg import GoalID

from ros_bt_py_interfaces.msg import (
    CapabilityImplementation,
    Precondition,
    RemoteCapabilitySlotStatus,
    CapabilityExecutionStatus,
    Tree,
)
from ros_bt_py_interfaces.action import (
    ExecuteRemoteCapability,
)
from ros_bt_py_interfaces.srv import (
    GetCapabilityImplementations,
    LoadTree,
    PrepareLocalImplementation,
    AddNode,
    MoveNode,
    CheckPreconditionStatus,
    ClearTree,
    MoveNode,
    RequestCapabilityExecution,
    GetLocalBid,
    FindBestCapabilityExecutor,
    MigrateTree,
    RunRemoteCapabilitySlot,
    CancelRemoteCapabilitySlot,
    GetAvailableRemoteCapabilitySlots,
    ReserveRemoteCapabilitySlot
)

from ros_bt_py.exceptions import BehaviorTreeException
from ros_bt_py.nodes.sequence import Sequence
from ros_bt_py.capability import set_capability_io_bridge_id
from ros_bt_py.debug_manager import DebugManager
from ros_bt_py.helpers import HashableCapabilityInterface, json_decode
from ros_bt_py.migration import MigrationManager, check_node_versions
from ros_bt_py.ros_helpers import AsyncServiceProxy
from ros_bt_py.tree_manager import TreeManager

@dataclasses.dataclass
class RemoteCapabilitySlotStatusData:
    """Data class holding all information about the remote capability slots current status."""

    status: str = RemoteCapabilitySlotStatus.IDLE
    reserved: bool = False
    timestamp: Optional[Time] = None
    reauction_threshold: float = float("inf")
    implementation_name: str = ""
    remote_mission_control_topic: str = ""

@dataclasses.dataclass
class RemoteCapabilityGoalStatus:
    """Represents the status of a remote capability execution running in a local slot."""

    status: str
    cancellation_requested: bool = False

class MissionControl():
    """
    Class to manage the capability implementations and interfaces on the local node.

    Additionally, it allows to exchange interface definitions with remote nodes.
    """

    ##############以下所有！！srv_name！！！（也就是～/）都改过，改之前只有～，但编译提示～后面必须有/，修改集中在158-216行

    # pylint: disable=too-many-instance-attributes

    def __init__(self,node : Node):
        """
        Create a new capability repository.

        The repository has to major interfaces, it communicates with other repositories and
        offers services geared towards its local subscribers.
        """
        self.node = node
        self.__capability_execution_status: Dict[
            HashableCapabilityInterface, Dict[str, int]
        ] = {}
        self.capability_repository_topic = (
            f"{self.node.get_namespace()}/capability_repository"
        )

        capability_repository_topic = f"{self.node.get_namespace()}/capability_repository/capabilities/implementations/get"

        self.__get_capability_implementations_proxy = self.node.create_client(
            GetCapabilityImplementations, capability_repository_topic
        )
        self.__get_capability_implementations_proxy_lock = RLock()

        self.__request_capability_execution_service: Service = self.node.create_service(
            srv_type=RequestCapabilityExecution,
            srv_name="~/execute_capability",
            callback=self.request_capability_execution
        )

        self.__get_local_bid_service: Service = self.node.create_service(
             GetLocalBid, "~/get_local_bid", self.get_local_bid
        )
 
        self.__get_local_bid_service_client = self.node.create_client(
            GetLocalBid,
            "~/get_local_bid",
        )

        self.__check_precondition_status_service: Service = self.node.create_service(
            CheckPreconditionStatus,
            "~/check_precondition_status",
            callback=self.check_precondition_status,#handler=self.check_precondition_status,
        )

        self.__prepare_local_implementation_service = self.node.create_service(
            PrepareLocalImplementation,
            "~/prepare_local_implementation",
            self.prepare_local_capability_implementation,
        )
        self.__prepare_local_implementation_service_lock = RLock()

        self._capability_execution_status_subscriber = self.node.create_subscription(
            msg_type=CapabilityExecutionStatus,
            topic="~/notify_capability_execution_status",
            callback=self.notify_capability_execution_status,
            qos_profile=10,
        )

        self.__execute_remote_capability_action_server = ActionServer(
            self.node,  ###add
            action_type=ExecuteRemoteCapability,
            action_name="~/execute_remote_capability",
            execute_callback=self.execute_remote_capability_exec,
            goal_callback=self.execute_remote_capability_goal_cb,
            cancel_callback=self.execute_remote_capability_cancel_cb,
        )

        self._local_remote_slots_status_subscriber = self.node.create_subscription(
            RemoteCapabilitySlotStatus,
            "~/remote_slot_status",
            callback=self.remote_capability_slot_status_callback,
            qos_profile=10,
        )

        self.__available_remote_capability_slots_service = self.node.create_service(
            GetAvailableRemoteCapabilitySlots,
            "~/get_available_remote_slots",
            self.get_available_remote_capability_slots,
        )

        self.__reserve_remote_capability_slot_service = self.node.create_service(
            ReserveRemoteCapabilitySlot,
            "~/reserve_remote_capability_slot",
            self.reserve_remote_capability_executor,
        )

        self._remote_capability_slot_lock = RLock()
        self._get_local_bid_lock = RLock()

        self._remote_capability_slot_status: Dict[
            str, RemoteCapabilitySlotStatusData
        ] = {}
        """
        Status of all known remote capability slots.
        It stores the name of the executor together with the current status.
        """
        self._remote_capability_slot_goals: Dict[str, RemoteCapabilityGoalStatus] = {}

        # 获取参数并转换为 Duration
        remote_capability_slot_reservation_duration_sec = 5.0#self.node.get_parameter("remote_capability_slot_reservation_duration_sec").value
        #rospy.get_param("~remote_capability_slot_reservation_duration_sec", 5.0)

        self._remote_capability_slot_reservation_duration = Duration(seconds=remote_capability_slot_reservation_duration_sec)

        # 获取队列主题名称
        team_join_topic = self.node.get_parameter("mission_control_join_topic").value if self.node.has_parameter("mission_control_join_topic") else "/team_join"

    
        self.__team_join_publisher: Publisher = self.node.create_publisher(Time, team_join_topic, qos_profile=10,)#queue_size=1)

        self.__team_join_subscriber = self.node.create_subscription(Time, team_join_topic, self.team_join_callback, qos_profile=10,)

        time_msg=self.node.get_clock().now().to_msg()  ####add
        self.__team_join_publisher.publish(Time(sec=time_msg.sec,nanosec=time_msg.nanosec)) ###add
        #self.__team_join_publisher.publish(Time(data=self.node.get_clock().now().to_msg()))

        self.result_queue = queue.Queue()

    def team_join_callback(self, msg: Time) -> None:
        """
        Handle team composition changes as callback.

        :param msg: Timestamp when the team composition change occurred.
        :return: None
        """
        with self._remote_capability_slot_lock:
            for goal_id in self._remote_capability_slot_goals:
                self.node.get_logger().debug(
                    f"Team composition has changed at {msg}. Canceling goal {goal_id}!"
                )
                self._remote_capability_slot_goals[
                    goal_id
                ].cancellation_requested = True

    def shutdown(self):
        """
        Shut down all the services and pub/sub of this node.

        :return: None
        """
        time_msg=self.node.get_clock().now().to_msg()  ####add
        self.__team_join_publisher.publish(time_msg)#(Time(sec=time_msg.sec,nanosec=time_msg.nanosec)) ###add
        #self.__team_join_publisher.publish(Time(data=self.node.get_clock().now().to_msg()))
        self.__team_join_subscriber.destroy()
        self.__team_join_publisher.destroy()
        self.__execute_remote_capability_action_server.destroy()
        self.__request_capability_execution_service.destroy()
        self.__get_local_bid_service.destroy()
        self.__get_local_bid_service_client.destroy()
        self.__check_precondition_status_service.destroy()
        self.__prepare_local_implementation_service.destroy()

    @staticmethod
    def __handle_async_service_call(service_proxy: AsyncServiceProxy):
        """
        Manage a call to a service via the AsyncServiceProxy.

        The call method needs to be called beforehand, this method just handles
        the possible states until the result is received.
        It checks the states the service_proxy can be in and returns results
        depending on the current execution state.
        This method in nonblocking.

        :param service_proxy: The service proxy the call has been placed for.

        :raises RuntimeError: Should the call to the AsyncServiceProxy be
        aborted or not placed before calling this method a BehaviorTreeException will be raised.
        :return: The response of the call if available, otherwise None.
        """
        state = service_proxy.get_state()

        if state in [AsyncServiceProxy.IDLE, AsyncServiceProxy.SERVICE_AVAILABLE]:
            return None

        if state is AsyncServiceProxy.TIMEOUT:
            raise RuntimeError(
                f"Service call timed out before succeeding" f" (state ID: {state})"
            )

        if state is AsyncServiceProxy.ABORTED:
            raise RuntimeError(
                f"Service call was aborted before succeeding" f" (state ID: {state})"
            )

        if state is AsyncServiceProxy.ERROR:
            raise RuntimeError(
                f"Service call returned a error before succeeding"
                f" (state ID: {state})"
            )

        if state is AsyncServiceProxy.RUNNING:
            return None

        if state is AsyncServiceProxy.RESPONSE_READY:
            response: RunRemoteCapabilitySlot.Response = service_proxy.get_response()
            if response.success:
                return response
            raise RuntimeError(f"Service call failed {response.error_message})")
        raise RuntimeError(f"Async proxy in invalid state {state})")
    
    def get_local_bid(self, request: GetLocalBid.Request) -> GetLocalBid.Response:
        """
        Calculate the bid for the local node to perform a certain action as a callback.

        This method receives all the local capability implementations, and loads them into the
        staging tree manager.
        Then the local bid is calculated for each of the implementations using
        the calculate_utility function.

        :param request: The actionlib goal containing the information about
        the required interface for which the bids should be calculated
        :return: None
        """
        # pylint: disable=too-many-statements,too-many-locals, too-many-return-statements
        service_response = GetLocalBid.Response()

        if request.node_id == "":
            service_response.success = False
            service_response.error_message = (
                "GetLocalBid failed as the node_id is not populated!"
            )
            self.node.get_logger().error(service_response.error_message)
            return service_response

        try:
            tags: Dict[str, str] = json_decode(request.implementation_tags_dict)
        except json.decoder.JSONDecodeError as exc:
            service_response.error_message = f"Failed to load tasks: {exc}"
            self.node.get_logger().warn(service_response.error_message)
            service_response.success = False
            return service_response

        with self.__get_capability_implementations_proxy_lock:
            try:
                self.__get_capability_implementations_proxy.wait_for_service(
                    timeout=1.0)
            except Exception:
                service_response.error_message = (
                    "Failed to find local implementation service"
                )
                self.node.get_logger().warn(service_response.error_message)
                service_response.success = False

                return service_response

            try:
                response: GetCapabilityImplementations.Response = (
                    self.__get_capability_implementations_proxy.call(
                        GetCapabilityImplementations.Request(interface=request.interface)
                    )
                )
            except (ROSInterruptException, Exception) as exc:
                service_response.error_message = (
                    f"Failed to get local implementations: {exc}"
                )
                self.node.get_logger().warn(service_response.error_message)
                service_response.success = False
                return service_response

        if not response.success:
            service_response.error_message = (
                f"Failed to get local implementations: {response.error_message}"
            )
            self.node.get_logger().warn(service_response.error_message)
            service_response.success = response.success
            return service_response

        valid_implementations: List[CapabilityImplementation] = []
        
        for p in response.implementations:
            implementation: CapabilityImplementation = p
            impl_tags: Dict[str, str] = json_decode(implementation.tags_dict)
            self.node.get_logger().error(f"Impl tags: {impl_tags}")

            shared_tags = {
                k: tags[k] for k in tags if k in impl_tags and tags[k] == impl_tags[k]
            }
            self.node.get_logger().erorr(f"Shared tags: {shared_tags}")
            if len(shared_tags) < len(impl_tags):
                self.node.get_logger().warn(
                    "Requested interface does not match with all tags of "
                    f"{implementation.name} with tags {impl_tags}"
                )
                continue
            else:
                valid_implementations.append(implementation)

        if len(valid_implementations) < 1:
            service_response.success = False
            service_response.error_message = "No suitable implementation found!"
            return service_response

        get_local_bid_tree_manager = TreeManager(
            name="LocalBidStagingManager",
            publish_tree_callback=nop,
            publish_debug_info_callback=nop,
            publish_debug_settings_callback=nop,
            publish_node_diagnostics_callback=nop,
            debug_manager=DebugManager(),
            simulate_tick=True,
            succeed_always=False,
        )
        get_local_bid_migration_manager = MigrationManager(
            rosnode=self.node,
            tree_manager=get_local_bid_tree_manager
        )

        implementation_utility: Dict[str, float] = {}
        for implementation in valid_implementations:
            tree = implementation.tree

            migration_request = MigrateTree.Request(tree=tree)
            migration_response = check_node_versions(migration_request)

            if migration_response.migrated:
                migration_response = get_local_bid_migration_manager.migrate_tree(
                    migration_request
                )
                if migration_response.success:
                    tree = migration_response.tree

            res = get_local_bid_tree_manager.load_tree(LoadTree.Request(tree=tree))
            if not res.success:
                self.node.get_logger().error(
                    "Failed to load implementation for calculating the local bid: "
                    f"{res.error_message}"
                )
                continue

            try:
                set_capability_io_bridge_id(
                    tree_manager=get_local_bid_tree_manager,
                    interface=request.interface,
                    io_bridge_id=request.node_id,
                )

                get_local_bid_tree_manager.find_root().setup()
                get_local_bid_tree_manager.find_root().tick()

                calculated_utility = (
                    get_local_bid_tree_manager.find_root().calculate_utility()
                )
            except (BehaviorTreeException, Exception) as exc:
                self.node.get_logger().error(f"Failed to calculate utility value from tree: {exc}")
                continue

            if calculated_utility.lower_bound_success == 0:
                implementation_utility[implementation.name] = (
                    calculated_utility.upper_bound_failure
                    - calculated_utility.lower_bound_failure
                )
            else:
                implementation_utility[implementation.name] = (
                    calculated_utility.lower_bound_success
                    + (
                        calculated_utility.upper_bound_failure
                        - calculated_utility.lower_bound_failure
                    )
                    / calculated_utility.lower_bound_success
                )

            try:
                get_local_bid_tree_manager.find_root().shutdown()
                get_local_bid_tree_manager.clear(ClearTree.Request())
            except BehaviorTreeException as exc:
                self.node.get_logger().error(f"Failed to shutdown and clear the tree: {exc}")
                continue

        if len(implementation_utility) < 1:
            service_response.error_message = (
                "No utilities could be calculated for any available implementation."
            )
            service_response.success = False
            self.node.get_logger().warn(service_response.error_message)
            return service_response

        sorted_implementation_bids: List[Tuple[str, float]] = sorted(
            list(implementation_utility.items()), key=lambda v: v[1]
        )

        (
            best_implementation_name,
            best_implementation_bid,
        ) = sorted_implementation_bids[0]
        service_response.bid = best_implementation_bid
        service_response.implementation_name = best_implementation_name
        service_response.success = True
        return service_response
    
    def remote_capability_slot_status_callback(self, msg: RemoteCapabilitySlotStatus):
        """
        Update the status of remote capability slots as subscriber.

        :param msg: The new slot status.
        :return: None
        """
        with self._remote_capability_slot_lock:
            try:
                self._remote_capability_slot_status[msg.name]
            except KeyError:
                self._remote_capability_slot_status[
                    msg.name
                ] = RemoteCapabilitySlotStatusData(
                    status=msg.IDLE, timestamp=self.node.get_clock().now(), reserved=False #####Clock.now()#RemoteCapabilitySlotStatus.IDLE可用，这里属于编辑器问题
                )
            self._remote_capability_slot_status[msg.name].status = msg.status

            if self._remote_capability_slot_status[msg.name].reserved:
                timestamp = self._remote_capability_slot_status[msg.name].timestamp
                if (
                    self.node.get_clock().now() ####Clock.now()
                    > timestamp + self._remote_capability_slot_reservation_duration
                ):
                    self._remote_capability_slot_status[msg.name].reserved = False
                    
    def reserve_remote_capability_executor(
        self, request: ReserveRemoteCapabilitySlot.Request
    ) -> ReserveRemoteCapabilitySlot.Response:
        """
        Reserve a remote capability slot in the local tree for a remote execution.

        :param request: Contains the information on the reservation.
        :return: None
        """
        response = ReserveRemoteCapabilitySlot.Response()

        with self._remote_capability_slot_lock:
            idle_executor_ids: List[str] = list(
                filter(
                    lambda x: (
                        lambda status_data: status_data.status
                        == RemoteCapabilitySlotStatus.IDLE
                        and not (
                            status_data.reserved
                            and self.node.get_clock().now() ####Clock.now()
                            < status_data.timestamp
                            + self._remote_capability_slot_reservation_duration
                        )
                    )(self._remote_capability_slot_status[x]),
                    self._remote_capability_slot_status,
                )
            )

        if len(idle_executor_ids) < 1:
            self.node.get_logger().error(
                "No idle remote_capability_slot found, cannot reserve remote capability slot.",
                logger_name="remote_capability_execution",
            )
            response.success = False
            response.error = "No idle remote_capability_slot found!"

            return response

        reserved_remote_capability_slot = idle_executor_ids.pop()
        # rospy.logdebug(
        #     f"Reserving slot {reserved_remote_capability_slot} for "
        #     f"{request.implementation_name} {request.reauction_threshold}"
        # )
        with self._remote_capability_slot_lock:

            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].reserved = True
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].timestamp = self.node.get_clock().now() ####Clock.now()
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].remote_mission_control_topic = request.remote_mission_control
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].reauction_threshold = request.reauction_threshold
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].implementation_name = request.implementation_name

        response.success = True
        return response
    
    def execute_remote_capability_exec(
        self,
        remote_slot_name: str,
        goal_handle: ServerGoalHandle,#在 ROS2 Action 服务器中，goal_handle 是由 ActionServer 回调传入的。通常，它用于更新目标的状态或返回结果。
        reauction_threshold: float,
    ):
        """
        Control the execution of a remote capability in a local slot.

        This method is intended to run in a separate thread to allow multiple parallel executions.
        :param remote_slot_name: The remote slot name where the capability should be executed.
        This must be a local capability slot.
        :param goal_handle: The goal definition containing all the information about the execution.
        :param reauction_threshold: Threshold when a reauction should occur.
        :return: None
        """
        goal_info: GoalInfo = goal_handle.goal_id()
        goal_id = str(goal_info.goal_id)

        goal: ExecuteRemoteCapability.Goal = goal_handle.request()

        prepare_local_capability_implementation_service_proxy = self.node.create_client(
            PrepareLocalImplementation,
            "~prepare_local_implementation"
        )

        response: PrepareLocalImplementation.Response = prepare_local_capability_implementation_service_proxy.call(
                                                            PrepareLocalImplementation.Request(
                                                                interface=goal.interface,
                                                                implementation_name=goal.implementation_name,
                                                            )
                                                        )
        
        if not response.success:
            self.node.get_logger().warn(
                f"Could not prepare local implementation for: {response.error_message}",
                logger_name="remote_capability_execution",
            )
            
            result=ExecuteRemoteCapability.Result(
                success=False, error_message=response.error_message
            )
            self.result_queue.put(result)
            return result

        run_remote_capability_slot_topic = self.node.get_namespace() + f"/remote_capability_slot/{remote_slot_name}/run"
        

        run_remote_capability_slot_service_proxy = AsyncServiceProxy(run_remote_capability_slot_topic, RunRemoteCapabilitySlot)

        cancel_remote_capability_slot_topic = self.node.get_namespace() + f"/remote_capability_slot/{remote_slot_name}/cancel"

        cancel_remote_capability_slot_service_proxy = self.node.create_client(cancel_remote_capability_slot_topic, CancelRemoteCapabilitySlot)

        try:
            run_remote_capability_slot_service_proxy.wait_for_service(
                timeout=Duration(seconds=5)
            )
        except Exception:
            self.node.get_logger().warn(
                "Could not contact remote_capability slot before timeout",
                logger_name="remote_capability_execution",
            )
            
            result=ExecuteRemoteCapability.Result(
                success=False,
                error_message="Could not contact remote_capability slot before timeout",
            )
            return result

        self.node.get_logger().info(
            f"Send goal to {run_remote_capability_slot_topic}",
            logger_name="remote_capability_execution",
        )
        run_remote_capability_slot_service_proxy.call_service(
            RunRemoteCapabilitySlot.Request(
                implementation_tree=response.implementation_subtree,
                interface=goal.interface,
                node_id=goal.node_id,
            )
        )
        try:
            response_run: Optional[
                RunRemoteCapabilitySlot.Response
            ] = self.__handle_async_service_call(
                run_remote_capability_slot_service_proxy
            )
        except RuntimeError as exc:
            self.node.get_logger().error(
                f"Failed to execute remote capability: {exc}",
                logger_name="remote_capability_execution",
            )
            result=ExecuteRemoteCapability.Result(
                success=False,
                error_message="Failed to execute remote capability",
            )
            return result

        expended_costs = 0

        local_bid_response = self.get_local_bid(
            GetLocalBid.Request(interface=goal.interface, node_id=goal.node_id)
        )
        if local_bid_response.success:
            previous_cost_projection = local_bid_response.bid
        else:
            previous_cost_projection = reauction_threshold * 0.8

        changed_reauction_threshold = reauction_threshold - previous_cost_projection

        while response_run is None:

            local_bid_response = self.get_local_bid(
                GetLocalBid.Request(interface=goal.interface, node_id=goal.node_id)
            )
            if local_bid_response.success:
                expended_costs += abs(previous_cost_projection - local_bid_response.bid)
                previous_cost_projection = local_bid_response.bid
                if expended_costs > changed_reauction_threshold:
                    try:
                        cancel_remote_capability_slot_service_proxy.call(
                            CancelRemoteCapabilitySlot.Request()
                        )
                    except Exception as exc:
                        self.node.get_logger().error(
                            f"Failed to cancel the remote capability slot execution: {exc}"
                        )
                    run_remote_capability_slot_service_proxy.stop_call()
                    goal_handle.canceled(text="Reauction started!")

                    result=ExecuteRemoteCapability.Result(
                        success=False,
                        error_message="Failed to cancel the remote capability slot execution",
                    )
                    return result

            if (
                self._remote_capability_slot_goals[goal_id].cancellation_requested
                or rclpy.ok()
            ):
                self.node.get_logger().info(
                    f"Cancellation of goal {goal_id} requested!",
                    logger_name="remote_capability_execution",
                )
                try:
                    cancel_remote_capability_slot_service_proxy.call(
                        CancelRemoteCapabilitySlot.Request()
                    )
                except Exception as exc:
                    self.node.get_logger().error(
                        f"Failed to cancel the remote capability slot execution: {exc}"
                    )

                run_remote_capability_slot_service_proxy.stop_call()
                goal_handle.canceled(text="Cancellation of goal requested!")
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        error_message="Failed to cancel the remote capability slot execution",
                    )
                return result

            try:
                response_run: Optional[
                    RunRemoteCapabilitySlot.Response
                ] = self.__handle_async_service_call(
                    run_remote_capability_slot_service_proxy
                )
            except RuntimeError as exc:
                self.node.get_logger().error(
                    f"Failed to execute remote capability: {exc}",
                    logger_name="remote_capability_execution",
                )
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        error_message="Failed to execute remote capability",
                    )
                return result

        result=ExecuteRemoteCapability.Result(
                success=response_run.success, error_message=response_run.error_message
            )
        del self._remote_capability_slot_goals[goal_id]
        return result

    def execute_remote_capability_goal_cb(self, goal_handle: ServerGoalHandle) -> None:
        """
        Execute remote capability action goal callback.

        This callback manages the arrival of a new goal to the action server.

        It will look for a new remote capability slot where the implementation
        in the goal can be executed. If an executor is found, a new thread will be started
        that processes the goal.

        :param goal_handle: The goal handle that contains all the necessary
        information for the new request.
        :return: None
        """
        goal_info: GoalInfo = goal_handle.goal_id()
        goal_id = str(goal_info.goal_id)
        goal_stamp = goal_info.stamp
        goal: ExecuteRemoteCapability.Goal = goal_handle.request()

        with self._remote_capability_slot_lock:
            idle_executor_ids: List[str] = list(
                filter(
                    lambda x: self._remote_capability_slot_status[x].status
                    == RemoteCapabilitySlotStatus.IDLE,
                    self._remote_capability_slot_status,
                )
            )
            reserved_executors = list(
                filter(
                    lambda x: (
                        lambda slot_status: slot_status.reserved
                        and slot_status.implementation_name == goal.implementation_name
                    )(self._remote_capability_slot_status[x]),
                    idle_executor_ids,
                )
            )

            # rospy.logdebug(f"Reserved executors: {reserved_executors}")

            if len(reserved_executors) >= 1:
                selected_executor_id = reserved_executors.pop()
                self._remote_capability_slot_status[
                    selected_executor_id
                ].reserved = False
                reauction_threshold = self._remote_capability_slot_status[
                    selected_executor_id
                ].reauction_threshold
                self._remote_capability_slot_status[
                    selected_executor_id
                ].reauction_threshold = float("inf")
            else:
                self.node.get_logger().error(
                    "No reserved remote_capability_slot found, cannot execute remote capability.",
                    logger_name="remote_capability_execution",
                )
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        no_executor_available=True,
                        error_message="No reserved remote capability slot executor available!",
                    )
                # goal_ids: GoalID = GoalStatus.goal_id()
                # goal_ids.id = goal_id
                # goal_ids.stamp = goal_stamp
                # self.__execute_remote_capability_action_server.publish_result(
                #     status=GoalStatus(
                #         goal_id=goal_ids, status=GoalStatus.SUCCEEDED
                #     ),
                #     result=ExecuteRemoteCapability.Result(
                #         success=False,
                #         no_executor_available=True,
                #         error_message="No reserved remote capability slot executor available!",
                #     ),
                # )
                return result

            self.remote_capability_slot_status_callback(
                RemoteCapabilitySlotStatus(
                    name=selected_executor_id,
                    status=RemoteCapabilitySlotStatus.RUNNING,
                    timestamp=self.node.get_clock().now(), ####Clock.now(),
                )
            )

            self._remote_capability_slot_goals[goal_id] = RemoteCapabilityGoalStatus(
                status=selected_executor_id, cancellation_requested=False
            )
            # rospy.logdebug("Starting thread!", logger_name="remote_capability_execution")
            thread = threading.Thread(
                name=f"Thread-RemoteCapabilitySlot-{selected_executor_id}",
                target=self.execute_remote_capability_exec,
                args=(selected_executor_id, goal_handle, reauction_threshold),
            )
            thread.start()
            self.node.get_logger().info(
                "Started thread for execution of remote capability!",
                logger_name="remote_capability_execution",
            )

            result = self.result_queue.get()
            return result

    def execute_remote_capability_cancel_cb(
        self, goal_handle: ServerGoalHandle
    ) -> None:
        """
        Cancel the execution of a remote capability.

        This callback manages the request for the cancellation of a running goal.
        The method will send a failed status back if the goal id is not
        associated with a running goal.

        :param goal_handle: The goal handle containing all the information about
        the goal to cancel.
        :return: None
        """
        goal_info: GoalInfo = goal_handle.goal_id()
        goal_id = str(goal_info.goal_id)
        if goal_id not in self._remote_capability_slot_goals:
            self.node.get_logger().warn("Requested cancellation of unknown remote capability goal!")
            self.__execute_remote_capability_action_server.publish_result(
                status=GoalStatus(
                    goal_id=goal_handle.get_goal_id(),
                    status=GoalStatus.ABORTED,
                ),
                result=ExecuteRemoteCapability.Result(),
            )
            return
        current_status = self._remote_capability_slot_goals[goal_id]
        self._remote_capability_slot_goals[goal_id] = RemoteCapabilityGoalStatus(
            status=current_status.status, cancellation_requested=True
        )
        return
    
    def notify_capability_execution_status(
        self, msg: CapabilityExecutionStatus
    ) -> None:
        """
        Notify the MissionControl node about changes in the capability execution status.

        :param msg: Update msg containing the new status.
        :return: None
        """
        hashable_interface = HashableCapabilityInterface(msg.interface)

        if hashable_interface not in self.__capability_execution_status:
            self.__capability_execution_status[hashable_interface] = {}

        self.__capability_execution_status[hashable_interface][
            msg.node_name
        ] = msg.status

    def request_capability_execution(
        self, request: RequestCapabilityExecution.Request
    ) -> RequestCapabilityExecution.Response:
        """
        Trigger the execution of a capability implementation via a remote capability executor.

        :param request: The request that should be archived.
        :return: Response containing the result of the remote execution.
        """
        response = RequestCapabilityExecution.Response()
        self.node.get_logger().info("Received request to execute capability!")

        if not request.require_local_execution:
            assignment_system_topic = f"{self.node.get_namespace()}/assignment_manager/find_best_executor"
            find_best_executor_service = self.node.create_client(
                FindBestCapabilityExecutor,
                assignment_system_topic
            )

            try:
                find_best_executor_service.wait_for_service(
                    timeout=Duration(seconds=5)
                )

                find_best_executor_response:  FindBestCapabilityExecutor.Response= find_best_executor_service.call(
                    FindBestCapabilityExecutor.Request(
                        capability=request.capability,
                        node_id=request.node_id,
                        mission_control_name=self.node.get_name(),
                        implementation_tags_dict=request.implementation_tags_dict,
                    )
                )

                if find_best_executor_response.success:
                    self.node.get_logger().info("Found a suitable implementation!")
                    response.success = True
                    response.implementation_name = (
                        find_best_executor_response.implementation_name
                    )
                    response.execute_local = find_best_executor_response.execute_local
                    response.remote_mission_controller_topic = (
                        find_best_executor_response.executor_mission_control_topic
                    )
                    return response

            except Exception:
                self.node.get_logger().warn(
                    "Assignment system could not be contacted before timeout!"
                )

            self.node.get_logger().info(
                "Assignment  system is currently offline. We will use the local implementation!"
            )
        else:
            self.node.get_logger().info("Local execution required!")

        local_bid_response = self.get_local_bid(
            GetLocalBid.Request(
                interface=request.capability,
                node_id=request.node_id,
                implementation_tags_dict=request.implementation_tags_dict,
            )
        )

        if local_bid_response.success:
            response.implementation_name = local_bid_response.implementation_name
            response.success = True
            response.execute_local = True
        else:
            response.success = False
            response.error_message = "Could not find a suitable implementation!"

        return response

    def check_precondition_status(
        self, request: CheckPreconditionStatus.Request
    ) -> CheckPreconditionStatus.Response:
        """
        Service to check if the tree tied to this mission controller fulfills the precondition.

        A precondition counts as fulfilled if there is a local capability node that
        is running the required capability.
        This includes both capabilities and remote capability slots.

        :param request: Request containing the precondition that should be checked.
        :return: True if fulfilled or False if not.
        """
        response = CheckPreconditionStatus.Response()
        hashable_capability = HashableCapabilityInterface(request.interface)

        capability_was_executed = (
            hashable_capability in self.__capability_execution_status
        )
        if not capability_was_executed:
            response.available = False
        else:
            response.available = any(
                s
                in [
                    CapabilityExecutionStatus.EXECUTING,
                    CapabilityExecutionStatus.SUCCEEDED,
                ]
                for n, s in self.__capability_execution_status[
                    hashable_capability
                ].items()
            )

        return response
    
    def prepare_local_capability_implementation(
        self, request: PrepareLocalImplementation.Request
    ) -> PrepareLocalImplementation.Response:
        """
        Service that prepares a local capability implementation for execution.

        This includes receiving it from the local capability storage, check and
        fulfill preconditions and then prepare it to be sent to the executor.

        The service will return unsuccessfully if the implementation cannot be found,
        the preconditions can not be fulfilled or the implementation is not a valid tree.

        :param request: The request containing all the information about the implementation.
        :return: A response containing the result status, as well as the potential resulting tree.
        """
        # pylint: disable=too-many-locals, too-many-statements
        response = PrepareLocalImplementation.Response()

        with self.__prepare_local_implementation_service_lock:

            prepare_local_implementation_tree_manager = TreeManager(
                name="LocalBidStagingManager",
                publish_tree_callback=nop,
                publish_debug_info_callback=nop,
                publish_debug_settings_callback=nop,
                publish_node_diagnostics_callback=nop,
                debug_manager=DebugManager(),
            )
            prepare_local_implementation_migration_manager = MigrationManager(
                rosnode=self.node,
                tree_manager=prepare_local_implementation_tree_manager
            )

            with self.__get_capability_implementations_proxy_lock:
                implementations_response: GetCapabilityImplementations.Response = (
                    self.__get_capability_implementations_proxy.call(
                        GetCapabilityImplementations.Request(interface=request.interface)
                    )
                )
            implementation: Optional[CapabilityImplementation] = next(
                filter(
                    lambda x: x.name == request.implementation_name,
                    implementations_response.implementations,
                ),
                None,
            )

            if implementation is None:
                response.success = False
                response.error_message = "Could not get local implementation!"
                return response

            tree: Tree = implementation.tree

            migration_request = MigrateTree.Request(tree=tree)
            migration_response = check_node_versions(migration_request)

            if migration_response.migrated:
                migration_response = (
                    prepare_local_implementation_migration_manager.migrate_tree(
                        migration_request
                    )
                )
                if migration_response.success:
                    tree = migration_response.tree

            service_response: LoadTree.Response = (
                prepare_local_implementation_tree_manager.load_tree(
                    LoadTree.Request(tree=tree)
                )
            )
            if not service_response.success:
                response.success = False
                response.error_message = (
                    f"Failed to load tree: {service_response.error_message}"
                )
                self.node.get_logger().error(response.error_message)
                return response

            root_node = prepare_local_implementation_tree_manager.find_root()
            root_sequence_node = Sequence(name="RootCapabilitySequence")
            service_response1: AddNode.Response = (
                prepare_local_implementation_tree_manager.add_node(
                    AddNode.Request(
                        parent_name=None,
                        node=root_sequence_node.to_msg(),
                        allow_rename=False,
                    )
                )
            )

            if not service_response1.success:
                response.success = False
                response.error_message = (
                    "Failed to add sequence for preconditions: "
                    f"{service_response1.error_message}"
                )
                self.node.get_logger().error(response.error_message)
                return response

            service_response2: MoveNode.Response = (
                prepare_local_implementation_tree_manager.move_node(
                    MoveNode.Request(
                        node_name=root_node.name,
                        new_parent_name=root_sequence_node.name,
                        new_child_index=0,
                    )
                )
            )

            if not service_response2.success:
                response.success = False
                response.error_message = (
                    f"Failed to move implementation under sequence node:"
                    f"{service_response2.error_message}"
                )
                self.node.get_logger().error(response.error_message)
                return response

            for precondition in reversed(implementation.preconditions):

                if precondition.type == Precondition.REMOTE:
                    available_services = rosidl_runtime_py.utilities.get_service("CheckPreconditionStatus")
                    # ！！！！和其他地方的get service list然后按照服务名在列表中查找不一样
                    # 原：rosservice_find("CheckPreconditionStatus")

                    for service_name in available_services:
                        service_client = self.node.create_client(
                            service_name, CheckPreconditionStatus
                        )
                        service_client.wait_for_service(1.0)
                        service_response3: CheckPreconditionStatus.Response = (
                            service_client(
                                CheckPreconditionStatus.Request(
                                    interface=precondition.capability
                                )
                            )
                        )
                        if service_response3.available:
                            self.node.get_logger().info(
                                f"Remote precondition: {precondition} is fulfilled"
                            )
                            continue

                local_precondition_fulfilled = self.check_precondition_status(
                    CheckPreconditionStatus.Request(interface=precondition.capability)
                ).available

                if not local_precondition_fulfilled:
                    response.success = False
                    response.error_message = "Local precondition not fulfilled!"
                    self.node.get_logger().error(response.error_message)
                    return response

                self.node.get_logger().debug(f"Precondition: {precondition} is fulfilled")

            response.success = True
            response.implementation_subtree = (
                prepare_local_implementation_tree_manager.tree_msg
            )
            return response
        
    def get_available_remote_capability_slots(
        self, requests: GetAvailableRemoteCapabilitySlots.Request
    ) -> GetAvailableRemoteCapabilitySlots.Response:
        """
        Return the amount of available remote capability slots.

        :param requests: None
        :return: Amount of available remote capability slots.
        """
        response = GetAvailableRemoteCapabilitySlots.Response()

        response.available_remote_capability_slots = len(
            list(
                filter(
                    lambda x: self._remote_capability_slot_status[x].status
                    == RemoteCapabilitySlotStatus.IDLE
                    and not (
                        self._remote_capability_slot_status[x].reserved
                        and self.node.get_clock().now() ####Clock.now()
                        < self._remote_capability_slot_status[x].timestamp
                        + self._remote_capability_slot_reservation_duration
                    ),
                    self._remote_capability_slot_status,
                )
            )
        )
        return response
    
def nop(msg):
    """
    No operation method.

    :return: None
    """
