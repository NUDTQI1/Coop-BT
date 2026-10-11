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
import uuid
from actionlib_msgs.msg import GoalID

from ros_bt_py_interfaces.msg import (
    CapabilityImplementation,
    CapabilityInterface,  ### change 0701
    Precondition,
    RemoteCapabilitySlotStatus,
    CapabilityExecutionStatus,
    Tree,
)

### change 0701
####### change 0604-2
from ros_bt_py_interfaces.msg import (
    DebugInfo,
    DebugSettings,
    NodeDiagnostics,
    Messages,
    Packages,
)
from std_msgs.msg import Float64
from rclpy.node import ReentrantCallbackGroup
from diagnostic_msgs.msg import DiagnosticArray
### change 0701

from ros_bt_py_interfaces.action import (
    ExecuteRemoteCapability,
)
import time
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


from rclpy.node import ReentrantCallbackGroup
from diagnostic_msgs.msg import DiagnosticArray
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)



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

    def __init__(
            self,
            node : Node,
            ):
        """
        Create a new capability repository.

        The repository has to major interfaces, it communicates with other repositories and
        offers services geared towards its local subscribers.
        """
        self.node = node



        self.__capability_execution_status: Dict[
            HashableCapabilityInterface, Dict[str, int]
        ] = {}
        # self.capability_repository_topic = (
        #     f"{self.node.get_namespace()}/capability_repository"
        # )

        self.capability_repository_topic = f"{self.node.get_namespace()}/capability_repository/capabilities/implementations/get"  ### change 0701

        self.__get_capability_implementations_proxy = self.node.create_client(
            GetCapabilityImplementations, 
            self.capability_repository_topic,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
                ),
            callback_group=ReentrantCallbackGroup()   ### change 0701
        )
        self.__get_capability_implementations_proxy_lock = RLock()

        self.node.get_logger().warning(f" start ceating service of mission control at {self.capability_repository_topic}! ")

        # self.__request_capability_execution_service: Service = self.node.create_service(
        #     RequestCapabilityExecution,
        #     "~/execute_capability",
        #     self.request_capability_execution,
        #     ### change 0701
        #     qos_profile=QoSProfile(
        #     reliability=QoSReliabilityPolicy.RELIABLE,
        #     durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        #     history=QoSHistoryPolicy.KEEP_LAST,
        #     depth=10,
        #     ),
        #     callback_group=ReentrantCallbackGroup()
        # )

        # self.__get_local_bid_service: Service = self.node.create_service(
        #      GetLocalBid, "~/get_local_bid", self.get_local_bid,
        #     qos_profile=QoSProfile(
        #     reliability=QoSReliabilityPolicy.RELIABLE,
        #     durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        #     history=QoSHistoryPolicy.KEEP_LAST,
        #     depth=10,
        #     ),
        #     callback_group=ReentrantCallbackGroup()
        # )
 
        self.__get_local_bid_service_client = self.node.create_client(
            GetLocalBid,
            "~/get_local_bid",
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
        )

        # self.__check_precondition_status_service: Service = self.node.create_service(
        #     CheckPreconditionStatus,
        #     "~/check_precondition_status",
        #     self.check_precondition_status,#handler=self.check_precondition_status,
        # )

        # self.__prepare_local_implementation_service = self.node.create_service(
        #     PrepareLocalImplementation,
        #     "~/prepare_local_implementation",
        #     self.prepare_local_capability_implementation,
        # )
        self.__prepare_local_implementation_service_lock = RLock()

        # self._capability_execution_status_subscriber = self.node.create_subscription(
        #     msg_type=CapabilityExecutionStatus,
        #     topic="~/notify_capability_execution_status",
        #     callback=self.notify_capability_execution_status,
        #     qos_profile=10,
        # )

        qos=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            )
        self.__execute_remote_capability_action_server = ActionServer(
            self.node,  ###add
            action_type=ExecuteRemoteCapability,
            action_name="~/execute_remote_capability",
            callback_group=ReentrantCallbackGroup(),        ### change 0701
            execute_callback=self.execute_remote_capability_exec,
            # goal_callback=self.execute_remote_capability_goal_cb,
            # cancel_callback=self.execute_remote_capability_cancel_cb,
            
        )

        # self._local_remote_slots_status_subscriber = self.node.create_subscription(
        #     RemoteCapabilitySlotStatus,
        #     "~/remote_slot_status",
        #     self.remote_capability_slot_status_callback,
        #     qos_profile=10,
        # )

        # self.__available_remote_capability_slots_service = self.node.create_service(
        #     GetAvailableRemoteCapabilitySlots,
        #     "~/get_available_remote_slots",
        #     self.get_available_remote_capability_slots,
        # )

        # self.__reserve_remote_capability_slot_service = self.node.create_service(
        #     ReserveRemoteCapabilitySlot,
        #     "~/reserve_remote_capability_slot",
        #     self.reserve_remote_capability_executor,
        # )

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
        remote_capability_slot_reservation_duration_sec = 6.0#self.node.get_parameter("remote_capability_slot_reservation_duration_sec").value
        #rospy.get_param("~remote_capability_slot_reservation_duration_sec", 5.0)

        # self._remote_capability_slot_reservation_duration = Time()
        # self._remote_capability_slot_reservation_duration.sec=5
        self._remote_capability_slot_reservation_duration = Duration(seconds=remote_capability_slot_reservation_duration_sec)

        # 获取队列主题名称
        team_join_topic = self.node.get_parameter("mission_control_join_topic").value if self.node.has_parameter("mission_control_join_topic") else "/team_join"

    
        self.__team_join_publisher: Publisher = self.node.create_publisher(Time, team_join_topic, qos_profile=10,)#queue_size=1)

        self.__team_join_subscriber = self.node.create_subscription(Time, team_join_topic, self.team_join_callback, qos_profile=10,)

        time_msg=self.node.get_clock().now().to_msg()  ####add
        self.__team_join_publisher.publish(time_msg)#(Time(sec=time_msg.sec,nanosec=time_msg.nanosec)) ###add
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
        #self.__request_capability_execution_service.destroy()
        #self.__get_local_bid_service.destroy()
        self.__get_local_bid_service_client.destroy()
        #self.__check_precondition_status_service.destroy()
        #self.__prepare_local_implementation_service.destroy()

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

        if state is AsyncServiceProxy.RESPONSE_READY:  ### change 0701 下一行
            response=service_proxy.get_response()#response: RunRemoteCapabilitySlot.Response = service_proxy.get_response()
            if response.success:
                return response
            raise RuntimeError(f"Service call failed {response.error_message})")
        raise RuntimeError(f"Async proxy in invalid state {state})")
    
    def get_local_bid(self, request: GetLocalBid.Request, response:GetLocalBid.Response) -> GetLocalBid.Response:   #, service_response: GetLocalBid.Response## change 0701
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
        self.node.get_logger().warn("start get bid!!!!!!!!!!!!!!!!!!!!!!!!!!!")
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
                    timeout_sec=1.0)   ### change 0701
            except Exception:
                service_response.error_message = (
                    "Failed to find local implementation service"
                )
                self.node.get_logger().warn(service_response.error_message)
                service_response.success = False

                return service_response

            try:
                self.node.get_logger().warn(f"8888888888888888888 Start GetImple ! interface:{request.interface}")
                ### change 0701
                # response: GetCapabilityImplementations.Response = (
                #    self.__get_capability_implementations_proxy.call_async(   ### change 0701
                #        GetCapabilityImplementations.Request(interface=request.interface)
                #    ).result()
                # )
                # self.node.get_logger().warn(f"8888888888888888888 client: {self.__get_capability_implementations_proxy.srv_name}")

                # while response is None:
                #     time.sleep(1.0)
                #     response: GetCapabilityImplementations.Response = (
                #     self.__get_capability_implementations_proxy.call_async(   ### change 0701
                #        GetCapabilityImplementations.Request(interface=request.interface)
                #     ).result()
                #     )

                #     self.node.get_logger().warn(f"8888888888888888888 wait response: {response}")
                
                
                new_req=GetCapabilityImplementations.Request()
                new_req.interface=request.interface
                response= GetCapabilityImplementations.Response()
                self.node.get_logger().warn(f"8888888888888888888 Start GetImple ! interface:{new_req.interface}")
                new_future=self.__get_capability_implementations_proxy.call_async(new_req)
                time_sum=0

                while not new_future.done():
                    time.sleep(1.0)
                    time_sum+=1.0
                    if time_sum>5.0:
                        response.success = False
                        service_response = response
                        break
                    self.node.get_logger().warn(f"Wait Get Imple ") #: {new_future.result()}...")
                response=new_future.result()

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
        
        self.node.get_logger().warn(f"8888888888888888888 end this ! ")
        for p in response.implementations:
            implementation: CapabilityImplementation = p
            impl_tags: Dict[str, str] = json_decode(implementation.tags_dict)
            self.node.get_logger().error(f"Impl tags: {impl_tags}")

            shared_tags = {
                k: tags[k] for k in tags if k in impl_tags and tags[k] == impl_tags[k]
            }
            self.node.get_logger().warn(f"9999999999999999999 start this1 {len(shared_tags)} ! ")
            self.node.get_logger().error(f"Shared tags: {shared_tags}")
            if len(shared_tags) < len(impl_tags):
                self.node.get_logger().warn(
                    "Requested interface does not match with all tags of "
                    f"{implementation.name} with tags {impl_tags}"
                )
                continue
            else:
                self.node.get_logger().warn(f"9999999999999999999 start this2 ! ")
                valid_implementations.append(implementation)
                


        if len(valid_implementations) < 1:
            service_response.success = False
            service_response.error_message = "No suitable implementation found!"
            return service_response

        



        
        #### change 0701
        self.publisher_callback_group = ReentrantCallbackGroup()
        self.tree_pub = self.node.create_publisher(
            Tree,
            "~/tree",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
        )
        self.debug_info_pub = self.node.create_publisher(
            DebugInfo,
            "~/debug/debug_info",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
        )
        self.debug_settings_pub = self.node.create_publisher(
            DebugSettings,
            "~/debug/debug_settings",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
        )
        self.node_diagnostics_pub = self.node.create_publisher(
            NodeDiagnostics,
            "~/debug/node_diagnostics",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
        )
        self.tick_frequency_pub = self.node.create_publisher(
            Float64,
            "~/debug/tick_frequency",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
        )
        self.ros_diagnostics_pub = self.node.create_publisher(
            DiagnosticArray,
            "/diagnostics",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
        )
        
        

        get_local_bid_tree_manager = TreeManager(
            ros_node=self.node,   #### change 0604
            name="LocalBidStagingManager",
            publish_tree_callback=self.tree_pub.publish,#self.nop(),#self.nop(self,self.tree_pub.publish),#nop,
            publish_debug_info_callback=self.debug_info_pub.publish,#self.nop(),#self.nop(self,self.debug_info_pub.publish),#(self,None),
            publish_debug_settings_callback=self.debug_settings_pub.publish,#self.nop(),#self.nop(self,self.debug_settings_pub.publish),#(self,None),
            publish_node_diagnostics_callback=self.node_diagnostics_pub.publish,#self.nop(),#self.nop(self,self.node_diagnostics_pub.publish),#(self,None),
            publish_diagnostic_callback=self.ros_diagnostics_pub.publish,#self.nop(),
            publish_tick_frequency_callback=self.tick_frequency_pub.publish,#self.nop(),
            #debug_manager=self.node.debug_manager,
        )
        self.node.get_logger().warn(f"9999999999999999999 start this3 !")


        
        
        # get_local_bid_tree_manager = TreeManager(
        #     name="LocalBidStagingManager",
        #     publish_tree_callback=nop,
        #     publish_debug_info_callback=nop,
        #     publish_debug_settings_callback=nop,
        #     publish_node_diagnostics_callback=nop,
        #     debug_manager=DebugManager(),
        #     simulate_tick=True,
        #     succeed_always=False,
        # )





        ###   change 0701   待修改！！！！！！！！！！！！！！！！！！！！！！！！！！！！！

        ### change 0701
        # get_local_bid_migration_manager = MigrationManager(
        #     rosnode=self.node,
        #     tree_manager=get_local_bid_tree_manager
        # )
        # self.node.get_logger().warn(f"9999999999999999999 start this3 !")

        implementation_utility: Dict[str, float] = {}
        for implementation in valid_implementations:
            tree = implementation.tree

            ### change 0701
            # migration_request = MigrateTree.Request(tree=tree)
            # migration_response = check_node_versions(migration_request)

            # if migration_response.migrated:
            #     migration_response = get_local_bid_migration_manager.migrate_tree(
            #         migration_request
            #     )
            #     if migration_response.success:
            #         tree = migration_response.tree

            ### change 0701
            req = LoadTree.Request()
            req.tree = tree

            res = get_local_bid_tree_manager.load_tree(req, LoadTree.Response())#(LoadTree.Request(tree=tree))
            while res is None:
                time.sleep(1.0)
                self.node.get_logger().warn(f" 8888888888888888888888888 wait!!!")
            if not res.success:
                self.node.get_logger().error(
                    "Failed to load implementation for calculating the local bid: "
                    f"{res.error_message}"
                )
                continue
            self.node.get_logger().warn(f" 8888888888888888888888888 end !!! self cap:{request.node_id}")

            try:
                set_capability_io_bridge_id(
                    tree_manager=get_local_bid_tree_manager,
                    interface=request.interface,
                    io_bridge_id=request.node_id,
                )

                # self.node.get_logger().warn(f" tttest1")
                # get_local_bid_tree_manager.find_root().setup()
                # self.node.get_logger().warn(f" tttest1")
                # get_local_bid_tree_manager.find_root().tick()


                self.node.get_logger().warn(f" 44444444444444444444444 {get_local_bid_tree_manager.find_root().name}")

                calculated_utility = (
                    get_local_bid_tree_manager.find_root().calculate_utility()
                )
            except (BehaviorTreeException, Exception) as exc:
                self.node.get_logger().error(f"Failed to calculate utility value from tree: {exc}")
                continue

            self.node.get_logger().warn(f" 444444444444444444444 {calculated_utility}!!!!")

            if calculated_utility.lower_bound_success == 0.0:
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

            self.node.get_logger().warn(f" 444444444444444444444 cut1!!!!")
            try:
                get_local_bid_tree_manager.find_root().shutdown()
                get_local_bid_tree_manager.clear(ClearTree.Request(),ClearTree.Response())
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

        self.node.get_logger().warn(f" 444444444444444444444 cut2!!!!")

        (
            best_implementation_name,
            best_implementation_bid,
        ) = sorted_implementation_bids[0]
        
        ###   change 0701  待修改！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！！


        
        self.node.get_logger().warn(f" best impl bid: {best_implementation_bid}")
        service_response.bid = best_implementation_bid # 1.0# 
        service_response.implementation_name = best_implementation_name # valid_implementations[0].name#
        service_response.success = True
        self.node.get_logger().info(f"Selected implementation: {service_response.implementation_name}")
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
                self.node.get_logger().warn(f"111 {self.node.get_clock().now()} 222 {timestamp} 333 {self._remote_capability_slot_reservation_duration}")
                test_time=self.node.get_clock().now().nanoseconds - timestamp.sec*1000000000- timestamp.nanosec- self._remote_capability_slot_reservation_duration.nanoseconds
                self.node.get_logger().warn(f"111 {test_time}")
                # if (
                #     self.node.get_clock().now().nanoseconds ####Clock.now()
                #     > timestamp.sec*1000000000+ timestamp.nanosec+ self._remote_capability_slot_reservation_duration.nanoseconds
                # ):
                #     self.node.get_logger().warn(f"Change reserved status of the slot! now time :{self.node.get_clock().now().nanoseconds}")
                #     self._remote_capability_slot_status[msg.name].reserved = False
                    
    def reserve_remote_capability_executor(
        self, request: ReserveRemoteCapabilitySlot.Request, response:ReserveRemoteCapabilitySlot.Response ### change 0701
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
                            and self.node.get_clock().now().nanoseconds ####Clock.now()
                            < status_data.timestamp.sec*1000000000+status_data.timestamp.nanosec
                            + self._remote_capability_slot_reservation_duration.nanoseconds
                        )
                    )(self._remote_capability_slot_status[x]),
                    self._remote_capability_slot_status,
                )
            )

        if len(idle_executor_ids) < 1:
            self.node.get_logger().error(
                "No idle remote_capability_slot found, cannot reserve remote capability slot.",
                #logger_name="remote_capability_execution",
            )
            response.success = False
            response.error = "No idle remote_capability_slot found!"

            return response

        reserved_remote_capability_slot = idle_executor_ids.pop()
        self.node.get_logger().warn(f"test-reserved remote cap slot: {reserved_remote_capability_slot}, and slot status is :{self._remote_capability_slot_status}")
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
            ].timestamp = self.node.get_clock().now().to_msg() ####Clock.now()  ###change 0701 不确定是否要加to msg
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].remote_mission_control_topic = request.remote_mission_control
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].reauction_threshold = request.reauction_threshold
            self._remote_capability_slot_status[
                reserved_remote_capability_slot
            ].implementation_name = request.implementation_name
            self.node.get_logger().warn(f"test-reserve remote slot: {self._remote_capability_slot_status}")

        response.success = True
        return response
    
    def execute_remote_capability_exec(
        self,
        #remote_slot_name: str,
        goal_handle: ServerGoalHandle,#在 ROS2 Action 服务器中，goal_handle 是由 ActionServer 回调传入的。通常，它用于更新目标的状态或返回结果。
        #goal_handle,
        #goal_id,
        #reauction_threshold: float,
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











        ########################## from here: goal_callback ################################


        self.node.get_logger().warn(f"1111111111111111118888888888888 {type(goal_handle)}")
        goal_info: GoalInfo = goal_handle.goal_id#()
        self.node.get_logger().warn(f"1111111111111111118888888888888 {goal_handle}")
        goal_id = str(goal_info)#.goal_id)
        #goal_stamp = goal_info.stamp
        goal: ExecuteRemoteCapability.Goal = goal_handle.request#()
        self.node.get_logger().warn("111111111111111111888888888888888 cut1")


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
            self.node.get_logger().warn(f"11111111111111111133333333333 cut2, reserved_executors:{reserved_executors}")
            self.node.get_logger().warn(f"idle_Executor_ids:{idle_executor_ids}")
            self.node.get_logger().warn(f"idle_Executor_status:{self._remote_capability_slot_status[idle_executor_ids[0]]}")
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
                    #logger_name="remote_capability_execution",
                )
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        no_executor_available=True,
                        error_message="No reserved remote capability slot executor available!",
                    )
                # goal_ids: GoalID = GoalStatus.goal_id()
                # goal_ids.id = goal_id
                # goal_ids.stamp = goal_stamp



                #ActionServer.notify_goal_done()
                self.node.get_logger().warn(f"type of exe remote action server: {type(self.__execute_remote_capability_action_server)}")
                #### change 0701#####################
                self.__execute_remote_capability_action_server.publish_result(
                    status=GoalStatus(
                        goal_id=goal_id, status=GoalStatus.SUCCEEDED#goal_id=goal_ids, status=GoalStatus.SUCCEEDED
                    ),
                    result=ExecuteRemoteCapability.Result(
                        success=False,
                        no_executor_available=True,
                        error_message="No reserved remote capability slot executor available!",
                    ),
                )
                #### change 0701#####################





                return result

            self.node.get_logger().warn("11111111111111111133333333333 cut3")
            self.remote_capability_slot_status_callback(
                RemoteCapabilitySlotStatus(
                    name=selected_executor_id,
                    status=RemoteCapabilitySlotStatus.RUNNING,
                    timestamp=self.node.get_clock().now().to_msg(), ####Clock.now(),  ### change 0701
                )
            )
            self.node.get_logger().warn(f"11111111111111111133333333333 cut4 {selected_executor_id}")

            self._remote_capability_slot_goals[goal_id] = RemoteCapabilityGoalStatus(
                status=selected_executor_id, cancellation_requested=False
            )
            # rospy.logdebug("Starting thread!", logger_name="remote_capability_execution")
            remote_slot_name=selected_executor_id
            self.node.get_logger().info(
                "Started thread for execution of remote capability!"#,
                #logger_name="remote_capability_execution",  
            )


            ########################## from here: goal_callback ################################




















        self.node.get_logger().warn("11111111111112222222222222")
        goal_info: GoalInfo = goal_handle.goal_id#()
        goal_id = str(goal_info)#.goal_id)

        goal: ExecuteRemoteCapability.Goal = goal_handle.request#()
        # goal_id=goal_id
        # goal=goal_handle

        prepare_local_capability_implementation_service_proxy = self.node.create_client(
            PrepareLocalImplementation,
            "~/prepare_local_implementation",
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
        )
        self.node.get_logger().warn("Received request to execute_remote_capability!")

        # response: PrepareLocalImplementation.Response = prepare_local_capability_implementation_service_proxy.call_async(
        #                                                     PrepareLocalImplementation.Request(
        #                                                         interface=goal.interface,
        #                                                         implementation_name=goal.implementation_name,
        #                                                     )
        #                                                 ).result()
        req11=PrepareLocalImplementation.Request()
        req11.interface=goal.interface
        req11.implementation_name=goal.implementation_name
        response=prepare_local_capability_implementation_service_proxy.call_async(req11)
        while not response.done():
            time.sleep(0.5)
            self.node.get_logger().warn(f"222222222222222222222")
        response=response.result()


        if not response.success:
            self.node.get_logger().warn(
                f"Could not prepare local implementation for: {response.error_message}",
                #logger_name="remote_capability_execution",
            )
            
            result=ExecuteRemoteCapability.Result(
                success=False, error_message=response.error_message
            )
            #self.result_queue.put(result)
            return result

        run_remote_capability_slot_topic = self.node.get_namespace() + f"/mission_control/remote_capability_slot/{remote_slot_name}/run"
        

        
        ############### change 0701 改变了此文件包括remote cap slot文件对应的 RunRemoteCapSlot 相关的client
        
        run_remote_capability_slot_service_proxy = self.node.create_client(
            RunRemoteCapabilitySlot, 
            run_remote_capability_slot_topic,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
            )
            # AsyncServiceProxy(run_remote_capability_slot_topic, RunRemoteCapabilitySlot)

        cancel_remote_capability_slot_topic = self.node.get_namespace() + f"/mission_control/remote_capability_slot/{remote_slot_name}/cancel"

        cancel_remote_capability_slot_service_proxy = self.node.create_client(
            CancelRemoteCapabilitySlot, 
            cancel_remote_capability_slot_topic,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
            )

        try:
            run_remote_capability_slot_service_proxy.wait_for_service(
                timeout_sec=5#Duration(seconds=5)  ### change 0701
            )
        except Exception:
            self.node.get_logger().warn(
                "Could not contact remote_capability slot before timeout",
                #logger_name="remote_capability_execution",
            )
            
            result=ExecuteRemoteCapability.Result(
                success=False,
                error_message="Could not contact remote_capability slot before timeout",
            )
            return result

        self.node.get_logger().info(
            f"Send goal to {run_remote_capability_slot_topic}"#,
            #logger_name="remote_capability_execution",          ### change 0701
        )
        ### change 0701
        # run_remote_capability_slot_service_proxy.call_async(#call_service(
        #     RunRemoteCapabilitySlot.Request(
        #         implementation_tree=response.implementation_subtree,
        #         interface=goal.interface,
        #         node_id=goal.node_id,
        #     )
        # )
        # try:
        #     response_run: Optional[
        #         RunRemoteCapabilitySlot.Response
        #     ] = self.__handle_async_service_call(
        #         run_remote_capability_slot_service_proxy
        #     )
        run_remote_req=RunRemoteCapabilitySlot.Request()
        run_remote_req.implementation_tree=response.implementation_subtree
        run_remote_req.interface=goal.interface
        run_remote_req.node_id=goal.node_id
        run_remote_response=run_remote_capability_slot_service_proxy.call_async(run_remote_req)
        sum_time=0
        while not run_remote_response.done():
            time.sleep(0.5)
            sum_time+=0.5
            self.node.get_logger().warn("111111111122222222")
            if sum_time>40:
                result=ExecuteRemoteCapability.Result(
                success=False,
                error_message="Failed to execute remote capability",
                )
                return result
        run_remote_response=run_remote_response.result()
        try:
            response_run = run_remote_response
        except RuntimeError as exc:
            self.node.get_logger().error(
                f"Failed to execute remote capability: {exc}",
                #logger_name="remote_capability_execution",
            )
            result=ExecuteRemoteCapability.Result(
                success=False,
                error_message="Failed to execute remote capability",
            )
            return result

        expended_costs = 0

        new_request=GetLocalBid.Request()
        new_request.node_id=goal.node_id        
        new_request.implementation_tags_dict=''
        new_request.interface=goal.interface

        self.node.get_logger().info(f"77777777777777777777 half part")

        local_bid_response = self.get_local_bid(
            new_request,GetLocalBid.Response()
        )
        # local_bid_response = self.get_local_bid(
        #     GetLocalBid.Request(interface=goal.interface, node_id=goal.node_id),GetLocalBid.Response()
        # )
        if local_bid_response.success:
            previous_cost_projection = local_bid_response.bid
        else:
            previous_cost_projection = reauction_threshold * 0.8
        #previous_cost_projection=1.0

        changed_reauction_threshold = reauction_threshold - previous_cost_projection
        self.node.get_logger().warn(f"1111111111333333333333 finish execute {response_run}")

        while response_run is None:

            local_bid_response = self.get_local_bid(
                GetLocalBid.Request(interface=goal.interface, node_id=goal.node_id),GetLocalBid.Response()
            )
            if local_bid_response.success:
                expended_costs += abs(previous_cost_projection - local_bid_response.bid)
                previous_cost_projection = local_bid_response.bid
                if expended_costs > changed_reauction_threshold:
                    try:
                        cancel_remote_capability_slot_service_proxy.call_async(
                            CancelRemoteCapabilitySlot.Request()
                        )
                    except Exception as exc:
                        self.node.get_logger().error(
                            f"Failed to cancel the remote capability slot execution: {exc}"
                        )
                    run_remote_capability_slot_service_proxy.destroy()# stop_call()  ### change 0701
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
                    #logger_name="remote_capability_execution",
                )
                try:
                    cancel_remote_capability_slot_service_proxy.call_async(
                        CancelRemoteCapabilitySlot.Request()
                    )
                except Exception as exc:
                    self.node.get_logger().error(
                        f"Failed to cancel the remote capability slot execution: {exc}"
                    )

                run_remote_capability_slot_service_proxy.destroy() #stop_call()   #### change 0701 不确定!!!!!!!!!!!!!!!!!!!!!!!
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
                    #logger_name="remote_capability_execution",
                )
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        error_message="Failed to execute remote capability",
                    )
                return result

        result=ExecuteRemoteCapability.Result(
                success=response_run.success, error_message=response_run.error_message
            )
        
        # result1=ExecuteRemoteCapability.Impl.GetResultService.Response()
        # result1.status = GoalStatus.SUCCEEDED
        # result1.result=result
        # future1=Future()
        # future=Future.set_result(future1,result=result1)
        # self.__execute_remote_capability_action_server._send_result_response(#publish_result(
        #     # future=Future(status=GoalStatus(
        #     #     goal_id=GoalID(id=goal_id,stamp=self.node.get_clock().now().to_msg()), status=GoalStatus.SUCCEEDED
        #     # ),
        #     # result=ExecuteRemoteCapability.Result(
        #     #     success=response.success, error_message=response.error_message
        #     # ))
        #     future=future
        # )
        goal_handle.succeed()
        del self._remote_capability_slot_goals[goal_id]
        self.node.get_logger().warn("111111111133333333333333 return result")
        #self.result_queue.put(result)
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
        self.node.get_logger().warn(f"111111111111111111333333333333333333333 receive goal msg!!!")
        self.node.get_logger().warn(f"1111111111111111113333333333333333333 {type(goal_handle)}")
        # goal_info: GoalInfo = goal_handle.goal_id()
        # goal_id = str(goal_info.goal_id)
        # goal_stamp = goal_info.stamp
        # goal: ExecuteRemoteCapability.Goal = goal_handle.request()
        self.node.get_logger().warn("11111111111111111133333333333 cut1")
        goal_id=str(uuid.uuid4())
        goal=goal_handle

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
            self.node.get_logger().warn("11111111111111111133333333333 cut2")

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
                    #logger_name="remote_capability_execution",
                )
                result=ExecuteRemoteCapability.Result(
                        success=False,
                        no_executor_available=True,
                        error_message="No reserved remote capability slot executor available!",
                    )
                # goal_ids: GoalID = GoalStatus.goal_id()
                # goal_ids.id = goal_id
                # goal_ids.stamp = goal_stamp




                #### change 0701#####################
                self.__execute_remote_capability_action_server.publish_result(
                    status=GoalStatus(
                        goal_id=goal_id, status=GoalStatus.SUCCEEDED#goal_id=goal_ids, status=GoalStatus.SUCCEEDED
                    ),
                    result=ExecuteRemoteCapability.Result(
                        success=False,
                        no_executor_available=True,
                        error_message="No reserved remote capability slot executor available!",
                    ),
                )
                #### change 0701#####################





                return result

            self.node.get_logger().warn("11111111111111111133333333333 cut3")
            self.remote_capability_slot_status_callback(
                RemoteCapabilitySlotStatus(
                    name=selected_executor_id,
                    status=RemoteCapabilitySlotStatus.RUNNING,
                    timestamp=self.node.get_clock().now().to_msg(), ####Clock.now(),  ### change 0701
                )
            )
            self.node.get_logger().warn(f"11111111111111111133333333333 cut4 {selected_executor_id}")

            self._remote_capability_slot_goals[goal_id] = RemoteCapabilityGoalStatus(
                status=selected_executor_id, cancellation_requested=False
            )
            # rospy.logdebug("Starting thread!", logger_name="remote_capability_execution")
            thread = threading.Thread(
                name=f"Thread-RemoteCapabilitySlot-{selected_executor_id}",
                target=self.execute_remote_capability_exec,
                args=(selected_executor_id, goal_handle, goal_id, reauction_threshold),   ### change 0701
            )
            thread.start()
            self.node.get_logger().info(
                "Started thread for execution of remote capability!"#,
                #logger_name="remote_capability_execution",  
            )
            self.node.get_logger().warn(f"11111111111111111133333333333 cut 5")
            #goal_handle.succeed()
            return GoalStatus.ACCEPTED
            

            #result = self.result_queue.get()
            #self.node.get_logger().warn(f"11111111111111111133333333333 cut 6")
            #return result

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
        self.node.get_logger().warn(f"111111111111333333333333 start notify exe status: {msg.status}!")

        if hashable_interface not in self.__capability_execution_status:
            self.__capability_execution_status[hashable_interface] = {}

        self.__capability_execution_status[hashable_interface][
            msg.node_name
        ] = msg.status

    def request_capability_execution(
        self, request: RequestCapabilityExecution.Request, response:RequestCapabilityExecution.Response ### change 0701
    ) -> RequestCapabilityExecution.Response:
        """
        Trigger the execution of a capability implementation via a remote capability executor.

        :param request: The request that should be archived.
        :return: Response containing the result of the remote execution.
        """
        response = RequestCapabilityExecution.Response()
        self.node.get_logger().warn("777777777777777777777 Received request to execute capability!")

        if not request.require_local_execution:
            assignment_system_topic = f"{self.node.get_namespace()}/assignment_manager/find_best_executor"
            self.find_best_executor_service = self.node.create_client(
                FindBestCapabilityExecutor,
                assignment_system_topic,   ### change 0701 待增加qos
                qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=10),#10,
                callback_group=ReentrantCallbackGroup()
            )
            self.node.get_logger().warn("777777777777777777777")

            try:
                self.find_best_executor_service.wait_for_service(
                    timeout_sec=5.0#Duration(seconds=5)
                )
                self.node.get_logger().warn("44444444444444444 can use find best")

                
                ### change 0701
                fb_request=FindBestCapabilityExecutor.Request()
                fb_request.capability=request.capability
                fb_request.node_id=request.node_id
                fb_request.mission_control_name=self.node.get_name()
                fb_request.implementation_tags_dict=request.implementation_tags_dict
                self.node.get_logger().warn(f"44444444444444444 wait for find {fb_request}")
                fb_response=self.find_best_executor_service.call_async(fb_request)
                sum_time=0
                while not fb_response.done():
                    self.node.get_logger().warn("44444444444444444 wait for find best")
                    time.sleep(1.0)
                    ### add 0312
                    sum_time+=1
                    if sum_time>12:
                        find_best_executor_response=FindBestCapabilityExecutor.Result(success=False)
                        break

                find_best_executor_response=fb_response.result()
                
                
                
                
                # find_best_executor_response:  FindBestCapabilityExecutor.Response= find_best_executor_service.call(
                #     FindBestCapabilityExecutor.Request(
                #         capability=request.capability,
                #         node_id=request.node_id,
                #         mission_control_name=self.node.get_name(),
                #         implementation_tags_dict=request.implementation_tags_dict,
                #     )
                # )

                if find_best_executor_response.success:
                    self.node.get_logger().info(f"Found a suitable implementation from {find_best_executor_response.executor_mission_control_topic}!")
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
            response.success = False
            response.error_message = "Could not find a remote helper!"
            return response
        else:
            self.node.get_logger().info("Local execution required!")
        
        ### change 0701
        self.node.get_logger().info(f"77777777777777777777 request: {request.capability}")

        new_request=GetLocalBid.Request()
        new_request.node_id=request.node_id        
        new_request.implementation_tags_dict=request.implementation_tags_dict
        new_request.interface=request.capability

        self.node.get_logger().info(f"77777777777777777777 half part")  
 

        local_bid_response = self.get_local_bid(
            # GetLocalBid.Request(
            #     interface=request.capability,
            #     node_id=request.node_id,
            #     implementation_tags_dict=request.implementation_tags_dict,
            # ),
            # GetLocalBid.Response()
            new_request,GetLocalBid.Response()
        )
        self.node.get_logger().info(f"77777777777777777777 end : {local_bid_response}")


        if local_bid_response.success:
            response.implementation_name = local_bid_response.implementation_name
            response.success = True
            response.execute_local = True
        else:
            response.success = False
            response.error_message = "Could not find a suitable implementation!"

        return response

    def check_precondition_status(
        self, request: CheckPreconditionStatus.Request, response: CheckPreconditionStatus.Response   ### change 0701
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
        self, request: PrepareLocalImplementation.Request, response: PrepareLocalImplementation.Response   ### change 0701
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
        self.node.get_logger().warn("Received request to prepare local implementation!")
        with self.__prepare_local_implementation_service_lock:

            
            
            
            
            
            
            
            #### change 0701
            self.publisher_callback_group = ReentrantCallbackGroup()
            self.tree_pub = self.node.create_publisher(
            Tree,
            "~/tree",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
            )
            self.debug_info_pub = self.node.create_publisher(
            DebugInfo,
            "~/debug/debug_info",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
            )
            self.debug_settings_pub = self.node.create_publisher(
            DebugSettings,
            "~/debug/debug_settings",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
            )
            self.node_diagnostics_pub = self.node.create_publisher(
            NodeDiagnostics,
            "~/debug/node_diagnostics",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
            )
            self.tick_frequency_pub = self.node.create_publisher(
            Float64,
            "~/debug/tick_frequency",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
            )
            self.ros_diagnostics_pub = self.node.create_publisher(
            DiagnosticArray,
            "/diagnostics",
            callback_group=self.publisher_callback_group,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
            )
        
        
            self.node.get_logger().warn(f"9999999999999999999999 cut this0 ! ")
            prepare_local_implementation_tree_manager = TreeManager(
                ros_node=self.node,   #### change 0604
                name="LocalBidStagingManager",
                publish_tree_callback=self.tree_pub.publish,#self.nop(),#self.nop(self,self.tree_pub.publish),#nop,
                publish_debug_info_callback=self.debug_info_pub.publish,#self.nop(),#self.nop(self,self.debug_info_pub.publish),#(self,None),
                publish_debug_settings_callback=self.debug_settings_pub.publish,#self.nop(),#self.nop(self,self.debug_settings_pub.publish),#(self,None),
                publish_node_diagnostics_callback=self.node_diagnostics_pub.publish,#self.nop(),#self.nop(self,self.node_diagnostics_pub.publish),#(self,None),
                publish_diagnostic_callback=self.ros_diagnostics_pub.publish,#self.nop(),
                publish_tick_frequency_callback=self.tick_frequency_pub.publish,
                #debug_manager=DebugManager(self.node,),
            )
            self.node.get_logger().warn(f"9999999999999999999999 cut this1 ! ")
            
            
            
            
            
            
            
            
            
            
            # prepare_local_implementation_tree_manager = TreeManager(
            #     name="LocalBidStagingManager",
            #     publish_tree_callback=nop,
            #     publish_debug_info_callback=nop,
            #     publish_debug_settings_callback=nop,
            #     publish_node_diagnostics_callback=nop,
            #     debug_manager=DebugManager(),
            # )
            
            
            
            
            
            
            #prepare_local_implementation_migration_manager :Optional[MigrationManager]= MigrationManager(#
            
            
            
            
            
            
            #     rosnode=self.node,
            #     tree_manager=prepare_local_implementation_tree_manager
            # )

            with self.__get_capability_implementations_proxy_lock:
                # implementations_response: GetCapabilityImplementations.Response = (
                #     self.__get_capability_implementations_proxy.call_async(  ### change 0701
                #         GetCapabilityImplementations.Request(interface=request.interface)
                #     ).result()  ### change 0701
                # )
                ### change 0701
                implementations_req=GetCapabilityImplementations.Request()
                implementations_req.interface=request.interface
                implementations_res=self.__get_capability_implementations_proxy.call_async(implementations_req)
                time_sum=0
                while not implementations_res.done():
                    time.sleep(0.1)
                    time_sum+=0.1
                    if time_sum>6.0:
                        implementations_response=None  ## add 0407
                        break
                    self.node.get_logger().warn(f"9999999999999999999999 cut this2 ! ")
                implementations_response=implementations_res.result()
            if implementations_response is not None:
                implementation: Optional[CapabilityImplementation] = next(
                    filter(
                        lambda x: x.name == request.implementation_name,
                        implementations_response.implementations,
                    ),
                    None,
                )
            else:
                implementation = None
            
            if implementation is None:
                implementation = implementations_response.implementations[0]
                self.node.get_logger().warn(f"999999 implementation for {request.implementation_name} : {implementation}")
            
            

            if implementation is None:
                response.success = False
                response.error_message = "Could not get local implementation!"
                return response
            
            tree: Tree = implementation.tree
            

            ### change 0701
            '''
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
            '''

            
            
            
            # service_response: LoadTree.Response = (
            #     prepare_local_implementation_tree_manager.load_tree(
            #         LoadTree.Request(tree=tree)
            #     )
            # )


            ### change 0701
            serv_req=LoadTree.Request(tree=tree)
            #serv_req.tree=tree
            serv_res=LoadTree.Response()
            service_response=prepare_local_implementation_tree_manager.load_tree(serv_req,serv_res)


            if not service_response.success:
                response.success = False
                response.error_message = (
                    f"Failed to load tree: {service_response.error_message}"
                )
                self.node.get_logger().error(response.error_message)
                return response

            root_node = prepare_local_implementation_tree_manager.find_root()
            root_sequence_node = Sequence(name="RootCapabilitySequence",ros_node=prepare_local_implementation_tree_manager.ros_node)
            


            ### change 0701
            # service_response1: AddNode.Response = (
            #     prepare_local_implementation_tree_manager.add_node(
            #         AddNode.Request(
            #             parent_name=None,
            #             node=root_sequence_node.to_msg(),
            #             allow_rename=False,
            #         )
            #     )
            # )
            serv_req1=AddNode.Request()
            serv_req1.parent_name=""   ### change 0701 关键！！！！！！！！！！！！！！！！！！！不能用None！
            self.node.get_logger().warn(f"9999999999999999999999 cut this3 ! ")
            serv_req1.node=root_sequence_node.to_msg()
            serv_req1.allow_rename=False
            serv_res1=AddNode.Response()
            self.node.get_logger().warn(f"9999999999999999999999 cut this {serv_req1} ! ")
            service_response1=prepare_local_implementation_tree_manager.add_node(serv_req1,serv_res1)
            self.node.get_logger().warn(f"9999999999999999999999 cut this4 ! ")




            # serv_req=RunTree.Request(tree=tree)
            # #serv_req.tree=tree
            # serv_res=LoadTree.Response()
            # service_response=prepare_local_implementation_tree_manager.load_tree(serv_req,serv_res)
            # if self._local_implementation_tree is not None:
            # with self._capability_lock:
            #     self._local_implementation_tree_root.shutdown()

            #     response: ControlTreeExecution.Response = self.tree_manager.control_execution(
            #         ControlTreeExecution.Request(
            #             command=ControlTreeExecution.Request.SHUTDOWN
            #         ),
            #         ControlTreeExecution.Response()  ### change 0701
            #     )
            #     if not response.success:
            #         self.logwarn(
            #             f"Could not shutdown local capability: {response.error_message}"
            #         )

            





            if not service_response1.success:
                response.success = False
                response.error_message = (
                    "Failed to add sequence for preconditions: "
                    f"{service_response1.error_message}"
                )
                self.node.get_logger().error(response.error_message)
                return response

            ### change 0701
            # service_response2: MoveNode.Response = (
            #     prepare_local_implementation_tree_manager.move_node(
            #         MoveNode.Request(
            #             node_name=root_node.name,
            #             new_parent_name=root_sequence_node.name,
            #             new_child_index=0,
            #         )
            #     )
            # )
            serv_req2=MoveNode.Request(
                        node_name=root_node.name,
                        new_parent_name=root_sequence_node.name,
                        new_child_index=0,
                    )
            serv_res2=MoveNode.Response()
            service_response2=prepare_local_implementation_tree_manager.move_node(serv_req2,serv_res2)
            self.node.get_logger().warn(f"9999999999999999999999 cut this5 ! ")

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
                    ##### change 0701
                    available_services = f"{self.node.get_namespace()}/mission_control/check_precondition_status"#rosidl_runtime_py.utilities.get_service("CheckPreconditionStatus")
                    # ！！！！和其他地方的get service list然后按照服务名在列表中查找不一样
                    # 原：rosservice_find("CheckPreconditionStatus")

                    #for service_name in available_services:
                    
                    ### change 0701
                    service_client = self.node.create_client(
                            CheckPreconditionStatus, available_services,
                            qos_profile=QoSProfile(
                            reliability=QoSReliabilityPolicy.RELIABLE,
                            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                            history=QoSHistoryPolicy.KEEP_LAST,
                            depth=10),
                            callback_group=ReentrantCallbackGroup()
                        )
                    service_client.wait_for_service(1.0)
                    service_response3: CheckPreconditionStatus.Response = (
                            service_client(
                                CheckPreconditionStatus.Request(
                                    interface=precondition.capability
                                )
                            )
                        )
                    self.node.get_logger().warn(f"9999999999999999999999 cut this5 ! ")
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
        self, requests: GetAvailableRemoteCapabilitySlots.Request, response: GetAvailableRemoteCapabilitySlots.Response   ### change 0701
    ) -> GetAvailableRemoteCapabilitySlots.Response:
        """
        Return the amount of available remote capability slots.

        :param requests: None
        :return: Amount of available remote capability slots.
        """
        response = GetAvailableRemoteCapabilitySlots.Response()
        self.node.get_logger().warn(f"444444444444444444 start get available remote cap slot!")
        self.node.get_logger().warn(f"from slot list: {self._remote_capability_slot_status}")
        response.available_remote_capability_slots = len(
            list(
                filter(
                    lambda x: self._remote_capability_slot_status[x].status
                    == RemoteCapabilitySlotStatus.IDLE
                    and not (
                        self._remote_capability_slot_status[x].reserved
                        and self.node.get_clock().now().nanoseconds ####Clock.now()
                        < self._remote_capability_slot_status[x].timestamp.nanosec+self._remote_capability_slot_status[x].timestamp.sec*1000000000
                        + self._remote_capability_slot_reservation_duration.nanoseconds
                    ),
                    self._remote_capability_slot_status,
                )
            )
        )
        self.node.get_logger().warn(f"444444444444444444 available cap slot: {response.available_remote_capability_slots}")
        return response
    
def nop(msg):
    """
    No operation method.

    :return: None
    """
