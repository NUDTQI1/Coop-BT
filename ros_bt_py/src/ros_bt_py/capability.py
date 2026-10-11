import secrets
import threading
from abc import ABC
from typing import Optional, Dict, List, Type

import rclpy
import rclpy.client
from rclpy.node import Node#
import rclpy.node
from rclpy.parameter import Parameter#
from rclpy.subscription import Subscription#
from rclpy.publisher import Publisher#
from rclpy.qos import QoSProfile, QoSDurabilityPolicy#
from rclpy.logging import LoggingSeverity#
from rclpy.duration import Duration
from rclpy.action import ActionClient
from rclpy.client import Client
from rclpy.task import Future
from threading import Thread
from builtin_interfaces.msg import Time
# from rclpy.time import Time
import rclpy.time
import time
from rclpy.exceptions import ROSInterruptException
from action_msgs.msg import GoalStatus

import rclpy.waitable
from ros_bt_py_interfaces.msg import (
    CapabilityInterface,
    Node as NodeMsg,
    Tree,
    CapabilityIOBridgeData,
    CapabilityExecutionStatus,
    NodeData as NodeDataMsg,
    UtilityBounds,
    PingMsg,
)

####### change 0604-2
from ros_bt_py_interfaces.msg import (
    Tree,
    DebugInfo,
    DebugSettings,
    NodeDiagnostics,
    Messages,
    Packages,
    DistanceCalculation
)
from std_msgs.msg import Float64
from rclpy.node import ReentrantCallbackGroup
from diagnostic_msgs.msg import DiagnosticArray
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)
####### change 0604-2

from ros_bt_py_interfaces.action import ExecuteRemoteCapability

from ros_bt_py_interfaces.srv import (
    PrepareLocalImplementation,
    LoadTree,
    ClearTree,
    ControlTreeExecution,
    GetCapabilityInterfaces,
    RequestCapabilityExecution,
    GetLocalBid,
)

from ros_bt_py.debug_manager import DebugManager
from ros_bt_py.exceptions import BehaviorTreeException, TreeTopologyError
from ros_bt_py.helpers import json_decode, json_encode, rgetattr
from ros_bt_py.node import define_bt_node, Leaf, Node as Node1
from ros_bt_py.node_data import NodeData
from ros_bt_py.node_config import NodeConfig
from ros_bt_py.ros_helpers import AsyncServiceProxy #这里还没有适配
from ros_bt_py.tree_manager import TreeManager
from rclpy.action.server import ServerGoalHandle
from action_msgs.msg import GoalInfo
import uuid

class CapabilityDataBridge(ABC, Leaf):#(Node, ABC, Leaf):  #change 5.21
    """Data bridge that allows the exchange of data between capability interfaces and nodes."""

    def __init__(
        self,
        #change 5.21
        #node_name: str,#这里新增了一个node_name，用于传递ros启动时节点名，子类实例化时自动把这个值补全
        ros_node=None, ######## change 0604, 对照ros1中IO Bridge和Node初始化的init参数对应关系
        debug_manager=None,
        name=None,
        options=None,
        simulate_tick=False,
        succeed_always=False,
    ):
        """
        Create a new capability data bridge node.

        :param node_name: The name of the ROS2 node.
        :param debug_manager: The debug manager to use.
        :param name: The name of the capability data bridge.
        :param options: The node options.
        """
        # 涉及多个类继承问题，所以显式初始化构造函数
        #Node().__init__(node_name)   #change 5.21
        
        # ABC().__init__()   ###### change 0604
        #Leaf().__init__(   ###### change 0604
        super(CapabilityDataBridge,self).__init__(
            #ros_node=self.ros_node,    ## change 1 #self 里有ros_node    #change 5.21
            ros_node=ros_node,  ######## change 0604, 对照ros1中IO Bridge和Node初始化的init参数对应关系
            options=options,
            debug_manager=debug_manager,
            name=name,
            simulate_tick=simulate_tick,
            succeed_always=succeed_always,
        )
        
        #self._ros_node: Optional[Node] = ros_node

        # Initialize the IO bridge ID
        self._io_bridge_id: Optional[str] = None

        # Declare and get the ROS2 parameter for the capability IO topic
        #self.ros_node.declare_parameter("capability_io_topic", "~/capabilities/io")
        #rclpy.node.Parameter("capability_io_topic", str, "~/capabilities/io")
        self.capability_io_topic: str=f"{self.ros_node.get_namespace()}/capabilities/io"#### change 0604 # self.ros_node.get_parameter("capability_io_topic").value
        
        self.ros_node.get_logger().warn(f"{self.capability_io_topic}, {self.ros_node.get_name()}")

    @property
    def capability_bridge_id(self) -> str:
        return self._io_bridge_id

    @capability_bridge_id.setter
    def capability_bridge_id(self, new_id: str):
        self._io_bridge_id = new_id

#@define_bt_node(NodeConfig(options={}, inputs={}, outputs={}, max_children=0))#add
class CapabilityInputDataBridge(CapabilityDataBridge):
    """CapabilityDataBridge that transfers the interface inputs to the implementations inputs."""

    def __init__(
        self,
        #change 5.21
        #node_name: str,#这里新增了一个node_name，用于传递ros启动时节点名，实例化这个类时，需要将这个参数传入，这是有别于ros1的地方
        ros_node:Optional[Node]=None,  ######## change 0604
        options: Optional[Dict] = None,
        debug_manager: Optional[DebugManager] = None,
        name: str = None,
        succeed_always: bool = False,
        simulate_tick: bool = False,
    ):
        super().__init__(
            #node_name=node_name,    #change 5.21
            ros_node=ros_node,  ######## change 0604
            options=options,
            debug_manager=debug_manager,
            name=name,
            simulate_tick=simulate_tick,
            succeed_always=succeed_always
        )

        #self._ros_node: Optional[Node] = ros_node
        self._source_capability_inputs_subscriber: Optional[Subscription] = None
        self._current_received_msg: Optional[CapabilityIOBridgeData] = None
        self._lock = threading.RLock()
        self._message_lock = threading.RLock()

    def _publish_inputs_cb(self, msg: CapabilityIOBridgeData):
        """
        Callback to store the received input message internally to be processed on the next tick.

        :param msg: The received message.
        """
        with self._message_lock:
            if (msg.node_id == self.capability_bridge_id) and (
                msg.type == CapabilityIOBridgeData.INPUT
            ):
                self.ros_node.get_logger().info(f"Received msg relevant to {self.name} input bridge!")#debug
                self._current_received_msg = msg

    def _do_setup(self):
        with self._lock:
            qos = QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=50)#QoSProfile(depth=10)
            self._source_capability_inputs_subscriber = self.ros_node.create_subscription(
                CapabilityIOBridgeData,
                self.capability_io_topic,
                self._publish_inputs_cb,
                qos,
                callback_group=ReentrantCallbackGroup()
            )
            self.ros_node.get_logger().warn(f"ros node:{self.ros_node.get_name()}, id:{self.capability_bridge_id}. io_topic:{self.capability_io_topic}")
            self.ros_node.get_logger().warn(f" 4444444444444444444 subscriber:{self.ros_node.get_clock().now()}")#{self._source_capability_inputs_subscriber.callback}")
            try:
                self.ros_node.get_logger().info("Waiting for initial message...")
                #rclpy.spin_once(self, timeout_sec=10)     #### change 0701  完全不确定！！！！！！
            except Exception as exc:
                self.ros_node.get_logger().warn(
                    f"Failed to receive an initial message for the input bridge: {self.ros_node.get_name()}"
                )
                raise BehaviorTreeException(
                    f"Input bridge {self.ros_node.get_name()} messages not populated!"
                ) from exc

            while self._current_received_msg is None:
                self.ros_node.get_logger().warn(f" 4444444444444444444 {self.name} input current_received_msg is None!")
                time.sleep(0.5)#rclpy.spin_once(self, timeout_sec=0.1)     #### change 0701  完全不确定！！！！！！

            for bridge_data in self._current_received_msg.bridge_data:
                node_data: NodeDataMsg = bridge_data
                try:
                    self.outputs[node_data.key] = json_decode(
                        node_data.serialized_value
                    )
                    self.ros_node.get_logger().warn(f" 4444444444444444444 current_msg:{self._current_received_msg.timestamp}")
                except TypeError as exc:
                    self.ros_node.get_logger().warn(f"Could not set output {node_data.key}: {exc}")
                    continue
            self._handle_outputs()

    def _do_tick(self):
        with self._lock:
            if self.state == NodeMsg.IDLE:
                self.state = NodeMsg.RUNNING

            if self.state == NodeMsg.RUNNING:
                with self._message_lock:
                    if self._current_received_msg is not None:
                        for bridge_data in self._current_received_msg.bridge_data:
                            node_data: NodeDataMsg = bridge_data
                            try:
                                self.outputs[node_data.key] = json_decode(
                                    node_data.serialized_value
                                )
                            except TypeError as exc:
                                self.ros_node.get_logger().warn(
                                    f"Could not set output {node_data.key}: {exc}"
                                )
                                continue

                        return NodeMsg.SUCCEEDED
                    return NodeMsg.RUNNING
            return self.state

    def _do_untick(self):
        with self._lock:
            with self._message_lock:
                self._current_received_msg = None
            return NodeMsg.IDLE

    def _do_reset(self):
        with self._lock:
            with self._message_lock:
                self._current_received_msg = None
                #self.ros_node.destroy_subscription(self._source_capability_inputs_subscriber)
                self._source_capability_inputs_subscriber = None
            return NodeMsg.IDLE

    def _do_shutdown(self):
        with self._lock:
            with self._message_lock:
                self._current_received_msg = None

            if self._source_capability_inputs_subscriber is not None:
                #self.ros_node.destroy_subscription(self._source_capability_inputs_subscriber)#ros2中订阅需要销毁，发布不需要
                self._source_capability_inputs_subscriber = None

#@define_bt_node(NodeConfig(options={}, inputs={}, outputs={}, max_children=0))#add
class CapabilityOutputDataBridge(CapabilityDataBridge):

    def __init__(
        self,
        #change 5.21
        #node_name: str,#这里新增了一个node_name，用于传递ros启动时节点名，实例化这个类时，需要将这个参数传入，这是有别于ros1的地方
        ros_node: Optional[Node]=None,  ######## change 0604
        options: Optional[Dict] = None,
        debug_manager: Optional[DebugManager] = None,
        name: str = None,
        succeed_always: bool = False,
        simulate_tick: bool = False,
    ):
        super().__init__(
            #node_name=node_name,    #change 5.21
            ros_node=ros_node,  ######## change 0604
            options=options,
            debug_manager=debug_manager,
            name=name,
            succeed_always=succeed_always,
            simulate_tick=simulate_tick,
        )
        #self._ros_node: Optional[Node] = ros_node
        self._source_capability_outputs_publisher: Optional[Publisher] = None
        self._lock = threading.RLock()

    def _do_setup(self):
        with self._lock:
            self._source_capability_outputs_publisher = self.ros_node.create_publisher(CapabilityIOBridgeData, self.capability_io_topic, qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10),
                callback_group=ReentrantCallbackGroup())#10)

    def _do_tick(self):
        with self._lock:
            ###################################################################################
            # if self.state == NodeMsg.IDLE:
            #     return NodeMsg.RUNNING
            # if self.state == NodeMsg.RUNNING:
            ###################################################################################
            if self.state == NodeMsg.IDLE:
                if self._source_capability_outputs_publisher is not None:
                    outputs = []
                    for key in self.inputs:
                        output = NodeDataMsg()
                        output.key = key
                        output.serialized_value = self.inputs.get_serialized(key=key)
                        output.serialized_type = self.inputs.get_serialized_type(key=key)
                        outputs.append(output)

                    msg = CapabilityIOBridgeData(
                        node_id=self.capability_bridge_id,
                        type=CapabilityIOBridgeData.OUTPUT,
                        bridge_data=outputs,
                        timestamp=self.ros_node.get_clock().now().to_msg(),
                    )
                    self._source_capability_outputs_publisher.publish(msg)
                    return NodeMsg.SUCCEEDED
                return NodeMsg.FAILED
            return NodeMsg.FAILED

    def _do_untick(self):
        with self._lock:
            return NodeMsg.IDLE

    def _do_reset(self):
        with self._lock:
            return NodeMsg.IDLE

    def _do_shutdown(self):
        with self._lock:
            if self._source_capability_outputs_publisher is not None:
                self._source_capability_outputs_publisher = None#ros2中订阅需要销毁，发布不需要，只需要给None就行

#@define_bt_node(NodeConfig(options={}, inputs={}, outputs={}, max_children=0))#add
class Capability(ABC, Leaf):#(Node, ABC, Leaf ):    #change 5.21
    IDLE = 0
    WAITING_FOR_ASSIGNMENT = 1
    EXECUTE_LOCAL = 2
    EXECUTE_REMOTE = 3
    WAITING_FOR_OUTPUT_VALUES = 4

    def __init__(
        self,
        #node_name: str,    #change 5.21
        ros_node: Optional[Node] = None,  ######## change 0604
        options: Optional[Dict] = None,
        debug_manager: Optional[DebugManager] = None,
        name: str = None,
        succeed_always: bool = False,
        simulate_tick: bool = False,
    ):  
        #Node().__init__(node_name)    #change 5.21
        #ABC().__init__()     ###### change 0604
        #Leaf().__init__(   ###### change 0604
        super().__init__(
            #ros_node=self.ros_node,    ## change 1 #self 里有ros_node    #change 5.21
            options=options,
            debug_manager=debug_manager,
            name=name,
            ros_node=ros_node,  ######## change 0604
            simulate_tick=simulate_tick,
            succeed_always=succeed_always,
        )

        #self._ros_node=self.ros_node

        '''
        self.capability_io_topic = self.ros_node.declare_parameter("capability_io_topic", "~/capabilities/io").get_parameter_value().string_value

        self.wait_for_local_mission_control_timeout_sec = self.ros_node.declare_parameter("wait_for_local_mission_control_timeout_sec", 2).get_parameter_value().integer_value

        self.wait_for_implementation_assignment_timeout_sec = self.ros_node.declare_parameter("wait_for_implementation_assignment_timeout_sec", 2 ).get_parameter_value().integer_value

        self.wait_for_remote_mission_control_tries = self.ros_node.declare_parameter( "wait_for_remote_mission_control_tries", 3).get_parameter_value().string_value

        self.wait_for_outputs_timeout_sec = self.ros_node.declare_parameter("wait_for_outputs_timeout_sec", 1 ).get_parameter_value().string_value

        '''
        self.capability_io_topic:str = f"{self.ros_node.get_namespace()}/capabilities/io"

        self.wait_for_local_mission_control_timeout_sec:int = 5#2

        self.wait_for_implementation_assignment_timeout_sec:int = 5#2

        self.wait_for_remote_mission_control_tries:int = 6#3

        self.wait_for_outputs_timeout_sec:int = 15#1

        

          # 检查并初始化 capability_interface
        if hasattr(self, "__capability_interface"):
            self.capability_interface = getattr(self, "__capability_interface")
        else:
            self.ros_node.get_logger().warning(
                "Created Capability without interface! This is not intended!",
                severity=LoggingSeverity.WARN,
            )
            raise BehaviorTreeException(
                "Created Capability without interface! This is not intended!"
            )

        # 检查并初始化 local_mc_topic
        if hasattr(self, "__local_mc_topic"):
            self.local_mc_topic = f"{self.ros_node.get_namespace()}/mission_control" # f"/turtle0/mission_control"   ##### change 0701
            self.ros_node.get_logger().warning(f"{self.local_mc_topic}")#getattr(self, "__local_mc_topic")
        else:
            self.ros_node.get_logger().warning(
                "Created Capability without local_mc_topic! This is not intended!",
                severity=LoggingSeverity.WARN,
            )
            raise BehaviorTreeException(
                "Created Capability without local_mc_topic! This is not intended!"
            )

        self._internal_state = self.IDLE
        
        ########### change 0604-2
        self.publisher_callback_group = ReentrantCallbackGroup()
        self.tree_pub = self.ros_node.create_publisher(
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
        self.debug_info_pub = self.ros_node.create_publisher(
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
        self.debug_settings_pub = self.ros_node.create_publisher(
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
        self.node_diagnostics_pub = self.ros_node.create_publisher(
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
        self.tick_frequency_pub = self.ros_node.create_publisher(
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
        self.ros_diagnostics_pub = self.ros_node.create_publisher(
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
        

        self.tree_manager = TreeManager(
            ros_node=self.ros_node,   #### change 0604
            name=name,
            publish_tree_callback=self.tree_pub.publish,#self.nop(),#self.nop(self,self.tree_pub.publish),#nop,
            publish_debug_info_callback=self.debug_info_pub.publish,#self.nop(),#self.nop(self,self.debug_info_pub.publish),#(self,None),
            publish_debug_settings_callback=self.debug_settings_pub.publish,#self.nop(),#self.nop(self,self.debug_settings_pub.publish),#(self,None),
            publish_node_diagnostics_callback=self.node_diagnostics_pub.publish,#self.nop(),#self.nop(self,self.node_diagnostics_pub.publish),#(self,None),
            publish_diagnostic_callback=self.ros_diagnostics_pub.publish,#self.nop(),
            publish_tick_frequency_callback=self.tick_frequency_pub.publish,#self.nop(),
            debug_manager=self.debug_manager,
        )


        self._old_state = self.state
        self._capability_lock = threading.RLock()


        self._implementation_name: Optional[str] = None

        # Action and service clients
        self._request_capability_execution_service_proxy: Optional[Client]=None#Optional[AsyncServiceProxy] = None
        self._prepare_local_implementation_service_proxy: Optional[Client]=None#Optional[AsyncServiceProxy] = None
        self._execute_capability_remote_client: Optional[ActionClient] = None
        self._capability_execution_status_publisher: Optional[Publisher] = None

        # Input and output data bridge information
        self._io_bridge_id: Optional[str] = None
        self._io_publisher: Optional[Publisher] = None
        self._io_subscriber: Optional[Subscription] = None
        self._last_received_outputs: Optional[CapabilityIOBridgeData] = None

        # Ping used to indicate that a remote capability is still running!
        self._ping_subscriber: Optional[Subscription] = None
        self._last_ping_timestamp: Optional[Time] = None

        # Result variables used to track if the implementation is finished
        self._result_status: Optional[str] = None
        self._result_timestamp: Optional[Time] = None
        # Variables for remote trees
        self._remote_mission_control_topic: Optional[str] = None
        self._execute_capability_remote_client_connection_tries: int = 0
        # Variables for local trees
        self._prepare_local_tree_thread: Optional[Thread] = None
        self._prepare_local_implementation_start_time: Optional[Time] = None

        self._local_implementation_tree: Optional[Tree] = None
        self._local_implementation_tree_root: Optional[Node1] = None
        self._setup_local_implementation_tree_status: str = NodeMsg.IDLE

        local_bid_service_topic = f"{self.local_mc_topic}/get_local_bid"
        self.ros_node.get_logger().warning("successs init capability111")
        self.__get_local_bid_service = self.ros_node.create_client(GetLocalBid, local_bid_service_topic)
        #self.__get_local_bid_service.wait_for_service()

        #为了一直action新增；变量
        self.execute_capability_remote_client_furture: Optional[Future] = None
        self.execute_capability_result: Optional[ExecuteRemoteCapability] = None
        self.has_execute_capability_result = False
        self.wait_execute_capability = False
        self.execute_capability_handle = None
        ### add 0312 为了让找不到本地实现的cap恢复remote exe
        self.find_local_impl = True

        self.ros_node.get_logger().warning(f"successs init capability222, inputs: {self.inputs}")


    def _do_calculate_utility(self):
        if self._io_publisher is not None:
            input_data = []
            for key in self.inputs:
                input_data_msg = NodeDataMsg()
                input_data_msg.key = key
                input_data_msg.serialized_value = self.inputs.get_serialized(key=key)
                input_data_msg.serialized_type = self.inputs.get_serialized_type(
                    key=key
                )
                input_data.append(input_data_msg)

            try:
                self._io_publisher.publish(
                    CapabilityIOBridgeData(
                        node_id=self._io_bridge_id,
                        type=CapabilityIOBridgeData.INPUT,
                        bridge_data=input_data,
                        timestamp=self.ros_node.get_clock().now() ,
                    )
                )
            except ROSInterruptException as exc:
                error_msg = f"Input data bridge publisher could not publish: {exc}"
                self.ros_node.get_logger().error(error_msg)
                raise RuntimeError(error_msg) from exc
            except Exception as exc:
                error_msg = f"Could not serialize inputs to publish: {exc}"
                self.ros_node.get_logger().error(error_msg)
                raise RuntimeError(error_msg) from exc

        # Call the service asynchronously
        request = GetLocalBid.Request(
            interface=self.capability_interface, node_id=self._io_bridge_id
        )
        future = self.__get_local_bid_service.call_async(request)

        # Wait for the response (can also use a callback approach)
        rclpy.spin_until_future_complete(self, future)

        if future.result() is None or not future.done():
            error_message = "No response"
            self.ros_node.get_logger().error(
                f"Could not calculate the utility value for the capability {self.name}: {error_message}"
            )
            return UtilityBounds(can_execute=False)

        response: GetLocalBid.Response = future.result() #这里我把response显式告知属于GetLocalBid.Response，否则下面编译器无法识别response.bid,future.result()返回是一个字典，包括result以及status
        return UtilityBounds(
            can_execute=True,
            has_lower_bound_success=True,
            lower_bound_success=0.0,
            has_upper_bound_success=True,
            upper_bound_success=response.bid,
            has_lower_bound_failure=True,
            lower_bound_failure=0.0,
            has_upper_bound_failure=True,
            upper_bound_failure=response.bid,
        )

    @staticmethod
    def nop(self, msg):#def nop(*args, **kwargs):
        """No operation callback."""
        pass

    @staticmethod
    def __handle_async_service_call(service_proxy: AsyncServiceProxy):#AsyncServiceProxy还没适配
        """
        Helper method to manage a call to a service via the AsyncServiceProxy.
        The call method needs to be called beforehand,
        this method just handles the possible states until the result is received.
        It checks the states the service_proxy can be in and returns results depending
        on the current execution state.
        This method in nonblocking.

        :param service_proxy: The service proxy the call has been placed for.

        :raises BehaviorTreeException: Should the call to the AsyncServiceProxy be aborted
        or not placed before calling
        this method a BehaviorTreeException will be raised.
        :return: The response of the call if available, otherwise None.
        """
        state = service_proxy.get_state()

        if state in [AsyncServiceProxy.IDLE, AsyncServiceProxy.SERVICE_AVAILABLE]:
            raise BehaviorTreeException(f"Service is still idle (state ID: {state})")

        if state is AsyncServiceProxy.TIMEOUT:
            raise BehaviorTreeException(
                f"Service call timed out before succeeding (state ID: {state})"
            )

        if state is AsyncServiceProxy.ABORTED:
            raise BehaviorTreeException(
                f"Service call was aborted before succeeding (state ID: {state})"
            )

        if state is AsyncServiceProxy.ERROR:
            raise BehaviorTreeException(
                f"Service call returned a error before succeeding (state ID: {state})"
            )

        if state is AsyncServiceProxy.RUNNING:
            return None

        if state is AsyncServiceProxy.RESPONSE_READY:
            AsyncServiceProxy.get_logger().warn("getting response 111111111111") ### change 0701 下一行
            response = service_proxy.get_response()#response: PrepareLocalImplementation.Response = service_proxy.get_response() #显式定义response，这样能识别response.success
            if response.success:
                return response
            raise BehaviorTreeException(
                f"Service call failed {response.error_message})"
            )
        raise RuntimeError(f"Async proxy in invalid state {state})")
    
    
    def _execute_action(self, future: Future):
        # 自己重新设计函数，异步获取action结果

        # 当第一次进来时，需要通过下面函数等待结果，结果未出来时，不再次运行这一步
        if not self.wait_execute_capability:
            self.ros_node.get_logger().warn(f"111117777777 {future.result()}")
            future.add_done_callback(self.execute_future_callback)
            self.wait_execute_capability = True

        # while self.wait_execute_capability:   ### change
        #     if self.has_execute_capability_result:
        #         self.ros_node.get_logger().warn(f"111117777777 has execute cap result")
        #         result = self.execute_capability_result.result
        #         status = self.execute_capability_result.status
        #         # 重置状态量
        #         self.wait_execute_capability = False
        #         self.has_execute_capability_result = False
        #         self.execute_capability_result = None
        #         self.wait_execute_capability = False ### change


        #         # if status in [GoalStatus.STATUS_ABORTED, GoalStatus.STATUS_UNKNOWN, GoalStatus.STATUS_ACCEPTED]:
        #         #     raise RuntimeError(f"Action ended without succeeding (state ID: {status})")

        #         # if status in [GoalStatus.STATUS_CANCELED, GoalStatus.STATUS_EXECUTING, GoalStatus.STATUS_CANCELING]:
        #         #     raise RuntimeError(f"Action was canceled before succeeding (state ID: {status})")

        #         if status == GoalStatus.STATUS_SUCCEEDED:
        #             self.ros_node.get_logger().warn(f"111117777777 succeed")
        #     #if self.execute_capability_result is not None:
        #             return result
        #         #return self.execute_capability_result
        #     return None
        if self.has_execute_capability_result:
                self.ros_node.get_logger().warn(f"111117777777 has execute cap result")
                result = self.execute_capability_result.result
                status = self.execute_capability_result.status
                # 重置状态量
                self.wait_execute_capability = False
                self.has_execute_capability_result = False
                self.execute_capability_result = None
                self.wait_execute_capability = False ### change


                # if status in [GoalStatus.STATUS_ABORTED, GoalStatus.STATUS_UNKNOWN, GoalStatus.STATUS_ACCEPTED]:
                #     raise RuntimeError(f"Action ended without succeeding (state ID: {status})")

                # if status in [GoalStatus.STATUS_CANCELED, GoalStatus.STATUS_EXECUTING, GoalStatus.STATUS_CANCELING]:
                #     raise RuntimeError(f"Action was canceled before succeeding (state ID: {status})")

                if status == GoalStatus.STATUS_SUCCEEDED:
                    self.ros_node.get_logger().warn(f"111117777777 succeed")
            #if self.execute_capability_result is not None:
                    return result
                else:#if status in [GoalStatus.STATUS_CANCELED, GoalStatus.STATUS_CANCELING]:
                    self.ros_node.get_logger().warn(f"111117777777 failed")
            #if self.execute_capability_result is not None:
                    return result
                #return self.execute_capability_result
        return None

    def execute_future_callback(self, future: Future):
        goal_handle = future.result()

        self.execute_capability_handle = goal_handle

        future = goal_handle.get_result_async()

        future.add_done_callback(self._execute_result_callback)

    def _execute_result_callback(self, future: Future):
        res = future.result()
        self.ros_node.get_logger().warn(f"1111166666 {res}")
        self.has_execute_capability_result = True
        self.execute_capability_result = res

    def _publish_outputs_cb(self, msg: CapabilityIOBridgeData):
        """
        Callback to store the received output values locally and allow them to be
        processed in the next tick.
        :param msg: The received output values.
        :return: None
        """
        # 筛选符合条件的消息
        if (msg.type == CapabilityIOBridgeData.OUTPUT and msg.node_id == self._io_bridge_id):
            # 加锁以确保线程安全
            with self._capability_lock:
                self._last_received_outputs = msg
                self.ros_node.get_logger().info("Output data received and stored.")  # 日志记录
                self.ros_node.get_logger().info(f"Output data: {msg}")







    def _cap_exe_allocation_cb(self, msg: DistanceCalculation):
        """
        Callback to store the received output values locally and allow them to be
        processed in the next tick.
        :param msg: The received output values.
        :return: None
        """
        # 筛选符合条件的消息
        if (msg.own_name == self.ros_node.get_namespace()):
            # 加锁以确保线程安全
            self.ros_node.get_logger().warn(f"1111111111116666666666666111111111111111:{msg.own_name},{msg.other_name}")
            self.allocated_cap_name = msg.other_name
        else:
            self.allocated_cap_name = None









    def _do_setup(self):
        try:
            # 获取并等待 prepare_local_implementation 服务
            prepare_local_implementation_name = f"{self.local_mc_topic}/prepare_local_implementation"

            ##### change 0701
            #self._prepare_local_implementation_service_proxy = AsyncServiceProxy(#self.ros_node.create_client(
            #    self.ros_node, prepare_local_implementation_name,PrepareLocalImplementation, 
            #     qos_profile=QoSProfile(
            #     reliability=QoSReliabilityPolicy.RELIABLE,
            #     durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            #     history=QoSHistoryPolicy.KEEP_LAST,
            #     depth=10,
            # ),
            # callback_group=ReentrantCallbackGroup()
            #)
            ##### change 0701 第二版
            self._prepare_local_implementation_service_proxy = self.ros_node.create_client(
                PrepareLocalImplementation, prepare_local_implementation_name,
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
                ),
                callback_group=ReentrantCallbackGroup()
            )

            
            self.ros_node.get_logger().warn(f"start service : {prepare_local_implementation_name}")

            if not self._prepare_local_implementation_service_proxy.wait_for_service(
                #timeout=self.wait_for_local_mission_control_timeout_sec
                timeout_sec=self.wait_for_local_mission_control_timeout_sec   #### change 0701
            ):
                raise TimeoutError(f"Service '{prepare_local_implementation_name}' not available")

            # 获取并等待 execute_capability 服务
            #### change 0701
            # self._request_capability_execution_service_proxy = AsyncServiceProxy(#self.ros_node.create_client(
            #     self.ros_node, f"{self.local_mc_topic}/execute_capability", RequestCapabilityExecution
            # )
            self._request_capability_execution_service_proxy = self.ros_node.create_client(
                RequestCapabilityExecution, 
                f"{self.local_mc_topic}/execute_capability",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
                ),
                callback_group=ReentrantCallbackGroup()
            )

            if not self._request_capability_execution_service_proxy.wait_for_service(
                #timeout=self.wait_for_local_mission_control_timeout_sec
                timeout_sec=self.wait_for_local_mission_control_timeout_sec    #### change 0701
            ):
                raise TimeoutError(f"Service '{self.local_mc_topic}/execute_capability' not available")
            else:
                self.ros_node.get_logger().warn(f"Service {self._request_capability_execution_service_proxy.srv_name} is ready!!!!!!!!!")

        except TimeoutError as exc:
            raise BehaviorTreeException(f"Service setup failed: {str(exc)}") from exc
        










        

        self._io_bridge_id = f"{self.name}" #_{secrets.randbelow(100000)} " change 0701
        self.ros_node.get_logger().warn(f"3333333333 {self.name}")






        self.allocated_cap_name = None
        self.coop_msg_sub = self.ros_node.create_subscription(
            DistanceCalculation,
            "~/capability_exe_allocation",
            self._cap_exe_allocation_cb,
            QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10),
            callback_group=ReentrantCallbackGroup()
            )















        
        # Create publishers and subscribers
        qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=50)
        self._io_publisher = self.ros_node.create_publisher(CapabilityIOBridgeData, self.capability_io_topic, qos_profile,callback_group=ReentrantCallbackGroup())#1)  ## change 0701
        self._output_bridge_subscriber = self.ros_node.create_subscription(
            CapabilityIOBridgeData, self.capability_io_topic, self._publish_outputs_cb, qos_profile,callback_group=ReentrantCallbackGroup()#1  ## change 0701
        )
        self._ping_subscriber = self.ros_node.create_subscription(
            PingMsg, '~/capabilities/ping', self._update_remote_exec_ping, qos_profile,callback_group=ReentrantCallbackGroup()#1  ## change 0701#1
        )

        # Create a publisher for capability execution status
        ### change 0701   ！！！！！！关键 ！！！！！！！！(改回来了，也能正常执行)
        notify_capability_execution_status_topic = self.ros_node.get_name() + '/notify_capability_execution_status'
        #notify_capability_execution_status_topic = self.ros_node.get_namespace() + '/mission_control/notify_capability_execution_status'
        self._capability_execution_status_publisher = self.ros_node.create_publisher(
            CapabilityExecutionStatus, notify_capability_execution_status_topic, qos_profile,callback_group=ReentrantCallbackGroup()# 10  ### change 0701
        )
        self.ros_node.get_logger().warn(f" Start Publish {notify_capability_execution_status_topic}")

    def _update_remote_exec_ping(self, msg: PingMsg):
        if self._io_bridge_id == msg.node_id:
            with self._capability_lock:
                self._last_ping_timestamp = msg.timestamp

    def _tick_unassigned(self) -> str:
        """
        Function performing the tick operation while in the UNASSIGNED state.
        :return: The new state as a NodeMsg constant.
        """
        self.ros_node.get_logger().warn(f"test1 unassigned, self name: {self.name}")
        if self._internal_state == self.IDLE:
            try:
                self.ros_node.get_logger().warn(f"test1 unassigned, self name: {self.name}")

                ####################################################################################################
                if self.options["require_coop"]:
                    time.sleep(0.1)
                    self.ros_node.get_logger().warn(f"look for local exe:{self.name}, symbol:{self.allocated_cap_name}")
                    require_local_execution = self.allocated_cap_name
                    time_sum = 0
                    while require_local_execution is None:
                        time.sleep(0.2)
                        require_local_execution = self.allocated_cap_name
                        self.ros_node.get_logger().warn(f"require_local_exe:{require_local_execution}")
                        time_sum = time_sum+0.2
                        if time_sum>0.6:
                            return NodeMsg.UNASSIGNED
                    if require_local_execution == self.name:
                        require_local_execution = True
                        self.ros_node.get_logger().warn(f"33333333333 start coop cap exe !!!")
                    else:
                        return NodeMsg.UNASSIGNED#require_local_execution = False

                ####################################################################################################
                else:
                    ### add 0312
                    if self.find_local_impl is True:
                        require_local_execution = self.options["require_local_execution"]
                    else:
                        require_local_execution = False
            except KeyError:
                require_local_execution = False

            tags: List[str] = self.options["inputs_names_for_implementation_tags"]
            implementation_tags_dict = dict()
            for tag in tags:
                input_tags = tag.split(".", 1)
                if len(input_tags) < 1:
                    self.ros_node.get_logger().error(f"Specified attribute {tag} is not valid")
                    self._cleanup()
                    return NodeMsg.FAILED

                input = input_tags[0]
                try:
                    input_value = self.inputs[input]
                except KeyError:
                    self.ros_node.get_logger().error(f"Specified attribute {tag} could not be found on inputs")
                    self._cleanup()

                if len(input_tags) == 2 and input_tags[1] != "":
                    try:
                        implementation_tags_dict[tag] = rgetattr(input_value, input_tags[1])
                    except AttributeError:
                        self.ros_node.get_logger().error(f"Specified attribute {input_tags[1]} could not be found on inputs")
                        self._cleanup()
                        return NodeMsg.FAILED
                else:
                    implementation_tags_dict[tag] = input_value

            new_request = RequestCapabilityExecution.Request()
            new_request.capability = self.capability_interface
            new_request.node_id = self._io_bridge_id
            new_request.require_local_execution = require_local_execution
            new_request.implementation_tags_dict = json_encode(implementation_tags_dict)

            rclpy.node.get_logger('capability').info(f" 22222 request:{new_request}")
            self.new_response = RequestCapabilityExecution.Response()
            future = self._request_capability_execution_service_proxy.call_async(new_request)#call_service(request)#  #### change 0701
            while not future.done():
                time.sleep(3.0)
                rclpy.node.get_logger('capability').info(f"222222 {self.new_response, future, future.result()}")
                #self._request_capability_execution_service_proxy.call_async(request)
            self.new_response=future.result()

            self.ros_node.get_logger().warn(f"tick unassigned: request 5555555555 {new_request}, request cap exe 1111 {future.result()}")
            ### add 0312
            if future.result().error_message == 'Could not find a remote helper!':
                return NodeMsg.FAILED
            if future.result().execute_local is False and future.result().remote_mission_controller_topic == '':
                self.find_local_impl=False
                require_local_execution = False
                return NodeMsg.UNASSIGNED
            
            self._internal_state = self.WAITING_FOR_ASSIGNMENT
            return NodeMsg.UNASSIGNED

        if self._internal_state == self.WAITING_FOR_ASSIGNMENT:
            try:
                ### change 0701
                request_capability_execution_response = self._request_capability_execution_service_proxy.service_is_ready()
                if request_capability_execution_response:
                    ### change 0701
                    # request_capability_execution_response=RequestCapabilityExecution.Response()
                    # future=self._request_capability_execution_service_proxy.call_async(request)
                    # while not future.done():
                    #     time.sleep(1.0)
                    request_capability_execution_response= self.new_response # future.result()
                else:
                    request_capability_execution_response = None
                      

                # request_capability_execution_response: Optional[RequestCapabilityExecution.Response] = self.__handle_async_service_call(self._request_capability_execution_service_proxy)

            except BehaviorTreeException as exc:
                self.logerr(
                    "Failed to request execute capability service, no implementation will be"
                    f" selected: {exc}"
                )
                self._cleanup()
                return NodeMsg.FAILED

            if request_capability_execution_response is None:
                return NodeMsg.UNASSIGNED

            self._implementation_name = request_capability_execution_response.implementation_name
            self.ros_node.get_logger().warn(f"if exe local: {request_capability_execution_response}")

            if request_capability_execution_response.execute_local:
                self.ros_node.get_logger().warn("Executing capability locally!")

                prepare_request = PrepareLocalImplementation.Request()
                prepare_request.implementation_name = self._implementation_name
                prepare_request.interface = self.capability_interface
                #self._prepare_local_implementation_service_proxy.call_service(prepare_request)
                future_pre=self._prepare_local_implementation_service_proxy.call_async(prepare_request)
                while not future_pre.done():
                    time.sleep(2.0)
                    rclpy.node.get_logger('capability').info(f"3333333")
                self._prepare_local_implementation_start_time = self.ros_node.get_clock().now()
                ### change 0701
                self._prepare_local_implementation_service_response=future_pre.result()

                self._internal_state = self.EXECUTE_LOCAL
            else:
                self.ros_node.get_logger().info(f"Executing capability remotely on {request_capability_execution_response.remote_mission_controller_topic}")
                self._internal_state = self.EXECUTE_REMOTE
                self._remote_mission_control_topic = f"{request_capability_execution_response.remote_mission_controller_topic}/mission_control"



            ##################### change 0701 自己加的，否则只能收到本命名空间的输入输出信息 #####################
            self.remote_capability_io_topic=f"{request_capability_execution_response.remote_mission_controller_topic}/capabilities/io"
            self._remote_output_bridge_subscriber = self.ros_node.create_subscription(
            CapabilityIOBridgeData, 
            self.remote_capability_io_topic, 
            self._publish_outputs_cb, 
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
            callback_group=ReentrantCallbackGroup()#1  ## change 0701
        )





            return NodeMsg.ASSIGNED
        return NodeMsg.FAILED

    def _tick_assigned(self) -> str:
        """
        Performs ticking operation while the node is in the assigned state.
        :return: The new node status.
        """
        if self._internal_state == self.EXECUTE_LOCAL:
            return self._tick_prepare_local_execution()

        if self._internal_state == self.EXECUTE_REMOTE:
            return self._tick_prepare_remote_execution()

        return NodeMsg.BROKEN

    def _setup_local_implementation_tree(self, tree: Tree):
        """
        Function to run in a separate thread used to set up the local implementation tree.
        :param tree: The tree to set up.
        :return: None
        """
        with self._capability_lock:
            self._setup_local_implementation_tree_status = NodeMsg.RUNNING

            # 调用加载树服务
            load_tree_response = self.tree_manager.load_tree(
                request=LoadTree.Request(tree=tree),
                response=LoadTree.Response(),
                prefix=self.name + "_"
            )

            if not load_tree_response.success:
                self.ros_node.get_logger().error(f"Error loading capability tree: {load_tree_response}")
                self._setup_local_implementation_tree_status = NodeMsg.FAILED
                return

            # 设置能力 I/O 桥接 ID
            set_capability_io_bridge_id(
                tree_manager=self.tree_manager,
                interface=self.capability_interface,
                io_bridge_id=self._io_bridge_id,
            )

            self.ros_node.get_logger().warn(f"1111111111111 io_bridge_id:{self._io_bridge_id}  11111")

            # 查找树根
            try:
                self._local_implementation_tree_root = self.tree_manager.find_root()
            except TreeTopologyError:
                self.ros_node.get_logger().error("Failed to get root of implementation tree!")
                self._setup_local_implementation_tree_status = NodeMsg.FAILED
                return

            # 初始化树根
            self._local_implementation_tree_root.shutdown()
            self._local_implementation_tree_root.setup()
            self.ros_node.get_logger().warn(f"Local imple tree set up ...")

            # 如果启用调试发布，发布子树信息
            if self.debug_manager and self.debug_manager.get_publish_subtrees():
                self.debug_manager.add_subtree_info(self.name, self.tree_manager.to_msg())

            self._setup_local_implementation_tree_status = NodeMsg.SUCCESS



    def _tick_prepare_local_execution(self) -> str:
        """
        Perform tick operation while the node is prepared for local capability execution.
        :return: The status of the node while the preparation is running.
        """
        if self._prepare_local_tree_thread is not None:
            if self._setup_local_implementation_tree_status == NodeMsg.RUNNING:
                return NodeMsg.ASSIGNED

            if self._setup_local_implementation_tree_status in [
                NodeMsg.IDLE,
                NodeMsg.FAILED,
            ]:
                return NodeMsg.FAILED

            # 发布执行状态
            self._capability_execution_status_publisher.publish(
                CapabilityExecutionStatus(
                    interface=self.capability_interface,
                    node_name=self.ros_node.get_name(),
                    status=CapabilityExecutionStatus.EXECUTING,
                )
            )
            return NodeMsg.RUNNING

        # 调用异步服务以检查本地树的准备状态
        try:
            ### change 0701
            # prepare_local_implementation_response = self.__handle_async_service_call(
            #     self._prepare_local_implementation_service_proxy
            # )
            prepare_local_implementation_response = self._prepare_local_implementation_service_response
        except BehaviorTreeException as exc:
            self.ros_node.get_logger().error(f"Failed to receive prepared tree from mission control: {exc}")
            return NodeMsg.FAILED

        # 超时处理
        if prepare_local_implementation_response is None:
            if (
                self.ros_node.get_clock().now()
                - self._prepare_local_implementation_start_time
                > Duration(seconds=10)
            ):
                self.ros_node.get_logger().error("Timeout when waiting for local implementation to be prepared!")
                return NodeMsg.FAILED
            return NodeMsg.ASSIGNED

        # 处理失败响应
        if not prepare_local_implementation_response.success:
            error_msg = (
                "Prepare local implementation request failed: "
                f"{prepare_local_implementation_response.error_message}"
            )
            self.ros_node.get_logger().error(error_msg)
            raise BehaviorTreeException(error_msg)

        # 创建线程以设置本地实现树
        self.ros_node.get_logger().error(f"Local Imple Tree Thread start!")
        self._prepare_local_tree_thread = Thread(
            target=self._setup_local_implementation_tree,
            args=(prepare_local_implementation_response.implementation_subtree,),
        )
        self._prepare_local_tree_thread.start()

        return NodeMsg.ASSIGNED

    def _tick_prepare_remote_execution(self) -> str:
        """
        Ticks the node while remote execution is prepared.
        :return: the new status after the node was prepared.
        """
        # 初始化 Action Client
        qos=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            )
        if self._execute_capability_remote_client is None:
            action_name = f"{self._remote_mission_control_topic}/execute_remote_capability"
            self._execute_capability_remote_client = ActionClient(
                self.ros_node, ExecuteRemoteCapability, action_name, callback_group=ReentrantCallbackGroup(),  ###  change 0701 
                ### 不确定不加callback group有什么结果
            )
            self.ros_node.get_logger().warn(f"ac client: {action_name}")
            self.ros_node.get_logger().debug(f"Initialized ActionClient for: {action_name}")

        # 检查连接重试次数
        if self._execute_capability_remote_client_connection_tries > 10:
            self._cleanup()
            raise BehaviorTreeException(
                f'Action server "{self._remote_mission_control_topic}/execute_remote_capability"'
                " not available after trying 10 times."
            )

        # 尝试连接 Action Server
        if not self._execute_capability_remote_client.wait_for_server(timeout_sec=10.0):
            self._execute_capability_remote_client_connection_tries += 1
            self.ros_node.get_logger().warn(f"try to connect ac server...")
            return NodeMsg.ASSIGNED

        # 发送目标到服务器
        goal_msg = ExecuteRemoteCapability.Goal(
            interface=self.capability_interface,
            implementation_name=self._implementation_name,
            node_id=self._io_bridge_id,
        )
        # 为_execute_action逻辑重构新增的变量
        # goal_1=uuid.uuid4()
        # goal_info=GoalInfo(goal_id=goal_1)
        # goal=ServerGoalHandle(None,goal_info,goal_msg)
        self.execute_capability_remote_client_furture = self._execute_capability_remote_client.send_goal_async(goal=goal_msg,feedback_callback=self._feedback_cb)
         
        self.ros_node.get_logger().info(f"Sent goal to Action Server: {goal_msg}")

        # 记录当前时间戳
        with self._capability_lock:
            self._last_ping_timestamp = self.ros_node.get_clock().now()
            #.to_msg()  ### change 0701 不确定是否加to msg：不加，因为下文中要与duration进行运算

        # 发布执行状态
        self._capability_execution_status_publisher.publish(
            CapabilityExecutionStatus(
                interface=self.capability_interface,
                node_name=self.ros_node.get_name(),
                status=CapabilityExecutionStatus.EXECUTING,
            )
        )


        return NodeMsg.RUNNING

    def _feedback_cb(self, feedback) -> None:
        self._feedback = feedback

    def __tick_executing_local(self) -> str:
        """
        Performs the ticking operation if the capability is executed locally.
        It retrieves the local implementation and performs the ticking operations.

        :raise BehaviorTreeException: If the implementation cannot be received
        or nodes cannot be instantiated.
        :return: The status of the node from NodeMsg.
        """
        # 执行能力树的 tick 操作
        
        new_state = self._local_implementation_tree_root.tick()
        self.ros_node.get_logger().warn(f"Start ticking executing local ... new state:{new_state}")

        # 如果启用了调试模式，发布子树信息
        if self.debug_manager and self.debug_manager.get_publish_subtrees():
            self.debug_manager.add_subtree_info(self.name, self.tree_manager.to_msg())

        # 根据新状态处理逻辑
        if new_state in [NodeMsg.ASSIGNED, NodeMsg.UNASSIGNED]:
            return NodeMsg.RUNNING

        if new_state in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
            # 发布能力执行结果状态
            
            if new_state == NodeMsg.SUCCEEDED:
                status = CapabilityExecutionStatus.SUCCEEDED
            else:
                status = CapabilityExecutionStatus.FAILED
            
                    
            self._capability_execution_status_publisher.publish(
                CapabilityExecutionStatus(
                    interface=self.capability_interface,
                    node_name=self.ros_node.get_name(),
                    status=status,
                )
            )
            # 停止能力树的执行并清理资源
            self._local_implementation_tree_root.shutdown()
            self._shutdown_local_implementation_tree()

        return new_state

    def __tick_executing_remote(self) -> str:
        """
        Performs the ticking operation when operating on a remote node.

        :return: The status of the node as a NodeMsg status.
        """
        # 检查远程执行是否超时
        current_time = self.ros_node.get_clock().now()
        #self.ros_node.get_logger().info(f"{current_time.to_msg()}, {self._last_ping_timestamp}")
        if hasattr(self._last_ping_timestamp,"sec"):
            last_ping_timestamp = self._last_ping_timestamp.sec
        if hasattr(self._last_ping_timestamp,"nanoseconds"):
            last_ping_timestamp = self._last_ping_timestamp.nanoseconds/1000000000
        self.ros_node.get_logger().info("time show")
        #if (current_time - self._last_ping_timestamp) > Duration(
            #seconds=self.options["execution_timeout_sec"]
        #):
        if (current_time.to_msg().sec - last_ping_timestamp) > self.options["execution_timeout_sec"]:
            self.ros_node.get_logger().error(
                f"ExecuteRemoteCapability action timed out after "
                f'{self.options["execution_timeout_sec"]} seconds'
            )
            if self.execute_capability_handle is not None:
                self.execute_capability_handle.cancel_goal_async()
                self.execute_capability_handle = None
            self._cleanup()
            return NodeMsg.FAILED

        # 执行远程能力并检查结果
        try:
            execute_remote_implementation_result: Optional[
                ExecuteRemoteCapability.Result
            ] = self._execute_action(self.execute_capability_remote_client_furture)
            
        except BehaviorTreeException as exc:
            self.ros_node.get_logger().error(f"Failed to execute action: {exc}")
            self._cleanup()
            return NodeMsg.FAILED

        # 如果结果为空，说明任务仍在运行
        if execute_remote_implementation_result is None:
            return NodeMsg.RUNNING

        self.ros_node.get_logger().warn(f"11111111117777777777 {execute_remote_implementation_result}")
        # 检查执行结果是否成功
        if not execute_remote_implementation_result.success:
            if execute_remote_implementation_result.no_executor_available:
                self.ros_node.get_logger().error("No executor available at the moment! Try again later!")
                self._cleanup()
                return NodeMsg.UNASSIGNED

            self.ros_node.get_logger().error(
                f"Failed to execute action: {execute_remote_implementation_result.error_message}"
            )
            self._cleanup()
            return NodeMsg.FAILED

        return NodeMsg.SUCCEEDED

    def _tick_running(self) -> str:
        """
        Performs ticking operations for capabilities during the NodeMsg.RUNNING status.
        :return: Either NodeMsg.RUNNING, NodeMsg.SUCCESS or NodeMsg.FAILED
        """
        new_state = NodeMsg.RUNNING
        self.ros_node.get_logger().warn(f"tick run!")
        # 等待输出数据
        if self._internal_state == self.WAITING_FOR_OUTPUT_VALUES:
            if self._last_received_outputs is not None:
                for bridge_data in self._last_received_outputs.bridge_data:
                    node_data: NodeDataMsg = bridge_data

                    try:
                        self.outputs[node_data.key] = json_decode(
                            node_data.serialized_value
                        )
                        self.ros_node.get_logger().warn(f"11111118888888 output result get!")



                        self.result_exe_pub = self.ros_node.create_publisher(
                        DistanceCalculation,
                        f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_finish_result",
                        qos_profile=QoSProfile(
                        reliability=QoSReliabilityPolicy.RELIABLE,
                        durability=QoSDurabilityPolicy.VOLATILE,
                        history=QoSHistoryPolicy.KEEP_LAST,
                        depth=10,),
                        callback_group=ReentrantCallbackGroup())
                        self.ros_node.get_logger().warn(f"result msg :owner:{self.ros_node.get_namespace()}, cap:{self.name}")
                        self.result_exe_pub.publish(DistanceCalculation(own_name=self.ros_node.get_namespace(),other_name=self.name))
                        self.ros_node.get_logger().warn(f"result status: {self._result_status}")
                        




                    except TypeError as exc:
                        self.ros_node.get_logger().warn(f"Could not set output {node_data.key}: {exc}")
                        continue
                return self._result_status

            # 检查是否超时
            current_time = self.ros_node.get_clock().now()
            if (current_time - self._result_timestamp) > Duration(
                seconds=self.wait_for_outputs_timeout_sec
            ):
                self.ros_node.get_logger().warn(
                    "Timed out while waiting for results! Check if the implementation contains a"
                    " valid OutputDataBridge!"
                )
                return self._result_status

        # 执行本地能力
        if self._internal_state == self.EXECUTE_LOCAL:
            new_state = self.__tick_executing_local()

        # 执行远程能力
        if self._internal_state == self.EXECUTE_REMOTE:
            new_state = self.__tick_executing_remote()

        # 如果执行完成，切换状态并更新结果
        if new_state in [NodeMsg.FAILED, NodeMsg.SUCCEEDED]:
            self.ros_node.get_logger().info(f"11111111888888 node get succeed or failed!!")
            self._result_status = new_state
            self._internal_state = self.WAITING_FOR_OUTPUT_VALUES
            self._result_timestamp = self.ros_node.get_clock().now()
            new_state = NodeMsg.RUNNING
            # self.result_exe_pub = self.ros_node.create_publisher(
            #     DistanceCalculation,
            #     f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_finish_result",
            #     qos_profile=QoSProfile(
            #     reliability=QoSReliabilityPolicy.RELIABLE,
            #     durability=QoSDurabilityPolicy.VOLATILE,
            #     history=QoSHistoryPolicy.KEEP_LAST,
            #     depth=10,),
            #     callback_group=ReentrantCallbackGroup())
            # self.result_exe_pub.publish(DistanceCalculation(own_name=self.ros_node.get_namespace(),other_name=self.name))

        return new_state

    def _do_tick(self):

        self.ros_node.get_logger().warn(f"self {self.name} state when ticking :{self.state}")
        self.ros_node.get_logger().warn(f"self inputs when ticking for {self._io_bridge_id} :{self.inputs}")

        if self._io_publisher is not None:
            input_data = []
            self.ros_node.get_logger().warn(f" io_publish when ticking :{self.inputs}")
            for key in self.inputs:
                input_data_msg = NodeDataMsg()
                input_data_msg.key = key
                input_data_msg.serialized_value = self.inputs.get_serialized(key=key)
                input_data_msg.serialized_type = self.inputs.get_serialized_type(key=key)
                input_data.append(input_data_msg)
            try:
                self.ros_node.get_logger().warn(f"self internal state:{self._internal_state}")
                
                if self._internal_state == self.EXECUTE_REMOTE:
                    self._remote_input_bridge_publisher = self.ros_node.create_publisher(
                    CapabilityIOBridgeData, 
                    self.remote_capability_io_topic, 
                    qos_profile=QoSProfile(
                        reliability=QoSReliabilityPolicy.RELIABLE,
                        durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                        history=QoSHistoryPolicy.KEEP_LAST,
                        depth=10,
                    ),
                    callback_group=ReentrantCallbackGroup()#1  ## change 0701
                )
                    self._remote_input_bridge_publisher.publish(
                        CapabilityIOBridgeData(
                            node_id=self._io_bridge_id,
                            type=CapabilityIOBridgeData.INPUT,
                            bridge_data=input_data,
                            timestamp=self.ros_node.get_clock().now().to_msg(),  # 使用 ROS 2 的时间戳
                        )
                    )
                else:
                    self.ros_node.get_logger().warn(f"if io publisher working: {self._io_publisher}")
                    self._io_publisher.publish(
                        CapabilityIOBridgeData(
                            node_id=self._io_bridge_id,
                            type=CapabilityIOBridgeData.INPUT,
                            bridge_data=input_data,
                            timestamp=self.ros_node.get_clock().now().to_msg(),  # 使用 ROS 2 的时间戳
                        )
                    )
            # except ROSSerializationException as exc:
            #     error_msg = f"Could not serialize inputs to publish: {exc}"
            #     self.ros_node.get_logger().error(error_msg)
            #     raise BehaviorTreeException(error_msg) from exc
            except Exception as exc:
                error_msg = f"Input data bridge publisher could not publish: {exc}"
                self.ros_node.get_logger().error(error_msg)
                raise BehaviorTreeException(error_msg) from exc

        # 模拟运行
        if self.simulate_tick:
            # self.ros_node.get_logger().debug(f"Simulating tick. {self.name} is not executing!")
            if self.succeed_always:
                return NodeMsg.SUCCEEDED
            return NodeMsg.RUNNING

        # 状态恢复
        if self.state == NodeMsg.PAUSED:
            self.state = self._old_state
            #####################################################################
            # change 0701 
            #####################################################################
            # self.ros_node.get_logger().warn(f"test1 recover from paused !!!")
            # #if self._result_status is not None:
            # self._old_state = NodeMsg.SUCCEEDED
            # #else:
            # #    self._old_state = self.state

        # 处理不同的状态
        if self.state == NodeMsg.IDLE:
            self.ros_node.get_logger().warn(f"test1 unassigned")
            self._capability_execution_status_publisher.publish(
                CapabilityExecutionStatus(
                    interface=self.capability_interface,
                    node_name=self.name,
                    status=CapabilityExecutionStatus.IDLE,
                )
            )
            return NodeMsg.UNASSIGNED

        if self.state == NodeMsg.UNASSIGNED:
            return self._tick_unassigned()

        if self.state == NodeMsg.ASSIGNED:
            return self._tick_assigned()

        if self.state == NodeMsg.RUNNING:
            return self._tick_running()

        # 未定义状态，返回失败
        return NodeMsg.FAILED

    def _do_untick(self):
        # 如果当前状态是暂停，直接返回
        if self.state == NodeMsg.PAUSED:
            return NodeMsg.PAUSED

        # 保存当前状态
        #####################################################################
        # change 0701 
        #####################################################################
        self._old_state = self.state
        self.ros_node.get_logger().warn(f"test1 untick :{self.state} !!!")
        # if self._result_status is not None:
        #     self._old_state = NodeMsg.PAUSED
        # else:
        #     self._old_state = self.state

        # IDLE、SUCCEEDED 或 FAILED 状态直接切换为 PAUSED
        # if self.state in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
        #     self.allocated_cap_name = None
        #     return NodeMsg.IDLE
        if self.state in [NodeMsg.IDLE, NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
            self.allocated_cap_name = None
            self._shutdown_local_implementation_tree()
            self._cleanup()
            return NodeMsg.PAUSED

        # 如果状态是 UNASSIGNED，重置内部状态并停止相关服务
        if self.state == NodeMsg.UNASSIGNED:
            #self._internal_state = self.IDLE
            if self._request_capability_execution_service_proxy is not None:
                self._request_capability_execution_service_proxy.service_is_ready()#destroy()# stop_call() ### change 0701

        # 如果状态是 ASSIGNED，根据具体内部状态执行对应操作
        if self.state == NodeMsg.ASSIGNED:
            if self._internal_state == self.EXECUTE_LOCAL:
                if self._prepare_local_implementation_service_proxy is not None:
                    self._prepare_local_implementation_service_proxy.service_is_ready()#destroy()# stop_call() ### change 0701

            if self._internal_state == self.EXECUTE_REMOTE:
                if self._execute_capability_remote_client is not None:
                    if self.execute_capability_handle is not None:
                        self.execute_capability_handle.cancel_goal_async()
                        self.execute_capability_handle = None

        # 尝试关闭本地实现树，如果失败，返回 FAILED
        if not self._shutdown_local_implementation_tree():
            return NodeMsg.FAILED

        # 默认返回 PAUSED
        return NodeMsg.PAUSED

    def _do_reset(self):
        #self._stop_calls_action_clients_async_service_clients()
        self.ros_node.get_logger().warn(f"999999999999999999999 reset cap:{self.name} !!! 99999999999999999999999")
        self.allocated_cap_name = None
        self._shutdown_local_implementation_tree()
        self._cleanup()

        return NodeMsg.IDLE

    def _do_shutdown(self):
        # 发布节点关闭状态
        self.ros_node.get_logger().warn(f"999999999999999999999 shutdown cap 9999999999999")
        if self._capability_execution_status_publisher is not None:
            self._capability_execution_status_publisher.publish(
                CapabilityExecutionStatus(
                    interface=self.capability_interface,
                    node_name=self.name,
                    status=CapabilityExecutionStatus.SHUTDOWN,
                )
            )

        # 关闭本地实现树
        self._shutdown_local_implementation_tree()

        # 注销 I/O 桥发布者和订阅者
        #self._unregister_io_bridge_publishers_subscribers()

        # 注销心跳订阅者
        if self._ping_subscriber is not None:
            #self._ping_subscriber.destroy()
            self._ping_subscriber = None

        # 停止异步服务客户端
        #self._stop_calls_action_clients_async_service_clients()

        # 关闭服务代理
        if self._request_capability_execution_service_proxy is not None:
            self._request_capability_execution_service_proxy.destroy() # shutdown() ### change 0701
            self._request_capability_execution_service_proxy = None

        if self._prepare_local_implementation_service_proxy is not None:
            self._prepare_local_implementation_service_proxy.destroy() # shutdown() ### change 0701
            self._prepare_local_implementation_service_proxy = None

        # 清理所有资源
        self._cleanup()

        # 设置所有资源为 None
        self._io_subscriber = None
        self._io_publisher = None
        self._ping_subscriber = None

        self._request_capability_execution_service_proxy = None
        self._prepare_local_implementation_service_proxy = None
        self._capability_execution_status_publisher = None

    def _unregister_io_bridge_publishers_subscribers(self):
        """
        Helper method that destroys all the I/O bridge publishers and subscribers if not None.
        :return: None
        """
        # 销毁 I/O 订阅者
        if self._io_subscriber is not None:
            #self._io_subscriber.destroy()
            self._io_subscriber = None

        # 销毁 I/O 发布者
        if self._io_publisher is not None:
            #self._io_publisher.destroy()
            self._io_publisher = None

        if self._remote_output_bridge_subscriber is not None:
            #self._remote_output_bridge_subscriber.destroy()
            self._remote_output_bridge_subscriber = None

    def _stop_calls_action_clients_async_service_clients(self):
        """
        Shuts down the running goals of action clients and async service clients in ROS 2.
        :return: None
        """
        # 停止异步服务调用：请求能力执行服务
        if self._request_capability_execution_service_proxy is not None:
            #self._request_capability_execution_service_proxy.destroy() # stop_call() ### change 0701
            self._request_capability_execution_service_proxy = None

        # 停止异步服务调用：准备本地实现服务
        if self._prepare_local_implementation_service_proxy is not None:
            #self._prepare_local_implementation_service_proxy.destroy() # stop_call() ### change 0701
            self._prepare_local_implementation_service_proxy = None

        # 停止动作客户端的目标
        if self._execute_capability_remote_client is not None:
            if self.execute_capability_handle is not None:
                self.execute_capability_handle.cancel_goal_async()
                self.execute_capability_handle = None

    def _shutdown_local_implementation_tree(self) -> bool:
        """
        Shutdown the local implementation tree in ROS 2.

        :return: True if the tree could be shutdown, False otherwise.
        """
        self.logwarn(f"{self.name} shutdown local impl 123...")
        if self._local_implementation_tree is not None:
            with self._capability_lock:
                self._local_implementation_tree_root.shutdown()

                response: ControlTreeExecution.Response = self.tree_manager.control_execution(
                    ControlTreeExecution.Request(
                        command=ControlTreeExecution.Request.SHUTDOWN
                    ),
                    ControlTreeExecution.Response()  ### change 0701
                )
                if not response.success:
                    self.logwarn(
                        f"Could not shutdown local capability: {response.error_message}"
                    )
                    return False

        return True
    
    def _clear_local_implementation_tree(self) -> bool:
        """
        Helper method that clears the local tree.
        :return: True if successful, false otherwise.
        """
        if self._local_implementation_tree is not None:
            with self._capability_lock:
                response: ClearTree.Response = self.tree_manager.clear(ClearTree.Request(),ClearTree.Response()) ### change 0701
                if not response.success:
                    self.ros_node.get_logger().warn(
                        f"Could not clear local capability tree: {response.error_message}"
                    )
                    return False
        return True

    def _cleanup(self):
        """
        Cleans up the internal states of the capability and resets them to the default values in ROS 2.
        :return: None
        """
        # Wait for prepare local tree thread to finish
        self.logwarn(f"{self.name} clean up 123...")
        with self._capability_lock:
            if self._prepare_local_tree_thread is not None:
                self._prepare_local_tree_thread.join()
            self._prepare_local_tree_thread: Optional[Thread] = None

            self._internal_state = self.IDLE

            self._implementation_name: Optional[str] = None
            self._result_status = None
            self._result_timestamp = None

            # Variables for remote trees
            self._remote_mission_control_topic: Optional[str] = None
            self._execute_capability_remote_client_connection_tries: int = 0

            # Variables for local trees
            self._clear_local_implementation_tree()
            self.logwarn(f"{self.name} clean local impl tree 123...")

            self._local_implementation_tree: Optional[Tree] = None
            self._local_implementation_tree_root: Optional[Node1] = None
            self._setup_local_implementation_tree_status = NodeMsg.IDLE

            del self._execute_capability_remote_client
            self._execute_capability_remote_client = None

def capability_node_class_from_capability_interface(
    capability_interface: CapabilityInterface, mission_control_topic: str
    ) -> Type[Capability]:
        """
        Creates a new class definition for the defined capability interface in ROS 2.

        :param capability_interface: The capability interface to use.
        :param mission_control_topic: The topic the local mission controller is running on.
        :return: Class definition for the capability that can be instantiated like any other node.
        """
        # 解析输入类型
        inputs_dict = {}
        for input_node_data in capability_interface.inputs:
            inputs_dict[input_node_data.key] = json_decode(input_node_data.serialized_type)#TODO(tjp):这里需要解决

        outputs_dict = {}
        for output_node_data in capability_interface.outputs:
            outputs_dict[output_node_data.key] = json_decode(
                output_node_data.serialized_type
            )

        capability_node_class = type(
            capability_interface.name,
            (Capability,),
            {
                "__capability_interface": capability_interface,
                "__local_mc_topic": mission_control_topic,
                "__module__": Capability.__module__,
            },
        )

        return define_bt_node(
            NodeConfig(
                options={
                    "require_local_execution": bool,
                    "execution_timeout_sec": float,
                    "inputs_names_for_implementation_tags": list,
                    "require_coop":bool,
                },
                inputs=inputs_dict,
                outputs=outputs_dict,
                max_children=0,
            )
        )(capability_node_class)

def capability_input_data_bridge_from_interface(
    capability_interface: CapabilityInterface,
    ) -> Type[CapabilityInputDataBridge]:
        """
        Create an in-memory version of a CapabilityInputDataBridge from a CapabilityInterface for ROS 2.

        :param capability_interface: The capability interface to create a representation for.
        :return: The class definition for a capability input bridge.
        """
        # 解析输入类型
        inputs_dict = {
            input_node_data.key: json_decode(input_node_data.serialized_type)#TODO(tjp):这里需要解决
            for input_node_data in capability_interface.inputs
        }

        # 动态创建输入桥接类
        capability_input_bridge_node_class = type(
            f"{capability_interface.name}_InputDataBridge",
            (CapabilityInputDataBridge,),
            {
                "__capability_interface": capability_interface,
                "__module__": f"{CapabilityInputDataBridge.__module__}.bridge",
            },
        )

        # 配置为行为树节点
        return define_bt_node(
            NodeConfig(
                options={},  # 无额外配置选项
                inputs={},  # 无直接节点输入
                outputs=inputs_dict,  # 将解析的输入作为输出
                max_children=0,  # 不支持子节点
            )
        )(capability_input_bridge_node_class)

def capability_output_data_bridge_from_interface(
    capability_interface: CapabilityInterface,
    ) -> Type[CapabilityOutputDataBridge]:
        """
        Creates an in-memory representation of a CapabilityOutputDataBridge from a CapabilityInterface for ROS 2.

        :param capability_interface: The capability interface to generate the class from.
        :return: The type definition of the capability output data bridge.
        """

        # 解析输出数据
        outputs_dict = {
            output_node_data.key: json_decode(output_node_data.serialized_type)
            for output_node_data in capability_interface.outputs
        }

        # 动态创建输出桥接类
        capability_output_bridge_node_class = type(
            f"{capability_interface.name}_OutputDataBridge",
            (CapabilityOutputDataBridge,),
            {
                "__capability_interface": capability_interface,
                "__module__": f"{CapabilityOutputDataBridge.__module__}.bridge",
            },
        )

        # 配置为行为树节点
        return define_bt_node(
            NodeConfig(
                options={},  # 无额外配置选项
                inputs=outputs_dict,  # 此类不需要输入
                outputs={},  # 将解析的输出作为节点输出
                max_children=0,  # 不支持子节点
            )
        )(capability_output_bridge_node_class)

def capability_node_class_from_capability_interface_callback(msg: Time, args: List):
    """
    ROS 2 service callback used to register capability interfaces with the local Node class.

    :param msg: The capability interface to register.
    :param args: The additional arguments passed to the callback.
    :return: Nothing
    """
    
    if len(args) < 2:
        rclpy.node.get_logger('capability_node_class').error(
            "Create capability node callback received less than two extra args."
        )
        return

    if not isinstance(args[0], str):
        rclpy.node.get_logger('capability_node_class').error("First argument is not a string!")
        return

    if not isinstance(args[1], Client):
        rclpy.node.get_logger('capability_node_class').error("Second argument is not a service client!")
        return

    rclpy.node.get_logger('capability_node_class').info(f"Received new capability information at {msg}")

    '''原版
    service_proxy: Client = args[1]
    response: GetCapabilityInterfaces.Response = service_proxy.call(
        GetCapabilityInterfaces.Request()
    )
    if not response.success:
        rclpy.node.get_logger('capability_node_class').warn(f"Failed to get capability interfaces: {response.error_message}!")
    if response.success:
        rclpy.node.get_logger('capability_node_class').warn(f"Succeed to get capability interfaces!")
    for interface in response.interfaces:
        capability_node_class_from_capability_interface(interface, args[0])
        capability_input_data_bridge_from_interface(interface)
        capability_output_data_bridge_from_interface(interface)'''
    service_proxy: Client = args[1]
    #node:Node=rclpy.node.args[0]
    rclpy.node.get_logger('capability_node_class').info(f"22222 ,{args[0]}, 2222222{args[1].srv_type},{args[1].srv_name},{args[1].qos_profile}")


    '''request = GetCapabilityInterfaces.Request()
    future = service_proxy.call_async(request)

    # Wait for the response (can also use a callback approach)
    rclpy.spin_until_future_complete(/, future)

    if future.result() is None or not future.done():
        error_message = "No response"
        rclpy.node.get_logger('capability_node_class').error(
                f"Could not get cap interface: {error_message}"
            )

    response: GetCapabilityInterfaces.Response = future.result().result


    
    try:
        #service_proxy.call(GetCapabilityInterfaces.Request())
        response= GetCapabilityInterfaces.Response()# = service_proxy.call_async(
        #GetCapabilityInterfaces.Request()
        #)
        #if_success=service_proxy.wait_for_service(3.0)
        #rclpy.node.get_logger('capability_node_class').info(f"222222222222{if_success}:{service_proxy.srv_name}")
        future=service_proxy.call_async(GetCapabilityInterfaces.Request())
        while future is None:
            future=service_proxy.call_async(GetCapabilityInterfaces.Request())
        #rclpy.node.get_logger('capability_node_class').info(f"222222222222")
        #rclpy.get_global_executor().spin_until_future_complete(future,10.0)#spin_until_future_complete(future)
        else:
            response: GetCapabilityInterfaces.Response = future.result()
        #response: GetCapabilityInterfaces.Response = service_proxy.call_async(
        #GetCapabilityInterfaces.Request()
        #).result()
    except Exception as exc:
        #if_success = service_proxy.wait_for_service(3.0)
        rclpy.node.get_logger('capability_node_class').info(f"33333333333333333333")
    '''
    #spin_thread = Thread(target=args[2], args=(service_proxy,))
    #spin_thread.start()
    #response = service_proxy.call_async(GetCapabilityInterfaces.Request()).result()
    #spin_thread:Optional[Thread] = None
    request=GetCapabilityInterfaces.Request()
    rclpy.node.get_logger('capability_node_class').warn(f"send request:{request},time:{msg}")
    response = GetCapabilityInterfaces.Response()
    
    response_call_time=rclpy.node.Clock().now().to_msg()
    rclpy.node.get_logger('capability_node_class').info(f"response:{response},call time:{response_call_time}")
    future=service_proxy.call_async(request)
    '''
    if future.result() is None:
        rclpy.node.get_logger('capability_node_class').info(f"wait for{if_success},try again:new request:{request}")
        time.sleep(2.0)
        future=service_proxy.call_async(request)
        response_call_time=rclpy.node.Clock().now().to_msg()'''
    while not future.done():
        time.sleep(2.0)
        #rclpy.node.get_logger('capability_node_class').info(f"222222222{future},{future.result()}")
    #    future=service_proxy.call_async(request)
        response_call_time=rclpy.node.Clock().now().to_msg()
    #rclpy.spin_until_future_complete(args[2], future)
    response=future.result()
    #response: GetCapabilityInterfaces.Response = service_proxy.call(GetCapabilityInterfaces.Request())
    rclpy.node.get_logger('capability_node_class').info(f"222222222222{response},{future.done()},{response_call_time}")
    if response is None or not response.success:
        rclpy.node.get_logger('capability_node_class').warn(f"Failed to get capability interfaces")#: {response.error_message}!")
    if response is not None and response.success:
        rclpy.node.get_logger('capability_node_class').warn(f"Succeed to get capability interfaces!")
    if response is not None:
        for interface in response.interfaces:
            rclpy.node.get_logger('capability_node_class').warn(f"{interface}")
            capability_node_class_from_capability_interface(interface, args[0])
            capability_input_data_bridge_from_interface(interface)
            capability_output_data_bridge_from_interface(interface)
            rclpy.node.get_logger('capability_node_class').warn(f"11111111111111 load bridge")

def set_capability_io_bridge_id(
    tree_manager: TreeManager, interface: CapabilityInterface, io_bridge_id: str
    ) -> None:
    """
    Set the input and output bridge topic id to a specific value.

    :param tree_manager: The tree manager where the tree is stored
    :param interface: The interface for which the input and output bridge values should be set.
    :param io_bridge_id: The unique id that should be used for communication.
    :return: None
    """
    # Log entry to indicate function has started
    for node in tree_manager.nodes.values():
        if (
            hasattr(node, "__capability_interface")
        ):
            interface = getattr(node, "__capability_interface")
        node.logwarn(f"11111 set io, cap:{interface}")
        if (
            hasattr(node, "__capability_interface")
            and getattr(node, "__capability_interface") == interface
        ):
            node.logwarn(f"{io_bridge_id}")
            if isinstance(node, CapabilityInputDataBridge):
                node.capability_bridge_id = io_bridge_id
                continue
            if isinstance(node, CapabilityOutputDataBridge):
                node.capability_bridge_id = io_bridge_id


