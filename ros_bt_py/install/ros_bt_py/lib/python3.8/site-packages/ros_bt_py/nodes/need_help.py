"""
Vision behaviors for TurtleBot3
"""


import rclpy
from rclpy.duration import Duration
from sensor_msgs.msg import Image
from typing import Optional, Dict, List, Type

from rclpy.qos import  QoSProfile, ReliabilityPolicy#QoSDurabilityPolicy,QoSReliabilityPolicy




import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

import ros_bt_py#py_trees
import ros_bt_py_interfaces#py_trees_ros
from ros_bt_py_interfaces.msg import Node as NodeMsg, Message, Package
from ros_bt_py.node import Node, Leaf, FlowControl, define_bt_node
from ros_bt_py.node_config import NodeConfig, OptionRef
from ros_bt_py.node_data import NodeData, NodeDataMap
from rclpy.node import Node as ROSNode
from rclpy.node import ReentrantCallbackGroup
from abc import ABC, abstractmethod

from ros_bt_py.ros_helpers import LoggerLevel
from ros_bt_py.helpers import json_encode, json_decode
from rclpy.subscription import Subscription#
from rclpy.publisher import Publisher#

from ros_bt_py.ros_nodes.action import ActionForSetType, Action

from ros_bt_py_interfaces.msg import (
    CapabilityInterface,
    NodeData as NodeDataMsg,
    CapabilityImplementation
)
from ros_bt_py.capability_repository import CapabilityRepository
from ros_bt_py.helpers import HashableCapabilityInterface
from typing import Dict, Set


from ros_bt_py_interfaces.srv import (
    ControlTreeExecution,
    GetCapabilityInterfaces,
    RequestCapabilityExecution,
    GetLocalBid,
)



@define_bt_node(
    NodeConfig(
        options={"namespace":str,"const1":int,"const2":int},   ############ add namespace
        inputs={"if_need_help": bool},
        outputs={"if_get_help":bool},
        max_children=0,
    ))
class LookForHelp(Leaf):
    """
    Gets images from the robot and looks for object using
    simple HSV color space thresholding and blob detection.
    """

    
    
    def _do_setup(self):

        
        self.capability_execution_request:Optional[Publisher]=None
        self._ros_node.declare_parameter("capability_topic", "~/capabilities")

        
        self.img_sub = self._ros_node.create_subscription(
            Image, "/"+self.namespace+"/camera/image_raw", self.img_callback, self.qos_profile#10
        )

        self.caplist:HashableCapabilityInterface =['add_cap']
        CapabilityRepository.capabilities = Set[self.caplist]
        CapabilityRepository.local_capabilities = Dict[
            HashableCapabilityInterface, Dict[str, CapabilityImplementation]
        ] = {}

        ## 在树中设置信息收发节点，通过msg为cap repo加载能力，并通过global函数获取全局interface验证加载是否成功


        ##  可能1：global_capability_interfaces_callback（回收环境中所有关于能力的全局声明，并转化为hash caplist
        ##  可能2：load capability（按照指定路径加载能力文件及其一切实现，引入yaml文件，但是是以msg的形式？）
        ##  
        ##  可能3：调用put_capability_implementation函数：Put a new implementation to the repository from the local node.
           # 在node中已经建立了相应的service，但是没有给函数具体的request
           #:param request: The service request.
           #:return: Success if the capability was successfully added to the repository.
           #Failure if the capability interface is unknown or the uuids are not valid.

        

    def _do_tick(self):

        # send broadcast msg
        self.capability_topic: str = self._ros_node.get_parameter("capability_topic").value
        self.capability_execution_request=self._ros_node.create_publisher(CapabilityInterface, self.capability_topic, 10)
        self.CapabilityInterface=CapabilityInterface()
        self.CapabilityInterface.name='add_cap'
        self.CapabilityInterface.description='test cap'
        self.CapabilityInterface.options=NodeDataMsg()
        self.CapabilityInterface.inputs=[]
        input1=NodeDataMsg(
                    key='constan1',
                    serialized_value='1',
                    serialized_type='int' # '{"py/type": "builtins.str"}',
                )
        self.CapabilityInterface.inputs.append(input1)
        input2=NodeDataMsg(
                    key='constan2',
                    serialized_value='2',
                    serialized_type='int' # '{"py/type": "builtins.str"}',
                )
        self.CapabilityInterface.inputs.append(input2)
        self.CapabilityInterface.outputs=NodeDataMsg()
        request=RequestCapabilityExecution.Request(
                        capability=self.CapabilityInterface,
                        node_id=self.options["namespace"],
                        require_local_execution=True,
                        implementation_tags_dict={"constant1":1,"constant2":2}#:Dict[str,value]={"constant1":1,"constant2":2}
                        )

        if self.capability_execution_request is not None:
            if self.inputs["if_need_help"]==True:
                try:
                    self.capability_execution_request.publish(request)
                except Exception as exc:
                    error_msg = f"Could not publish CapExec: {exc}"
                    self.get_logger().error(error_msg)
                    raise RuntimeError(error_msg) from exc
            return NodeMsg.SUCCEEDED
        return NodeMsg.FAILED        

            
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    
