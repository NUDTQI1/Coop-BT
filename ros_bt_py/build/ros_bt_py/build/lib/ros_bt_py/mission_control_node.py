#!/usr/bin/env python3
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
ROS Node running the MissionControl class.
"""

# pylint: disable=import-error, no-name-in-module

import rclpy
from rclpy.node import Node, ReentrantCallbackGroup
import time
from builtin_interfaces.msg import Time
from ros_bt_py_interfaces.srv import GetCapabilityInterfaces
from ros_bt_py.capability import capability_node_class_from_capability_interface_callback
from ros_bt_py.mission_control import MissionControl
from launch.substitutions import LaunchConfiguration  #add
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)#add
from ros_bt_py.ros_helpers import AsyncServiceProxy
from rclpy.executors import MultiThreadedExecutor


class MissionControlNode(Node):
    def __init__(self):
        super().__init__('mission_control')

        # 获取参数
        #self.local_repository_prefix = LaunchConfiguration('local_repository_prefix')#self.get_parameter('local_repository_prefix').get_parameter_value().string_value
        #if not self.local_repository_prefix:
        self.local_repository_prefix = '/capability_repository'  #_node'  #self.get_parameters("~local_repository_prefix")#
        
        
        local_mc_prefix = f"{self.get_namespace()}/mission_control"
        
        # 服务代理
        get_capability_interfaces_topic = self.resolve_topic_name(f"{self.get_namespace()}/capability_repository/capabilities/interfaces/get")
        self.get_logger().info(f"get_capability_interfaces_topic:{get_capability_interfaces_topic}")
        self.get_capability_interface_service = self.create_client(
            GetCapabilityInterfaces, 
            get_capability_interfaces_topic,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
            ),
            callback_group=ReentrantCallbackGroup())#qos_profile #=1000  # 队列大小
        # 订阅能力接口公告
        interfaces_announcement_topic = self.resolve_topic_name(f"{self.get_namespace()}/capability_repository/capabilities/interfaces")
        self.capability_interface_subscription = self.create_subscription(
            Time, 
            interfaces_announcement_topic, 
            lambda msg: capability_node_class_from_capability_interface_callback(msg, [local_mc_prefix, self.get_capability_interface_service]), 10)  # 队列大小
        #qos_profile=QoSProfile(
        #        reliability=QoSReliabilityPolicy.RELIABLE,
        #        durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        #        history=QoSHistoryPolicy.KEEP_LAST,
        #        depth=10,
        #    )
        
        # 初始化MissionControl

    #def shutdown(self):
    #    # 任务控制器的关闭操作
    #    self.destroy_client(self.get_capability_interface_service)
    #    self.destroy_subscription(self.capability_interface_subscription)
        
''' 0604版, 原版在”文档“文件夹 '''
def main(args=None):

    rclpy.init(args=args)
    try:
        MISSION_CONTROL = MissionControlNode()
        executor = MultiThreadedExecutor()
        executor.add_node(MISSION_CONTROL)
        try:
            executor.spin()
            while not executor.shutdown():  
                time.sleep(0.5)
        finally:
            executor.shutdown()

            #MISSION_CONTROL.shutdown()
            MISSION_CONTROL.destroy_node()
    finally:
        rclpy.shutdown()

if __name__ == "__main__":
    import sys

    main(sys.argv)
    '''
    rclpy.init(args=args)
    MissionControl_node = MissionControlNode()
    rclpy.spin(MissionControl_node)

    while not MissionControl_node.shutdown():  
        time.sleep(0.5)

    MissionControl_node.shutdown()
    rclpy.shutdown()'''