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

"""Module containing a ros node for the AssignmentManager class."""
from typing import Type

import rclpy

import rclpy.logging
from rclpy.node import Node

from ros_bt_py.assignment_manager.assignment_manager import (  ##加了src
    AssignmentManager,
    SimpleAssignmentManager,
)
from ros_bt_py.assignment_manager.parallel_auction_manager import ParallelAuctionManager  #加了src
#from ros_bt_py.parameters import tree_node_parameters   ##assignment_manager_node_parameters   ##add
#本来想通过yaml文件引入变量，但这样就必须要把变量都放在tree node parameters文件里，在setup中生成py文件时的模型名和节点名不好确定，就没有继续改tree node param
from launch.substitutions import LaunchConfiguration  #add
from rclpy.executors import MultiThreadedExecutor


class AssignmentManagerNode(Node):
    """
    Node to run a AssignmentManager class implementation.

    This sets up the rospy services and maps them to the
    respective methods in a AssignmentManager.
    """

    def __init__(
        self,
        manager_node_name: str,##add
        #local_topic_prefix: str,
        global_assignment_msg_topic_prefix: str,
        assignment_manager: Type[AssignmentManager],
    ):
        """
        Create a new AssignmentManagerNode.

        This method will directly setup rospy services.

        :param local_topic_prefix: The ros topic prefix used to communicate with
        local services like the mission control on the same robot.
        :param global_assignment_msg_topic_prefix: The ros topic used for communicating with
        other AssignmentManagers for example in an auction.
        :param assignment_manager: The assignment manager class implementation to use.
        """

        super().__init__('assignment_manager')  ##是否必要，对应原main中的init_node？

        local_topic_prefix: str = self.get_namespace()
        #global_assignment_msg_topic_prefix: str = self.get_parameter("assignment_topic_prefix")
        #if global_assignment_msg_topic_prefix is None:
        #    global_assignment_msg_topic_prefix='/assignments'

        self.assignment_manager = assignment_manager(
            #self,
            #manager_node_name,##add node_name=
            local_topic_prefix,#=local_topic_prefix,
            global_assignment_msg_topic_prefix,#=global_assignment_msg_topic_prefix,
        )


#add
def main(argv=None):
    rclpy.init(args=argv)
    
    '''
    assignment_manager_node = AssignmentManagerNode( ###AssignmentManager_node
        manager_node_name="AssignmentManager",  ##add
        local_topic_prefix='tb1',
        global_assignment_msg_topic_prefix='tb1/',
        assignment_manager=SimpleAssignmentManager)#AssignmentManager)
    '''
    
    ##add
    #param_listener = assignment_manager_node_parameters.ParamListener(assignment_manager_node)
    #params = param_listener.get_params()
    ##add
    
    try:
        '''
        #LOCAL_TOPIC_PREFIX: str = assignment_manager_node.get_namespace()  ##rospy.get_namespace()
        ASSIGNMENT_TOPIC_PREFIX: str = LaunchConfiguration('assignment_topic_prefix')#params.assignment_topic_prefix
        #ASSIGNMENT_TOPIC_PREFIX: str = AssignmentManager_node.get_parameter(#rospy.get_param(
        #    "assignment_topic_prefix").get_parameter_value().string_value
        if not ASSIGNMENT_TOPIC_PREFIX:
        '''
        #ASSIGNMENT_TOPIC_PREFIX :str = Node.get_parameter("assignment_topic_prefix")
        #if ASSIGNMENT_TOPIC_PREFIX is None:
        ASSIGNMENT_TOPIC_PREFIX = "/assignments"

    except Exception as exc:
        rclpy.logging.get_logger().error(f"Failed to retrieve parameters from parameter server: {exc}")  #assignment_manager_node##AssignmentManager_node
        raise exc
    ASSIGNMENT_MANAGER_NODE = AssignmentManagerNode(
        manager_node_name="AssignmentManager",  ##add
        #local_topic_prefix=LaunchConfiguration('self_namespace'), #Node.get_namespace(),#LOCAL_TOPIC_PREFIX,
        global_assignment_msg_topic_prefix=ASSIGNMENT_TOPIC_PREFIX,
        assignment_manager=ParallelAuctionManager,
    )
    ###### add 0604
    executor = MultiThreadedExecutor()
    executor.add_node(ASSIGNMENT_MANAGER_NODE)
    try:
        executor.spin()
    finally:
        executor.shutdown()
    
    ###### delete 0604 # rclpy.spin(ASSIGNMENT_MANAGER_NODE)#assignment_manager_node)  ###AssignmentManager_node


if __name__ == "__main__":
    import sys

    main(sys.argv)

'''
if __name__ == "__main__":
    rospy.init_node("assignment_manager")
    try:
        LOCAL_TOPIC_PREFIX: str = rospy.get_namespace()
        ASSIGNMENT_TOPIC_PREFIX: str = rospy.get_param(
            "assignment_topic_prefix", default="/assignments"
        )

    except rospy.ROSException as exc:
        rospy.logerr(f"Failed to retrieve parameters from parameter server: {exc}")
        raise exc
    ASSIGNMENT_MANAGER_NODE = AssignmentManagerNode(
        local_topic_prefix=LOCAL_TOPIC_PREFIX,
        global_assignment_msg_topic_prefix=ASSIGNMENT_TOPIC_PREFIX,
        assignment_manager=ParallelAuctionManager,
    )
    rospy.spin()
'''