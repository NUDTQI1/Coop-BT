#!/usr/bin/env python
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


import rclpy

from rclpy.action import ActionServer
from rclpy.node import Node, ReentrantCallbackGroup
from rclpy.publisher import Publisher
from rclpy.service import Service

from ros_bt_py_interfaces.msg import RemoteSlotState
from ros_bt_py_interfaces.action import RunTree
from ros_bt_py_interfaces.srv import ControlTreeExecution, EvaluateUtility

from ros_bt_py.remote_tree_slot import RemoteTreeSlot  ##加了src
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)


### test

class RemoteTreeSlotNode(Node):
    """Provide :class:`ros_bt_py.remote_tree_slot.RemoteTreeSlot` functionality.

    This ROS node offers a :class:`ros_bt_py_msgs.msg.RunTreeAction`
    server, and two service servers for
    :class:`ros_bt_py_msgs.srv.ControlTreeExecution` and
    :class:`ros_bt_py_msgs.srv.EvaluateUtility`.

    They are in the node's private namespace, making it possible to
    run multiple `RemoteTreeSlot` nodes in the same namespace.

    Most useful in conjunction with the
    :class:`ros_bt_py.nodes.remote_tree.RemoteTree` Behavior Tree
    node, which will send
    :class:`ros_bt_py_msgs.srv.ControlTreeExecution` requests to the
    slot.

    """
    def __init__(self):
        super().__init__('remote_tree_slot')
#def main():
    
    #rclpy.__init__.creat_node("remote_tree_slot")

        self.slot_state_pub = self.create_publisher(
        RemoteSlotState, 
        "~/slot_state", 
        qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ),
        callback_group=ReentrantCallbackGroup(),
        #latch=True, queue_size=1
        )

        self.get_logger().warn("Remote Tree Slot start part 1.")
        self.remote_slot = RemoteTreeSlot(ros_node=self,publish_slot_state=self.slot_state_pub.publish)  #### change 0701 不确定有没有括号

    # Connect the action server's callbacks to the RemoteTreeSlot
        self.action_server = ActionServer(
        self,
        RunTree,
        "~/run_tree",
        execute_callback=self.remote_slot.run_tree_handler,  ####### change 0701 ！！！！！不确定，没有原本对应的execute callback函数
        goal_callback=self.remote_slot.run_tree_handler,
        cancel_callback=self.remote_slot.cancel_run_tree_handler,
        #auto_start=False,
        )

    # Set up the service servers using the RemoteTreeSlot's handlers
        self.control_tree_service = self.create_service(
        ControlTreeExecution,
        "~/control_slot_execution",
        callback=self.remote_slot.control_tree_execution_handler,
        )
        self.evaluate_utility_service = self.create_service(
        EvaluateUtility,
        "~/evaluate_utility",
        callback=self.remote_slot.evaluate_utility_handler,
        )

    # And go!
        self.action_server.execute(taken_data=None)#.start()  #放在main里面还是外面？？

# change 0701
def main(argv=None):
    rclpy.init(args=argv)
    RemoteTreeSlot_node = RemoteTreeSlotNode()
    RemoteTreeSlot_node.get_logger().info("Remote Tree Slot Node Strart.")
    rclpy.spin(RemoteTreeSlot_node)


if __name__ == "__main__":
    #main()
    import sys
    main(sys.argv)

#add

#    rclpy.spin()


#if __name__ == "__main__":
#    main()
