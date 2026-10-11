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

"""Module containing the node definition for the CapabilityRepository."""
# pylint: disable=import-error, no-name-in-module


import rclpy
from rclpy.node import Node, ReentrantCallbackGroup

from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)

from builtin_interfaces.msg import Time
from ros_bt_py_interfaces.msg import CapabilityInterface
from ros_bt_py_interfaces.srv import (
    LoadCapabilities,
    DeleteCapabilityImplementation,
    PutCapabilityImplementation,
    GetCapabilityImplementations,
    GetCapabilityInterfaces,
    PutCapabilityInterfaces,
    SaveCapabilities,
)

from ros_bt_py.capability_repository import CapabilityRepository
from rclpy.executors import MultiThreadedExecutor

class CapabilityRepositoryNode(Node):
    """
    The ROS2 node managing the CapabilityRepository class.
    """

    def __init__(self, node_name):
        super().__init__(node_name)

        # Topic prefix used for the global capability interface exchange
        global_capability_topic_prefix = self.declare_parameter("global_capability_topic_prefix", "/gc").value

        # Local publishers 循环更新本地能力接口    ###### change 0604
        self.__local_capability_interface_update_publisher = self.create_publisher(
            Time, 
            "~/capabilities/interfaces",
            #1000) 
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1000))                                                                   
        self.__local_capability_implementation_update_publisher = self.create_publisher(
            Time, 
            "~/capabilities/implementations", 
            #1000)
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1000))

        # Global publishers 循环发布全局能力接口需求request和内容Cap interface
        self.__global_capability_interfaces_publisher = self.create_publisher(
            CapabilityInterface, 
            f"{global_capability_topic_prefix}/interfaces", 
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1000)
            )#1000)  # add 0604（不确定，可删除）
        self.__global_capability_interfaces_requests_publisher = self.create_publisher(
            Time, 
            f"{global_capability_topic_prefix}/interfaces/request", 
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=1,
            ))

        # Capability repository initialization
        self.capability_repository = CapabilityRepository(
            self,
            publisher_interface_updates_locally=self.__local_capability_interface_update_publisher,
            publisher_implementation_updates_locally=(self.__local_capability_implementation_update_publisher),
            publisher_interface_globally=self.__global_capability_interfaces_publisher,
            publisher_interface_request_globally=(self.__global_capability_interfaces_requests_publisher)
        )
        
        # Local services
        self.__local_capabilities_load_service = self.create_service(
            LoadCapabilities,
            "~/capabilities/load",
            self.capability_repository.load_capabilities,
            #callback_group=ReentrantCallbackGroup(),
        )
        self.__local_capabilities_save_service = self.create_service(
            SaveCapabilities,
            "~/capabilities/save",
            self.capability_repository.save_capabilities,
        )
        self.__local_capability_interfaces_put_service = self.create_service(
            PutCapabilityInterfaces,
            "~/capabilities/interfaces/put",
            self.capability_repository.put_capability_interfaces,
        )
        self.__local_capability_interfaces_get_service = self.create_service(
            GetCapabilityInterfaces,
            "~/capabilities/interfaces/get",
            self.capability_repository.get_capability_interfaces,
        )
        self.__local_capability_implementations_get_service = self.create_service(
            GetCapabilityImplementations,
            "~/capabilities/implementations/get",
            self.capability_repository.get_capability_implementations,
        )
        self.__local_capability_implementations_put_service = self.create_service(
            PutCapabilityImplementation,
            "~/capabilities/implementations/put",
            self.capability_repository.put_capability_implementation,
        )
        self.__local_capability_implementations_delete_service = self.create_service(
            DeleteCapabilityImplementation,
            "~/capabilities/implementations/delete",
            self.capability_repository.delete_capability_implementation,
        )

        # Global subscribers
        self.__global_capability_interfaces_subscriber = self.create_subscription(
            CapabilityInterface,
            f"{global_capability_topic_prefix}/interfaces",
            self.capability_repository.global_capability_interfaces_callback,
            10,
        )
        self.__global_capability_interfaces_requests_subscriber = self.create_subscription(
            Time,
            f"{global_capability_topic_prefix}/interfaces/request",
            self.capability_repository.global_capability_interfaces_request_callback,
            10,
        )

    def shutdown(self) -> None:
        """
        Shuts down the node and disables all services and publishers.

        :return: None
        """
        self.get_logger().info("Shutting down services and publishers!")
        self.destroy_publisher(self.__global_capability_interfaces_publisher)
        self.destroy_publisher(self.__global_capability_interfaces_requests_publisher)
        self.destroy_subscription(self.__global_capability_interfaces_subscriber)
        self.destroy_subscription(self.__global_capability_interfaces_requests_subscriber)

        self.destroy_service(self.__local_capabilities_save_service)
        self.destroy_service(self.__local_capabilities_load_service)
        self.destroy_service(self.__local_capability_interfaces_get_service)
        self.destroy_service(self.__local_capability_interfaces_put_service)
        self.destroy_service(self.__local_capability_implementations_put_service)
        self.destroy_service(self.__local_capability_implementations_get_service)
        self.destroy_service(self.__local_capability_implementations_delete_service)

        self.capability_repository.shutdown()


def main(args=None):
    rclpy.init(args=args)
    node = CapabilityRepositoryNode("capability_repository")#初始化时，定义节点名
    node.get_logger().info("CapabilityRepositoryNode started.")

    ####### add 0604
    executor=MultiThreadedExecutor()
    executor.add_node(node)

    try:
        executor.spin()###### change 0604 # rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()###### change 0604 # node.shutdown()
        node.destroy_node()
    rclpy.shutdown()


if __name__ == "__main__":
    main()
