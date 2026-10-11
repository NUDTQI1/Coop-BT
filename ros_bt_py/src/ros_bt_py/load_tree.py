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


import argparse
from os import path

import rclpy
import sys

import rclpy.logging
from ros_bt_py_interfaces.srv import LoadTree
from rclpy.service import Service
from rclpy.node import Node
from ament_index_python.packages import get_package_share_path

#add
class LoadTreeNode(Node):
    def __init__(self, namespace, timeout,pkg,tree_file):
        super().__init__('load_bt')#, anonymous=True)
        service_name = namespace.strip().rstrip("/") + "/load_tree"

        load_tree_proxy = self.create_client(LoadTree, service_name)
        load_tree_proxy.wait_for_service(timeout_sec=timeout)

        self.req = LoadTree.Request()
        if pkg:
            self.req.tree.path = "package://" + pkg.strip() + "/" + tree_file.strip()
        else:
            self.req.tree.path = "file://" + path.abspath(tree_file.strip())
        res = load_tree_proxy(self.req)

        if res.success:
            self.get_logger().info("Successfully loaded tree from path " + self.req.tree.path)
        else:
            self.get_logger().error(res.error_message)

#在Node节点初始化的部分开启client并等待
#add


def main(args=None):
    if args is None:
        args=sys.argv
    
    parser = argparse.ArgumentParser(description="Load a BT from a yaml file")

    parser.add_argument("pkg", nargs="?", help="package name")
    parser.add_argument(
        "tree_file",
        help=(
            "path of the YAMl file - if pkg is given, relative to package root, "
            "a regular path otherwise"
        ),
    )
    parser.add_argument(
        "--namespace",
        default="/tree_node",
        help='namespace of the tree services. defaults to "/tree_node"',
    )
    parser.add_argument("--timeout", default=2.0, type=float)

    # Filter out ROS arguments and pass everything but the filename to
    # argparse
    args = parser.parse_args(sys.argv()[1:])
    rclpy.init(args=args)
    node = LoadTreeNode(args._get_args.namespace, args.timeout, args.pkg, args.tree_file)
    ###导入参数的时候，参数都是以argument的形式存在parser里的，提不出来（后面几个args.的形式是直接给的）

    rclpy.spin(node)  
    rclpy.shutdown()
    return 0 if node.res.success else 1

    '''
    service_name = args.namespace.strip().rstrip("/") + "/load_tree"
    rclpy.init_node("load_bt", anonymous=True)

    load_tree_proxy = node.create_client(LoadTree, service_name)
    load_tree_proxy.wait_for_service(args.timeout)

    req = LoadTree.Request()
    if args.pkg:
        req.tree.path = "package://" + args.pkg.strip() + "/" + args.tree_file.strip()
    else:
        req.tree.path = "file://" + path.abspath(args.tree_file.strip())
    res = LoadTreeNode.load_tree_proxy(req)

    if res.success:
        node.get_logger().info("Successfully loaded tree from path " + req.tree.path)
        return 0
    else:
        node.get_logger().error(res.error_message)
        return 1
    '''


if __name__ == "__main__":
    main()
