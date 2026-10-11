"""
Navigation behaviors for TurtleBot3.
"""
import transforms3d
import yaml
import random
import rclpy

from action_msgs.msg import GoalStatus
from rclpy.action import ActionClient
from nav2_msgs.action import NavigateToPose

from ament_index_python.packages import get_package_share_directory


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


from ros_bt_py.ros_nodes.action import ActionForSetType, Action
from rclpy.client import Client
from rclpy.task import Future
from ros_bt_py_interfaces.srv import (
    GetCapabilityInterfaces
)
from ros_bt_py_interfaces.msg import (
    DistanceCalculation,
    CapabilityRequest
)
from std_msgs.msg import Bool
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)
import time
from gazebo_msgs.srv import DeleteEntity
import std_msgs.msg
from geometry_msgs.msg import Point



@define_bt_node(
    NodeConfig(
        options={"file_path": str,"list_type":type,"Location_type":type},
        inputs={},
        outputs={"loc_list":OptionRef("list_type"),#list,#NodeData(data_type=list,initial_value=[]),#
                 "Locations":OptionRef("Location_type")},
        max_children=0,
    ))
class SetLocationList(Leaf):##Leaf？
    """Gets a location name from the queue"""

    def _do_setup(self):#setup(self):
        self.location_file = self.options["file_path"]
        with open(self.options["file_path"]) as f:  #, "r"
            self.locations = yaml.load(f, Loader=yaml.FullLoader)
        
        self.loc_list = list(self.locations.keys())
        #random.shuffle(self.loc_list)
        
        
    def _do_tick(self):##tick(self):      #update(self):    
        
        """Checks for the status of the navigation action"""

        if self.simulate_tick:
            if self.state == NodeMsg.SUCCEEDED:
                self.logdebug("Simulating tick. Action is not executing!")
                return NodeMsg.SUCCEEDED
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        self.outputs["Locations"] = self.locations
        self.outputs["loc_list"] = self.loc_list#self.options["list_value"]

        # get_interface_topic="/turtle0/capability_repository/capabilities/interfaces/get"
        # self.logwarn(f"{get_interface_topic}")
        # service_proxy: OptionRef[Client] = self.ros_node.create_client(GetCapabilityInterfaces, get_interface_topic,callback_group=ReentrantCallbackGroup())
        # request=GetCapabilityInterfaces.Request()
        # self.logwarn(f"send request:{request}")
        # if_success = service_proxy.wait_for_service(3.0)
        # future=service_proxy.call_async(request)
        # if future.result() is None or not future.done():
        #     self.logwarn(f"wait for{if_success},try again:new request:{request}")
        #     future=service_proxy.call_async(request)
        # #rclpy.spin_until_future_complete(args[2], future)
        # response:GetCapabilityInterfaces.Response=future.result()
        # self.logwarn(f"get response:{response}")



        if self.outputs["loc_list"] is not None: #target_color is not None:
            # self.logwarn(self.outputs.callbacks)#
            self.logwarn(self.outputs["loc_list"])
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE



@define_bt_node(NodeConfig(
    options={"msgs":type},
    inputs={"get_msg":OptionRef("msgs")},  
    outputs={"target_color": str,"next_target":str},
    max_children=0,
    ))
class GetColor(Leaf):##Leaf？
    """Gets a location name from the queue"""

   
    def _do_setup(self):
        
        #self.options["color"]
        return NodeMsg.IDLE
        
        #self.input["get_msg"]=NodeData.set_updated("get_msg")
        
    def _do_tick(self):##tick(self):     #update(self):
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        if self.simulate_tick:
            if self.state == NodeMsg.SUCCEEDED:
                self.logdebug("Simulating tick. Action is not executing!")
                return NodeMsg.SUCCEEDED
        
        #self.inputs.get_callback("get_msg")
        #self.inputs.handle_subscriptions()
        
        self.logwarn(self.inputs.is_updated("get_msg"))
        self.logwarn(self.inputs["get_msg"])
        self.outputs["target_color"] = 'blue'
        self.outputs["next_target"] = 'red'
        if self.inputs["get_msg"]==[]:
            return NodeMsg.RUNNING
        else:
            if self.outputs["target_color"] is not None: #target_color is not None:
                self.logwarn(self.inputs["get_msg"])
                if len(self.inputs["get_msg"])<5:
                    self.outputs["target_color"] = self.inputs["get_msg"]
                return NodeMsg.SUCCEEDED
            else:
                return NodeMsg.FAILED
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE



@define_bt_node(
    NodeConfig(
        options={"list_msgs":type,"location_msgs":type},#"pos_list":dict
        inputs={"get_list":OptionRef("list_msgs"),
                "get_locations":OptionRef("location_msgs")},
        outputs={"target_pose":list},
        max_children=0,
        ))
class GetLocationFromQueue(Leaf):##Leaf？
    """Gets a location name from the queue"""

    def _do_setup(self):    ##__init__(self, name, location_dict)
        
        self.location_dict =self.inputs["get_locations"] #location_dict
        self.loc_list= self.inputs["get_list"]#self.location_dict.keys())
        
        
    def _do_tick(self):     #update(self):
        """Checks for the status of the navigation action"""
        
        
        if self.inputs["get_list"]==[]:
            self.loc_list= self.loc_list
            self.location_dict =self.location_dict

        else:
            self.loc_list= self.inputs["get_list"]
            self.location_dict =self.inputs["get_locations"]
            
            self.logwarn(self.inputs["get_list"])
        random.shuffle(self.loc_list)    
        target_location = self.loc_list.pop()
        self.logwarn(f"Selected location {target_location}")#logger.info(f"Selected location {target_location}")
        self.outputs["target_pose"] = self.location_dict[target_location]

        return NodeMsg.SUCCEEDED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE



@define_bt_node(NodeConfig(
    options={"pose_type":type,"namespace":str},   ############ add namespace
    inputs={"get_pose":OptionRef("pose_type")},  #{"get_msg":NodeData(data_type=list,initial_value=['location4'])},#list,initial_value=[]#str},},#
    outputs={},
    max_children=0,
    ))

        
class move(Leaf):#(Action):
    @abstractmethod
    def create_move_base_goal(self, x, y, theta):
        # Creates a MoveBaseGoal message from a 2D navigation pose
            goals = NavigateToPose.Goal()
            goals.pose.header.frame_id = "map"
            goals.pose.header.stamp = self._ros_node.get_clock().now().to_msg()
            #node.get_clock().now().to_msg()
            goals.pose.pose.position.x = x
            goals.pose.pose.position.y = y
            quat = transforms3d.euler.euler2quat(0, 0, theta)
            goals.pose.pose.orientation.w = quat[0]
            goals.pose.pose.orientation.x = quat[1]
            goals.pose.pose.orientation.y = quat[2]
            goals.pose.pose.orientation.z = quat[3]
            return goals
            
    @abstractmethod        
    def goal_callback(self, future):
            res = future.result()
            if res is None or not res.accepted:
                return
            future = res.get_result_async()
            future.add_done_callback(self.goal_result_callback)
    @abstractmethod
    def goal_result_callback(self, future):
        # If there is a result, we consider navigation completed and save the
        # result code to be checked in the `update()` method.
            self.goal_status = future.result().status
            
            
    def _do_setup(self):
        
        self.goal_status = None
        self.namespace = self.ros_node.get_namespace()

    def _do_tick(self):
        self.goal_status = None
        self.client = ActionClient(self._ros_node, NavigateToPose, self.namespace+"/navigate_to_pose",callback_group=ReentrantCallbackGroup())#"/"+
        self.client.wait_for_server()
        
        self.pose=self.inputs["get_pose"]#self.pose=[2.75, 2.5, -1.571]
        x, y, theta = self.pose
        self.goalpose = self.create_move_base_goal(x, y, theta)
        self.send_goal_future = self.client.send_goal_async(self.goalpose)
        self.send_goal_future.add_done_callback(self.goal_callback)
        
        
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        while self.goal_status is None:
            if self.goal_status == GoalStatus.STATUS_SUCCEEDED:
                self.ros_node.get_logger().warn(f"Finish move action!")
                return NodeMsg.SUCCEEDED
        
        if self.goal_status is not None:
            if self.goal_status == GoalStatus.STATUS_SUCCEEDED:
                return NodeMsg.SUCCEEDED
            else:
                return NodeMsg.FAILED
        self.logwarn(self.goal_status)
        
        #return NodeMsg.RUNNING

            
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE



@define_bt_node(NodeConfig(
    options={"pose_type":type},   ############ add namespace
    inputs={"get_pose":OptionRef("pose_type")},  #{"get_msg":NodeData(data_type=list,initial_value=['location4'])},#list,initial_value=[]#str},},#
    outputs={"reach_result":bool, "reach_pose":list},
    max_children=0,
    ))

class RandomMove(Leaf):#(Action):
    @abstractmethod
    def create_move_base_goal(self, x, y, theta):
        # Creates a MoveBaseGoal message from a 2D navigation pose
            goals = NavigateToPose.Goal()
            goals.pose.header.frame_id = "map"
            goals.pose.header.stamp = self._ros_node.get_clock().now().to_msg()
            #node.get_clock().now().to_msg()
            goals.pose.pose.position.x = x
            goals.pose.pose.position.y = y
            quat = transforms3d.euler.euler2quat(0, 0, theta)
            goals.pose.pose.orientation.w = quat[0]
            goals.pose.pose.orientation.x = quat[1]
            goals.pose.pose.orientation.y = quat[2]
            goals.pose.pose.orientation.z = quat[3]
            return goals
            
    @abstractmethod        
    def goal_callback(self, future):
        try:
            res = future.result()
            if res is None or not res.accepted:
                return
            

            self.goal_handle=res
            if self.help_received:
                self.ros_node.get_logger().info("Help received before goal execution, canceling...")
                self.cancel_future = res.cancel_goal_async()
                self.cancel_future.add_done_callback(self.cancel_done_callback)
                return


            future = res.get_result_async()
            future.add_done_callback(self.goal_result_callback)

        except Exception as e:
            self.ros_node.get_logger().error(f"Error in goal callback: {str(e)}")
            self.goal_status = GoalStatus.STATUS_ABORTED


    @abstractmethod
    def goal_result_callback(self, future):
        # If there is a result, we consider navigation completed and save the
        # result code to be checked in the `update()` method.
        try:
            self.goal_status = future.result().status
            self.ros_node.get_logger().info(f"Goal result callback: {self.goal_status}")
        except Exception as e:
            self.ros_node.get_logger().error(f"Error in goal callback: {str(e)}")
            self.goal_status = GoalStatus.STATUS_ABORTED


    
    @abstractmethod
    def help_callback(self, msg):
        """Callback when help message is received"""
        self.ros_node.get_logger().info("Help message received! Stopping navigation...")
        self.help_received = True
        #self.ros_node.get_logger().info(f"{self.goal_handle.is_active}")
        self.ros_node.get_logger().info(f"{self.goal_handle}")

        
        # Cancel current goal if active
        if self.goal_handle is not None:#self.goal_handle and self.goal_handle.is_active:
            self.ros_node.get_logger().info("Canceling active navigation goal...")
            self.cancel_future = self.goal_handle.cancel_goal_async()
            self.cancel_future.add_done_callback(self.cancel_done_callback)
    
    @abstractmethod
    def cancel_done_callback(self, future):
        """Callback when goal cancellation is complete"""
        cancel_response = future.result()
        if cancel_response.goals_canceling:
            self.ros_node.get_logger().info("Goal successfully canceled")
            self.goal_status = GoalStatus.STATUS_CANCELED
        else:
            self.ros_node.get_logger().info("Goal failed to cancel")
            
            
    def _do_setup(self):
        
        self.goal_status = None
        self.help_received = False
        self.capability_request_sub = self.ros_node.create_subscription(
            Bool, #list 和 传递次数 发送人名称
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/stop_move", # ask_for_help", #f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_request",
            self.help_callback,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            )
        self.task_publish_pub = self.ros_node.create_publisher(
            DistanceCalculation, 
            f"/global/public_task_publish",
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            )
        ########################## 不知道什么用但关了 change 0427 ###########################
        # self.best_cap_req_pub = self.ros_node.create_subscription(
        #     CapabilityRequest,
        #     f"{self.ros_node.get_namespace()}/BehaviorTreeNode/Coop/capability_request",
        #     self.help_callback,
        #     qos_profile=QoSProfile(
        #     reliability=QoSReliabilityPolicy.RELIABLE,
        #     durability=QoSDurabilityPolicy.VOLATILE,
        #     history=QoSHistoryPolicy.KEEP_LAST,
        #     depth=10,),
        #     callback_group=ReentrantCallbackGroup()
        #     )
        ########################## 不知道什么用但关了 change 0427 ###########################
        ########################## 不知道什么用但关了 change 0427 ###########################

    def _do_tick(self):
        self.goal_status = None
        self.client = ActionClient(self._ros_node, NavigateToPose, self.ros_node.get_namespace()+"/navigate_to_pose",callback_group=ReentrantCallbackGroup())
        self.client.wait_for_server()
        
        self.pose=self.inputs["get_pose"]#self.pose=[2.75, 2.5, -1.571]
        x, y, theta = self.pose
        self.goalpose = self.create_move_base_goal(x, y, theta)
        self.send_goal_future = self.client.send_goal_async(self.goalpose)
        self.send_goal_future.add_done_callback(self.goal_callback)
        
        
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        while self.goal_status is None:
            # if self.goal_status == GoalStatus.STATUS_SUCCEEDED or self.goal_status == 4:
            #     self.outputs["reach_result"]=True
            #     self.task_publish_pub.publish(DistanceCalculation(own_name="/turtle0",other_name="blue"))
            #     return NodeMsg.SUCCEEDED
            time.sleep(0.05)
        
        if self.goal_status is not None:
            self.logwarn(self.goal_status)
            self.outputs["reach_pose"]=self.inputs["get_pose"]
            if self.goal_status == GoalStatus.STATUS_SUCCEEDED or self.goal_status == 4:
                self.outputs["reach_result"]=True
                #self.task_publish_pub.publish(DistanceCalculation(own_name=f"{self.ros_node.get_namespace()}",other_name="blue"))
                return NodeMsg.SUCCEEDED
            elif self.goal_status == GoalStatus.STATUS_CANCELED or self.goal_status == 3:
                self.outputs["reach_result"]=True
                return NodeMsg.SUCCEEDED
            else:
                self.outputs["reach_result"]=False
                return NodeMsg.FAILED
        
        
        #return NodeMsg.RUNNING

            
    def _do_shutdown(self):
        self.goal_status = None
        self.help_received = False
        pass

    def _do_reset(self):
        self.goal_status = None
        self.help_received = False
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    






@define_bt_node(NodeConfig(
    options={"pose_type":type},   ############ add namespace
    inputs={"get_pose":OptionRef("pose_type")},  #{"get_msg":NodeData(data_type=list,initial_value=['location4'])},#list,initial_value=[]#str},},#
    outputs={"reach_result":bool, "reach_pose":list},
    max_children=0,
    ))
class RandomMove1(Leaf):#(Action):
    @abstractmethod
    def create_move_base_goal(self, x, y, theta):
        # Creates a MoveBaseGoal message from a 2D navigation pose
            goals = NavigateToPose.Goal()
            goals.pose.header.frame_id = "map"
            goals.pose.header.stamp = self._ros_node.get_clock().now().to_msg()
            #node.get_clock().now().to_msg()
            goals.pose.pose.position.x = x
            goals.pose.pose.position.y = y
            quat = transforms3d.euler.euler2quat(0, 0, theta)
            goals.pose.pose.orientation.w = quat[0]
            goals.pose.pose.orientation.x = quat[1]
            goals.pose.pose.orientation.y = quat[2]
            goals.pose.pose.orientation.z = quat[3]
            return goals
            
    @abstractmethod        
    def goal_callback(self, future):
        try:
            res = future.result()
            if res is None or not res.accepted:
                return
            

            self.goal_handle=res
            if self.help_received:
                self.ros_node.get_logger().info("Help received before goal execution, canceling...")
                self.cancel_future = res.cancel_goal_async()
                self.cancel_future.add_done_callback(self.cancel_done_callback)
                return


            future = res.get_result_async()
            future.add_done_callback(self.goal_result_callback)

        except Exception as e:
            self.ros_node.get_logger().error(f"Error in goal callback: {str(e)}")
            self.goal_status = GoalStatus.STATUS_ABORTED


    @abstractmethod
    def goal_result_callback(self, future):
        # If there is a result, we consider navigation completed and save the
        # result code to be checked in the `update()` method.
        try:
            self.goal_status = future.result().status
            self.ros_node.get_logger().info(f"Goal result callback: {self.goal_status}")
        except Exception as e:
            self.ros_node.get_logger().error(f"Error in goal callback: {str(e)}")
            self.goal_status = GoalStatus.STATUS_ABORTED


    
    @abstractmethod
    def help_callback(self, msg):
        """Callback when help message is received"""
        self.ros_node.get_logger().info("Help message received! Stopping navigation...")
        self.help_received = True
        #self.ros_node.get_logger().info(f"{self.goal_handle.is_active}")
        self.ros_node.get_logger().info(f"{self.goal_handle}")

        
        # Cancel current goal if active
        if self.goal_handle is not None:#self.goal_handle and self.goal_handle.is_active:
            self.ros_node.get_logger().info("Canceling active navigation goal...")
            self.cancel_future = self.goal_handle.cancel_goal_async()
            self.cancel_future.add_done_callback(self.cancel_done_callback)
    
    @abstractmethod
    def cancel_done_callback(self, future):
        """Callback when goal cancellation is complete"""
        cancel_response = future.result()
        if cancel_response.goals_canceling:
            self.ros_node.get_logger().info("Goal successfully canceled")
            self.goal_status = GoalStatus.STATUS_CANCELED
        else:
            self.ros_node.get_logger().info("Goal failed to cancel")
            
            
    def _do_setup(self):
        
        self.goal_status = None
        self.help_received = False
        self.running_status = None
        self.capability_request_sub = self.ros_node.create_subscription(
            Bool, #list 和 传递次数 发送人名称
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/stop_move", # ask_for_help", #f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_request",
            self.help_callback,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            )
        self.task_publish_pub = self.ros_node.create_publisher(
            DistanceCalculation, 
            f"/global/public_task_publish",
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            )
        self.best_cap_req_pub = self.ros_node.create_subscription(
            CapabilityRequest,
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/Coop/capability_request",
            self.help_callback,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
            callback_group=ReentrantCallbackGroup()
            )

    def _do_tick(self):
        # if self.simulate_tick:
        #     self.logdebug("Simulating tick. Action is not executing!")
        #     return NodeMsg.RUNNING
        if self.goal_status is None and self.running_status is None:
            self.running_status = True
            self.client = ActionClient(self._ros_node, NavigateToPose, self.ros_node.get_namespace()+"/navigate_to_pose",callback_group=ReentrantCallbackGroup())
            self.client.wait_for_server()
            
            self.pose=self.inputs["get_pose"]#self.pose=[2.75, 2.5, -1.571]
            x, y, theta = self.pose
            self.goalpose = self.create_move_base_goal(x, y, theta)
            self.send_goal_future = self.client.send_goal_async(self.goalpose)
            self.send_goal_future.add_done_callback(self.goal_callback)
        
        
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        if self.goal_status is None and self.running_status:
            # if self.goal_status == GoalStatus.STATUS_SUCCEEDED or self.goal_status == 4:
            #     self.outputs["reach_result"]=True
            #     self.task_publish_pub.publish(DistanceCalculation(own_name="/turtle0",other_name="blue"))
            #     return NodeMsg.SUCCEEDED
            time.sleep(0.5)
            return NodeMsg.RUNNING
        
        if self.goal_status is not None:
            self.logwarn(self.goal_status)
            self.outputs["reach_pose"]=self.inputs["get_pose"]
            if self.goal_status == GoalStatus.STATUS_SUCCEEDED or self.goal_status == 4:
                self.outputs["reach_result"]=True
                #self.task_publish_pub.publish(DistanceCalculation(own_name=f"{self.ros_node.get_namespace()}",other_name="blue"))
                return NodeMsg.SUCCEEDED
            elif self.goal_status == GoalStatus.STATUS_CANCELED or self.goal_status == 3:
                self.outputs["reach_result"]=True
                return NodeMsg.SUCCEEDED
            else:
                self.outputs["reach_result"]=False
                return NodeMsg.FAILED
        
        
        #return NodeMsg.RUNNING

            
    def _do_shutdown(self):
        self.logwarn("move shutdown 1")
        self.goal_status = None
        self.help_received = False
        self.running_status = None
        return NodeMsg.IDLE#pass

    def _do_reset(self):
        self.logwarn("move reset 1")
        self.goal_status = None
        self.help_received = False
        self.running_status = None
        #self.client.destroy()
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE



    



@define_bt_node(
    NodeConfig(
        options={},#"pos_list":dict
        inputs={"object_position":list, "manager_name":str},
        outputs={"result":bool},
        max_children=0,
        ))
class ClearObject(Leaf):
    """Reach the task object and clear it"""

    def _do_setup(self):
        
        #self.task_object_position =self.inputs["object_position"]
        
        self.delete_cli = self.ros_node.create_client(DeleteEntity, "/delete_entity")  #add
        while not self.delete_cli.wait_for_service(timeout_sec=1.0):  #add
            self.ros_node.get_logger().info("Waiting for delete service...")
        

    
    
    
    def _do_tick(self):  
        """Checks for the status of the navigation action"""
        
        #self.task_object_position =self.inputs["object_position"]
        # while self.inputs["object_position"]==[]:
        #     time.sleep(0.1)

            
        #self.logwarn(f"Start clearing object at [{self.task_object_position}]")
        
        self.clear_object_pub = self.ros_node.create_publisher(
            std_msgs.msg.Float32MultiArray,
            f"/clear_block",
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
        )

        msg = std_msgs.msg.Float32MultiArray()
        msg.data.fromlist(self.inputs["object_position"])
        msg.data[0] = msg.data[0] - 0.3
        self.ros_node.get_logger().info(f"Clear object at [{msg.data}]")
        self.clear_object_pub.publish(msg)


        # 调用以下服务可以删除block
        # req = DeleteEntity.Request()
        # req.name = 'red1_block'
        # self.delete_cli.call_async(req)
        
        
        #self.outputs["target_pose"] = self.location_dict[target_location]
        self.outputs["result"] = True

        return NodeMsg.SUCCEEDED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    


@define_bt_node(
    NodeConfig(
        options={},
        inputs={"publisher":str},
        outputs={"point_position":list},
        max_children=0,
        ))
class GetPointPosition(Leaf):
    """Reach the task object and clear it"""

    def _do_setup(self):
        
        
        self.point_list=[]
        

    def point_get_cb(self,msg):
        
        self.point_list=[msg.x,msg.y,0.0]
    
    
    def _do_tick(self):  
        """Point msg to list(position)"""
        
        self.publisher = self.inputs["publisher"]
        
        self.point_position_sub = self.ros_node.create_subscription(
            Point,
            f"{self.publisher}/task_center_position",
            self.point_get_cb,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.VOLATILE,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,),
        )

        
        while self.point_list == []:
            time.sleep(0.1)

        
        self.outputs["point_position"] = self.point_list

        return NodeMsg.SUCCEEDED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE