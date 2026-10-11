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
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        self.outputs["Locations"] = self.locations
        self.outputs["loc_list"] = self.loc_list#self.options["list_value"]
        if self.outputs["loc_list"] is not None: #target_color is not None:
            self.logwarn(self.outputs.callbacks)#
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
        self.namespace = self.options["namespace"]

    def _do_tick(self):
        self.goal_status = None
        self.client = ActionClient(self._ros_node, NavigateToPose, "/"+self.namespace+"/navigate_to_pose",callback_group=ReentrantCallbackGroup())
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

