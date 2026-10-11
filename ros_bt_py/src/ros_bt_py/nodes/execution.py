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
from geometry_msgs.msg import PoseStamped
from std_msgs.msg import Bool,Int16
import math
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)
from ros_bt_py_interfaces.msg import (
    DistanceCalculation, 
    CapabilityRequest, 
    CapabilityFeedback, 
    JoinTeam, 
    CapList
)
from ros_bt_py.exceptions import BehaviorTreeException
from rclpy.parameter import Parameter
import time



from ros_bt_py_interfaces.msg import UtilityBounds











@define_bt_node(
    NodeConfig(
        options={},#"symbol_name":str,
        inputs={},
        outputs={"origional_cap_order":list},
        max_children=0,
        ))
class GetTaskOrder(Leaf):
    """Check the state of a symbol"""

    def _do_setup(self):    
        
        self.origional_req_pub = self.ros_node.create_publisher(
                CapabilityRequest, #list 和 传递次数 发送人名称
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/origional_capability_request",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        
    def _do_tick(self):     
        """Output the value of a symbol"""
        if not self.ros_node.has_parameter("origional_task_req"):
            self.outputs["origional_cap_order"]=[]
            return NodeMsg.SUCCEEDED
        else:
            self.task_list:list = self.ros_node.get_parameter("origional_task_req").value
            self.task_order = []
            index = 0
            for v in self.task_list:
                index = index + 1
                if v == 1:
                    self.task_order.append([index])
                if v > 1:
                    index_list = [index]*v
                    self.task_order.append(index_list)
            self.ros_node.get_logger().warn(f"task order: {self.task_order}")
            if len(self.task_order) > 0:
                self.outputs["origional_cap_order"]=self.task_order  ### list格式，还没有转换成CapReq格式
        if not len(self.task_order) == 0:
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
        ####### remote 和 cap+get task order并行
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE






@define_bt_node(
    NodeConfig(
        options={},
        inputs={"origional_cap_order":list,"team_cap_list":list},
        outputs={},#"param_value":OptionRef("param_type")
        max_children=0,
    ))
class CheckCapExecutor(Leaf):##Leaf？
    """Gets a location name from the queue"""


    def _do_setup(self):#setup(self):
        self.origional_req_pub = self.ros_node.create_publisher(
                CapabilityRequest, #list 和 传递次数 发送人名称
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/origional_capability_request",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        
        
    def _do_tick(self):##tick(self):      #update(self):    
        
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        self.task_order = self.inputs["origional_cap_order"]
        if not self.task_order == []:
            self.team_cap_record:list = self.inputs["team_cap_list"]
            self.ros_node.get_logger().warn(f"team cap list: {self.team_cap_record}")
            #=self.ros_node.get_parameter("team_cap_record").value.cap_list
            self.team_member_record:list = self.ros_node.get_parameter("team_member_record").value
        else:
            return NodeMsg.SUCCEEDED
        self.new_cap_order:list = []
        self.new_cap_owner:list = []
        last_index = 0
        for v in self.team_cap_record:
            self.ros_node.get_logger().warn(f"v:{v}")
            index = self.team_cap_record.index(v,last_index)  
            ### 防止有相同的能力列表，每次都从上一次结束的位置后面开始搜索
            for w in v: ### 如果一个agent提供不止一种能力
                self.new_cap_order.append(w)   ### 依次取出能力，形成1cap-1owner序列
                self.new_cap_owner.append(self.team_member_record[index])
            last_index = index+1
        

        for v in self.task_order: ### 每种能力
            last_index = 0
            for w in v:   ### 该种类能力的需求个数
                self.target_index = self.new_cap_order.index(w,last_index)
                self.target_robot = self.new_cap_owner[self.target_index]

                ### 发出执行指令并回收结果

                


                self.ros_node.get_logger().warn(f"target cap:{w}, target robot: {self.target_robot}")

                ### 如果成功
                last_index = self.target_index+1


        if last_index > 0: #target_color is not None:
            
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
        options={"file_path":str},#"symbol_name":str,
        inputs={"team_cap_list":list, "team_list":list, "task_name":str},
        outputs={"cap_req":list},
        max_children=0,
        ))
class GetCapList(Leaf):
    """Check the state of a symbol"""

    @abstractmethod    
    def receive_result_cb(self,msg):
        if msg.own_name is not None:
            # owner_name=self.cap_owner, other_name=self.cap_name
            self.ros_node.get_logger().warn(f"11111118888888 get cap exe result !!!!!")
            self.receive_cap_result_name = msg.other_name
            self.receive_cap_result_owner = msg.own_name

    @abstractmethod
    def get_parallel_number_cb(self,msg):
        if msg is not None:
            self.ros_node.get_logger().warn(f" Get parallel cap num!")
            self.get_parallel_num=True
            self.ros_node.set_parameters([Parameter('parallel_number',Parameter.Type.INTEGER,msg.data)])


    ## 增加每个cap的input（NodeData格式），cap input前面的输入部分改成sub+msg2filed格式
    ## cap req也要改，改成input_list+caplist格式
    
    def _do_setup(self):    
        
        self.cap_file = self.options["file_path"]
        self.receive_cap_result_name = None
        self.receive_cap_result_owner = None
        #self.target = self.inputs["target_color"]
        with open(self.options["file_path"]) as f:
            self.caps = yaml.load(f, Loader=yaml.FullLoader)
        # self.execute_req_pub = self.ros_node.create_publisher(
        #         DistanceCalculation, #list 和 传递次数 发送人名称
        #         f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_exe_allocation",
        #         qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #         callback_group=ReentrantCallbackGroup()    ### change 0701
        # )
        # self.execute_finish_sub = self.ros_node.create_subscription(
        #     DistanceCalculation,
        #     f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_finish_result",
        #     self.receive_result_cb,
        #     qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #         callback_group=ReentrantCallbackGroup()
        # )
        self.outputs["cap_req"] = []
        
        ### 循环取出cap_list中的元素，发送信息给执行节点，收到msg返回后再循环+1
        count = 0
        #self.cap_execute:list=[]
        self._old_state = self.state
        self.state = None
        self.count = 0
        self.waiting_symbol = False
        self.number = 0
        self.num_count = 0

        ### 标志该任务所需的能力均执行完毕
        self.task_exe_finish_pub = self.ros_node.create_publisher(
                DistanceCalculation, #list 和 传递次数 发送人名称
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/task_exe_finish",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
        )
        self.parallel_cap_remote = self.ros_node.create_subscription(
                Int16,
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/parallel_cap_exe",
                self.get_parallel_number_cb,
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup())
        self.ros_node.declare_parameter("parallel_number",1)
        self.execute_finish_sub = None
        self.cap_to_exe=[]
        self.member_to_exe=[]

        self.initial_exe_list_symbol=False
        self.finish_exe_number=0
        self.exe_member_list=[]
        self.exe_cap_name=None
        self.get_parallel_num=False


        
    def _do_tick(self):     
        """Output the value of a symbol"""
        # self.outputs["cap_req"] = []
        
        # ### 循环取出cap_list中的元素，发送信息给执行节点，收到msg返回后再循环+1
        # count = 0
        # self.cap_execute:list=[]



        


        # if self.simulate_tick:
        #     # self.ros_node.get_logger().debug(f"Simulating tick. {self.name} is not executing!")
        #     if self.succeed_always:
        #         return NodeMsg.SUCCEEDED
        #     return NodeMsg.RUNNING

        # 状态恢复

        if self.simulate_tick:
            return NodeMsg.RUNNING
        
        if self.state == NodeMsg.PAUSED:
            self.state = self._old_state

        # 处理不同的状态
        if self.state == NodeMsg.IDLE:
            self.ros_node.get_logger().warn(f"start cap execution !")
            return NodeMsg.ASSIGNED


        if self.state == NodeMsg.ASSIGNED:
            self.ros_node.get_logger().warn(f"start cap assigned ! number:{self.number}, cap number:{self.finish_exe_number}")
            return self._tick_assigned()

        if self.state == NodeMsg.RUNNING:
            self.ros_node.get_logger().warn(f"start cap running !")
            return self._tick_running()

        # 未定义状态，返回失败
        return NodeMsg.FAILED



    def _tick_assigned(self) -> str:
        if self.ros_node.get_parameter("center_symbol").value:
            if not self.initial_exe_list_symbol:
                ## 记录原任务中所需要的能力
                task_req_list:list=self.ros_node.get_parameter("origional_task_req").value
                self.ros_node.get_logger().warn(f"task_req:{task_req_list}")
                
                for v in range(len(task_req_list)):
                    
                    if not task_req_list[v]==0:
                        self.cap_to_exe.append(self.caps[v+1])
                        self.member_to_exe.append([])

                self.ros_node.get_logger().warn(f"cap to exe:{self.cap_to_exe},member to exe:{self.member_to_exe}")
                while self.count<len(self.inputs["team_cap_list"]):
            #for v in self.inputs["team_cap_list"]:
            #    #index = v.index
            #    self.outputs["cap_req"].append(self.caps[v])
                    
                    self.cap_list = self.inputs["team_cap_list"][self.count]

                    #self.cap_list = self.caps[self.inputs["team_cap_list"][count]]

                    self.cap_owner = self.inputs["team_list"][self.count]
                    self.ros_node.get_logger().warn(f"count: {self.count}, owner:{self.cap_owner} for cap :{self.cap_list}")
                    self.parallel_cap_number = self.ros_node.create_publisher(
                            Int16,
                            f"{self.cap_owner}/BehaviorTreeNode/parallel_cap_exe",
                            qos_profile=QoSProfile(
                            reliability=QoSReliabilityPolicy.RELIABLE,
                            durability=QoSDurabilityPolicy.VOLATILE,
                            history=QoSHistoryPolicy.KEEP_LAST,
                            depth=10,),
                            callback_group=ReentrantCallbackGroup())
                    #if self.cap_owner==self.ros_node.get_namespace():
                    self.parallel_cap_number.publish(Int16(data=len(self.cap_list)))
                    ########### 问题：收到的时间晚于建立sub的时间
                    self.ros_node.get_logger().warn(f"Send parallel cap number to {self.cap_owner}!")

                    ## 记录每个能力需要的执行成员
                    for n in self.cap_list:
                        for p in self.cap_to_exe:
                            self.ros_node.get_logger().warn(f"cap:{n},to_exe{p}")
                            if self.caps[n]==p:
                                self.member_to_exe[self.cap_to_exe.index(p)].append(self.cap_owner)
                                #break
                    
                    self.count = self.count+1

                self.ros_node.get_logger().warn(f"cap to exe:{self.cap_to_exe},member to exe:{self.member_to_exe}")
                self.initial_exe_list_symbol=True
                




            if self.finish_exe_number<len(self.cap_to_exe):
                
                
                
                self.exe_member_list=self.member_to_exe[self.finish_exe_number]
                self.exe_cap_name=self.cap_to_exe[self.finish_exe_number]
                
                new_state = NodeMsg.RUNNING
                
                self.number = 0


                self.ros_node.get_logger().warn(f"cap execution node is assigned !!!!!!!")

                return new_state
        
            else:
                self.task_exe_finish_pub.publish(DistanceCalculation(own_name=self.ros_node.get_namespace(),other_name=self.inputs["task_name"]))
                return NodeMsg.SUCCEEDED
        else:
            self.ros_node.get_logger().warn(f"Member waiting for self cap exe ! ")
            # self.parallel_cap_remote = self.ros_node.create_subscription(
            #     Int16,
            #     f"{self.ros_node.get_namespace()}/BehaviorTreeNode/parallel_cap_exe",
            #     self.get_parallel_number_cb,
            #     qos_profile=QoSProfile(
            #     reliability=QoSReliabilityPolicy.RELIABLE,
            #     durability=QoSDurabilityPolicy.VOLATILE,
            #     history=QoSHistoryPolicy.KEEP_LAST,
            #     depth=10,),
            #     callback_group=ReentrantCallbackGroup())

            while self.get_parallel_num == False:
                time.sleep(0.05)
            new_state = NodeMsg.SUCCEEDED
            
            return new_state

    def _tick_running(self):

            if self.number == 0:
                self.receive_cap_result_name = None
                self.receive_cap_result_owner = None
                self.member_number=len(self.exe_member_list)
                
            self.ros_node.get_logger().warn(f"cap execution node is running !!!!!!!")
            self.ros_node.get_logger().warn(f"cap execution num :{self.number}, member list: {self.exe_member_list}, self.waiting:{self.waiting_symbol}")
            
            
            if self.number < self.member_number and self.waiting_symbol is False:
                # self.cap_name = self.cap_list[self.number]
                # self.cap_name = self.caps[self.cap_name]
                self.cap_name=self.exe_cap_name
                self.cap_owner=self.exe_member_list[self.number]
                self.ros_node.get_logger().warn(f"cap_name:{self.cap_name}, cap_owner:{self.cap_owner}")
                #self.cap_execute.append(self.cap_name) # [count])
                
                
                self.execute_req_pub = self.ros_node.create_publisher(
                DistanceCalculation, #list 和 传递次数 发送人名称
                f"{self.cap_owner}/BehaviorTreeNode/capability_exe_allocation",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
                )
                
                
                self.execute_req_pub.publish(DistanceCalculation(own_name=self.cap_owner, other_name=self.cap_name))
                self.execute_finish_sub = self.ros_node.create_subscription(
                DistanceCalculation,
                f"{self.cap_owner}/BehaviorTreeNode/capability_finish_result",
                self.receive_result_cb,
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()
                )
                self.waiting_symbol = True
                self.number = self.number+1
                

            if self.waiting_symbol:
                time_sum = 0
                self.num_count+=1
                if self.num_count>50:
                    return NodeMsg.FAILED
                self.ros_node.get_logger().warn(f"receive cap name:{self.receive_cap_result_name}, receive cap owner:{self.receive_cap_result_owner}")
                while not self.receive_cap_result_name == self.cap_name:
                    time.sleep(0.5)
                    time_sum = time_sum + 0.5
                    if time_sum > 1.5:
                        self.waiting_symbol = True
                        return NodeMsg.RUNNING
                while not self.receive_cap_result_owner == self.cap_owner:
                    time.sleep(0.5)
                    time_sum = time_sum + 0.5
                    if time_sum > 1.5:
                        self.waiting_symbol = True
                        return NodeMsg.RUNNING
                # if time_sum > 10:
                #     return NodeMsg.FAILED
                self.ros_node.get_logger().warn(f"last member exe is done !!!")
                self.waiting_symbol = False



            if self.number == self.member_number and self.waiting_symbol is False:
                self.finish_exe_number=self.finish_exe_number+1

                
                

            #if self.count==len(self.inputs["team_cap_list"]):
                
                self.state = NodeMsg.ASSIGNED
                new_state = self.state
            else:
                return NodeMsg.RUNNING

            return new_state




    # def _tick_running(self):

    #     if self.outputs["cap_req"] is not None:
    #         return NodeMsg.SUCCEEDED
    #     else:
    #         return NodeMsg.FAILED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        #self._stop_calls_action_clients_async_service_clients()
        self.receive_cap_result_name = None
        self.receive_cap_result_owner = None
        self.get_parallel_num = False

        self.cap_to_exe=[]
        self.member_to_exe=[]

        self.initial_exe_list_symbol=False
        self.finish_exe_number=0
        self.exe_member_list=[]
        self.exe_cap_name=None

        #self.cap_execute:list=[]
        self._old_state = NodeMsg.IDLE
        self.state = None
        self.count = 0
        self.waiting_symbol = False
        self.number = 0
        # if self.execute_finish_sub is not None:
        #     self.ros_node.destroy_subscription(self.execute_finish_sub)

        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    




@define_bt_node(
    NodeConfig(
        version="0.1.0",
        options={"needed_successes": int},
        inputs={},
        outputs={},
        max_children=None,
    )
)
class ParallelCaps(FlowControl):
    """
    The Parallel ticks all of its children every tick.

    Its success is determined depending on `needed_successes` - if at
    least `needed_success` of its children have returned SUCCEEDED,
    the Parallel too returns SUCCEEDED.

    On the other hand, if more than N - `needed_successes` of its
    children have returned FAILED, so will the Parallel.

    Different values of `needed_successes` can produce behavior
    similar to the "barrier" (wait until all actions are finished) and
    "heureka" (first success aborts all other actions) parallel
    programming primitives, and more.

    To prevent thrashing when multiple children return RUNNING for
    different lengths of time, a child that has returned SUCCEEDED or
    FAILED once will not be ticked again untill **all** children have
    reached a result.

    """

    def _do_setup(self):
        if len(self.children) < self.options["needed_successes"]:
            raise BehaviorTreeException(
                (
                    "Option value needed_successes (%d) cannot be larger than "
                    "the number of children (%d)"
                )
                % (self.options["needed_successes"], len(self.children))
            )
        for child in self.children:
            child.setup()

    def _do_tick(self):
        # Just like Sequence and Fallback, reset after having returned
        # SUCCEEDED or FAILED once

        self.parallel_successes = self.ros_node.get_parameter("parallel_number").value# + 1
        self.ros_node.get_logger().warn(f"11111111 parallel cap number:{self.parallel_successes}")

        if self.state in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
            # for child in self.children:
            #     child.reset()
            return NodeMsg.IDLE

        self.successes = 0
        self.failures = 0
        for child in self.children:
            if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
                child.tick()
            if child.state == NodeMsg.SUCCEEDED:
                self.successes += 1
                #self.children.remove(child)
            if child.state == NodeMsg.FAILED:
                self.failures += 1
                #self.children.remove(child)
        self.ros_node.get_logger().warn(f"11111111 success cap number:{self.successes}")
        if self.successes >= self.parallel_successes:
            # untick all running children
            # for child in self.children:
            #     if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
            #         child.reset()#untick()
            return NodeMsg.SUCCEEDED
        elif self.failures > len(self.children) - self.parallel_successes:
            # untick all running children
            for child in self.children:
                if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
                    child.untick()
            return NodeMsg.FAILED
        # if self.state == NodeMsg.RUNNING:
        #     return self._tick_running()
        return NodeMsg.RUNNING

    def _tick_running(self) -> str:
        for child in self.children:
            if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
                child.tick()
            if child.state == NodeMsg.SUCCEEDED:
                self.successes += 1
                self.children.remove(child)
            if child.state == NodeMsg.FAILED:
                self.failures += 1
                self.children.remove(child)
        self.ros_node.get_logger().warn(f"11111111 success cap number:{self.successes}")
        if self.successes >= self.parallel_successes:
            # untick all running children
            # for child in self.children:
            #     if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
            #         child.reset()#untick()
            return NodeMsg.SUCCEEDED
        elif self.failures > len(self.children) - self.parallel_successes:
            # untick all running children
            for child in self.children:
                if child.state not in [NodeMsg.SUCCEEDED, NodeMsg.FAILED]:
                    child.untick()
            return NodeMsg.FAILED
        else:
            return NodeMsg.RUNNING

    def _do_shutdown(self):
        for child in self.children:
            child.shutdown()

    def _do_reset(self):
        for child in self.children:
            child.reset()
            self.ros_node.get_logger().warn(f"9999999999999 parallel reset child node: {child.name} 9999999999999")
        return NodeMsg.IDLE

    def _do_untick(self):
        for child in self.children:
            child.untick()
        return NodeMsg.IDLE

    def _do_calculate_utility(self):
        if len(self.children) < self.options["needed_successes"]:
            raise BehaviorTreeException(
                (
                    "Option value needed_successes (%d) cannot be larger than "
                    "the number of children (%d)"
                )
                % (self.options["needed_successes"], len(self.children))
            )

        # This calculation is fairly straightforward:
        #
        # The lower bound for success is the lowest sum of
        # "needed_successes" children's lower success bounds.
        #
        # The upper bound for success is the highest sum of
        # "needed_successes" children's upper success bounds.
        #
        # We get both by first sorting the array of bounds by the
        # respective bound (ascending for the lower bounds, descending
        # for the upper bounds) and then summing the first
        # "needed_successes" values from the sorted array.

        child_bounds = [child.calculate_utility() for child in self.children]
        bounds = UtilityBounds()
        bounds.can_execute = all((b.can_execute for b in child_bounds))

        # Early return if any child cannot execute
        if not bounds.can_execute:
            return bounds

        success_threshold = self.options["needed_successes"]

        best_success_group = sorted(child_bounds, key=lambda b: b.lower_bound_success)[
            :success_threshold
        ]
        bounds.has_lower_bound_success = all(
            (b.has_lower_bound_success for b in best_success_group)
        )
        if bounds.has_lower_bound_success:
            bounds.lower_bound_success = math.fsum(
                (b.lower_bound_success for b in best_success_group)
            )

        worst_success_group = sorted(
            child_bounds, reverse=True, key=lambda b: b.upper_bound_success
        )[:success_threshold]
        bounds.has_upper_bound_success = all(
            (b.has_upper_bound_success for b in worst_success_group)
        )
        if bounds.has_upper_bound_success:
            bounds.upper_bound_success = math.fsum(
                (b.upper_bound_success for b in worst_success_group)
            )

        # Now we do the same for the failures - the only difference
        # here is that failure_threshold is
        #
        # 1 + (num_children - success_threshold)
        #
        # because of the semantics of Parallel explained in the class
        # docstring

        # The minimum number of child failures for an overall Parallel failure
        failure_threshold = 1 + (len(self.children) - success_threshold)

        best_failure_group = sorted(child_bounds, key=lambda b: b.lower_bound_failure)[
            :failure_threshold
        ]
        bounds.has_lower_bound_failure = all(
            (b.has_lower_bound_failure for b in best_failure_group)
        )
        if bounds.has_lower_bound_failure:
            bounds.lower_bound_failure = math.fsum(
                (b.lower_bound_failure for b in best_failure_group)
            )

        worst_failure_group = sorted(
            child_bounds, reverse=True, key=lambda b: b.upper_bound_failure
        )[:failure_threshold]
        bounds.has_upper_bound_failure = all(
            (b.has_upper_bound_failure for b in worst_failure_group)
        )
        if bounds.has_upper_bound_failure:
            bounds.upper_bound_failure = math.fsum(
                (b.upper_bound_failure for b in worst_failure_group)
            )

        return bounds