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
from std_msgs.msg import Bool
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
import array
import tf2_msgs.msg
from tf2_ros import TransformListener, Buffer
import tf2_msgs
from builtin_interfaces.msg import Time





@define_bt_node(
    NodeConfig(
        options={"file_path":str},
        inputs={},
        outputs={"task_name":str, "origional_cap_req":list ,"best_req_msg":CapabilityRequest},
        max_children=0,
        ))
class TaskListInput(Leaf):
    """Check the state of a symbol"""

    @abstractmethod    
    def receive_task_finish_cb(self,msg):
        if msg.own_name is not None:
            self.receive_result_owner = msg.own_name
            self.receive_result_task = msg.other_name


    @abstractmethod    
    def receive_public_task_cb(self,msg):
        if not msg.own_name =='':

            ## 改成任务队列，每次取出2-3个计算前后时间差距，小于0.5s的算成同时发布的任务
            ## 在两个任务中，先计算距离，再同时被包含的情况下计算距离大小和匹配程度，加权决定将哪个作为req msg

            ## 任务队列何时清空？每次把收到任务全部取出，依次计算距离判断是否在范围内（舍弃——计算过于复杂，还是自身先更新距离计算、只接收附近几个robot的任务————要计算与全部robot距离）
            ## （舍弃————距离内的任务再比较，减少计算量）

            # 1# self.task_center_name = msg.own_name
            # 1# self.task_name = msg.other_name

            # self.ros_node.get_logger().warn(f"Idle Robot get a public task msg:{msg} !")
            # self.receive_task_list.append(msg.other_name)
            # self.receive_task_center.append(msg.own_name)
            # self.receive_task_time.append(self.ros_node.get_clock().now().to_msg())
            # if msg.own_name==self.ros_node.get_namespace():
            #     self.enter_task_exe_pub.publish(Bool(data=True))


            # receive_task_sub=self.ros_node.create_subscription(
            #     CapabilityRequest,
            #     f"{msg.own_name}/BehaviorTreeNode/capability_request",
            #     self.capability_request_cb,
            #     qos_profile=QoSProfile(
            #     reliability=QoSReliabilityPolicy.RELIABLE,
            #     durability=QoSDurabilityPolicy.VOLATILE,
            #     history=QoSHistoryPolicy.KEEP_LAST,
            #     depth=10,),
            #     callback_group=ReentrantCallbackGroup()
            # )
            # self.receive_task_sub.append(receive_task_sub)
            # self.receive_task_number = self.receive_task_number + 1





            ## change 0427
            self.ros_node.get_logger().warn(f"Idle Robot get a public task msg:{msg} !")
            
            if msg.own_name==self.ros_node.get_namespace():
                self.enter_task_exe_pub.publish(Bool(data=True))
                self.receive_task_list.append(msg.other_name)
                self.receive_task_center.append(msg.own_name)
                self.receive_task_time.append(self.ros_node.get_clock().now().to_msg())
                self.receive_task_number = self.receive_task_number + 1
            else:
                receive_task_sub=self.ros_node.create_subscription(
                    CapabilityRequest,
                    f"{msg.own_name}/BehaviorTreeNode/capability_request",
                    self.capability_request_cb,
                    qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=10,),
                    callback_group=ReentrantCallbackGroup()
                )
                self.receive_task_sub.append(receive_task_sub)
                sumtime=0
                while sumtime<5:
                    if not self.cap_req_cap ==[]:
                        self.receive_task_list.append(msg.other_name)
                        self.receive_task_center.append(msg.own_name)
                        self.receive_task_time.append(self.ros_node.get_clock().now().to_msg())
                        self.receive_task_number = self.receive_task_number + 1
                        break
                    time.sleep(0.5)
                    sumtime+=0.51
            


    


    @abstractmethod
    def receive_peer_pose_msg_cb(self,msg):
        ns=self.peer_name.lstrip('/')
        if msg.transforms[0].header.frame_id == f'{ns}/odom' and msg.transforms[0].child_frame_id == f'{ns}/base_footprint':#原本frame_id是没有前缀的
            self.peer_transform = msg.transforms[0]

    @abstractmethod
    def receive_pose_msg_cb(self,msg):
        ns=self.ros_node.get_namespace().lstrip('/')
        if msg.transforms[0].header.frame_id == f'{ns}/odom' and msg.transforms[0].child_frame_id == f'{ns}/base_footprint':#原本frame_id是没有前缀的
            self.transform = msg.transforms[0]





    ###########################################################
    def capability_request_cb(self,msg):
        if msg is not None:
            self.cap_req_depth.append(msg.depth)
            self.cap_req_cap.append(msg.cap_req_list[0])
            self.cap_req_center.append(msg.manager)
            self.cap_req_time.append(self.ros_node.get_clock().now().to_msg())
            self.ros_node.get_logger().warn(f"receive cap req msg from {msg.manager} msg:{msg.cap_req_list}")
            self.enter_task_exe_pub.publish(Bool(data=True))

    def distance_measure_cb(self,task_center_name)->float:
                self.peer_name=task_center_name
                self.transform = None
                self.peer_transform = None
                self.peer_pose_msg_sub = self.ros_node.create_subscription(
                tf2_msgs.msg.TFMessage,
                "/tf",#f"{task_center_name}/tf",   
                self.receive_peer_pose_msg_cb,
                qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=20,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
                )
                self.pose_msg_sub = self.ros_node.create_subscription(
                tf2_msgs.msg.TFMessage,
                "/tf",#f"{self.ros_node.get_namespace()}/tf",   
                self.receive_pose_msg_cb,
                qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=20,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
                )
                self.ros_node.get_logger().warn(f"Distance measure test 1")
                time_sum=0

                while self.transform is None or self.peer_transform is None:
                    time.sleep(0.1)
                    time_sum+=0.1
                    if(time_sum>5):
                        dist = 5 #原来是6
                        break

                self.ros_node.get_logger().warn(f"Distance measure test 2")
                
                if self.peer_transform is not None:
                    self.x = self.transform.transform.translation.x
                    self.y = self.transform.transform.translation.y
                    self.peer_x = self.peer_transform.transform.translation.x
                    self.peer_y = self.peer_transform.transform.translation.y
                

                    dx = self.x - self.peer_x
                    dy = self.y - self.peer_y
                    dist = math.hypot(dx, dy)
                    self.ros_node.get_logger().warn(f"Distance between robot {task_center_name} and {self.ros_node.get_namespace()}: {dist}")

                if dist <= 4.2: # 2.1   ### add 0311   ## 原来是7
            # 发布通信消息（示例）
                    return dist
                else:
                    return 10.0



    
    
    def _do_setup(self):    
        
        ### task以color+manager的形式分别发送，如果本身接收到manager不是自身的任务，接换成topic publisher发送与manager之间的距离
        
        self.receive_task_number = 0
        self.public_task_sub = self.ros_node.create_subscription(
            DistanceCalculation,  ## manager name + task name
            f"/global/public_task_publish",
            self.receive_public_task_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )
        self.task_center_name = None
        self.task_name = None

        self.task_list = ["red", "blue", "blue"]
        self.task_number = 0
        self.receive_result_owner = None
        self.receive_result_task = None
        self.task_exe_finish_sub = self.ros_node.create_subscription(
            DistanceCalculation,
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/task_exe_finish",
            self.receive_task_finish_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        self.task_file = self.options["file_path"]
        #self.target = self.inputs["target_color"]
        with open(self.options["file_path"]) as f:
            self.tasks = yaml.load(f, Loader=yaml.FullLoader)
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

        #set_param = self.ros_node.declare_parameter("origional_task_req",0)


        self.receive_task_list = []
        self.receive_task_time = []
        self.receive_task_center = []
        self.receive_task_sub = []
        self.cap_req_cap = []
        self.cap_req_center = []
        self.cap_req_depth = []
        self.cap_req_time = []

        self.best_cap_req_pub = self.ros_node.create_publisher(
                CapabilityRequest,
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/Coop/capability_request",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()
            )
        
        self.enter_task_exe_pub=self.ros_node.create_publisher(
                Bool,
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/ask_for_help",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()
            )

        self.__stop_move_pub = self.ros_node.create_publisher(
            Bool,
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/stop_move",   ### 不确定！！！！！ new manager还是manager
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )### 改了发停止信息的位置，效果待修改，原来是ask for help停下，才能进入input的判断过程

        if not self.ros_node.has_parameter("msg_num_count"):
            self.ros_node.declare_parameter("msg_num_count",0)
        self.msg_number = 0
        
    
    
    
    def _do_tick(self):     
        """Output the value of a symbol"""
        
        # if self.task_number==0 or self.receive_result_task == self.task_list[self.task_number-1]:
        #     self.task_name = self.task_list[self.task_number]
        #     self.outputs["task_name"] = self.task_list[self.task_number]

        #     self.ros_node.get_logger().info("Tick once")
        #     self.outputs["origional_cap_req"] = None
        #     self.ros_node.undeclare_parameter("origional_task_req")

        #     try:
        #         self.outputs["origional_cap_req"]=self.tasks[self.task_name]  ### list格式，还没有转换成CapReq格式
        #         self.origional_req = CapList(cap_list=self.outputs["origional_cap_req"])
        #         self.origional_req_pub.publish(CapabilityRequest(cap_req_list=[self.origional_req],manager=self.ros_node.get_namespace(),depth=0))
        #     except Exception as exc:
        #         pass
        #     self.ros_node.get_logger().info("Get Origional Requirement!")
        #     self.ros_node.declare_parameter("origional_task_req",self.outputs["origional_cap_req"])
        #     #self.ros_node.set_parameters([Parameter('origional_task_req',Parameter.Type.,False)]"origional_task_req",self.outputs["origional_cap_req"])

        #     self.task_number = self.task_number + 1
        #     if self.task_number < len(self.task_list):
        #         return NodeMsg.ASSIGNED
        #     else:
        #         return NodeMsg.SUCCEEDED
        # else:
        #     return NodeMsg.ASSIGNED








        sum = 0
        self.logwarn("Task Input ticking...")
        if not self.ros_node.has_parameter("origional_task_req"):
            set_param = self.ros_node.declare_parameter("origional_task_req",0)
        while self.receive_task_center == []:
            time.sleep(0.1)
            return NodeMsg.ASSIGNED
        # 收到的任务列表，包括不同的任务名称而非具体的协助请求
        # 任务列表一直在变，而且可能有已经被响应的合作任务，如何处理多余信息————接收到任务信息之后就直接订阅处理了，具体是否要接收协助由中心决定，等待时间也需要修改
        # 清空的过程中是否会错漏数据————不会错漏，智能体恢复需要反应时间，请求订阅一直保留，选择执行的是最先到来的请求
        # 如何进行循环，如果按照任务分别进行判断，积攒的任务请求如何处理————放在public_task_cb里，来一个任务信息开一个，tick部分只循环协助请求列表的判断
        while self.task_center_name == None and (sum < self.receive_task_number or sum == self.receive_task_number):  #):  ###  delete 0310 #############
            #self.ros_node.get_logger().warn(f"sum:{sum}, receive_task_number:{self.receive_task_number}")
            if sum < self.receive_task_number or sum == self.receive_task_number:
                for v in self.receive_task_list:
                    #self.ros_node.get_logger().warn(f"number {sum} cap list {self.cap_req_cap} task number {self.receive_task_number}")
            
                    if sum < self.receive_task_number:
                        self.ros_node.get_logger().warn(f"number {sum} task:{v}-{self.receive_task_center[sum]}")
        #     # 原则1：按照任务顺序依次订阅任务中心
        #     # 判断距离

        #     # 这一段挪到收到任务信息的cb里了
        #     self.ros_node.create_subscription(
        #         CapabilityRequest,
        #         f"{self.receive_task_center[sum]}/BehaviorTreeNode/capability_request",
        #         self.capability_request_cb,
        #         qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #         callback_group=ReentrantCallbackGroup()
        #     )

            
            # 原则2：在没有收到其他任务协助请求的情况下，如果有自己的任务直接进入自身任务招募执行
                    if sum < self.receive_task_number and self.cap_req_cap == []:
                        if self.receive_task_center[sum] == self.ros_node.get_namespace():
                            self.task_name = self.receive_task_list[sum]# v
                            self.task_center_name = self.ros_node.get_namespace()
                            sum = self.receive_task_number +1
                            self.outputs["best_req_msg"] = CapabilityRequest()
                            break

        # 新增！
        # while self.cap_req_cap == []:
            # 原则3：等待其他任务中心发布需求，进入协作执行阶段（可能出现并行）
                    elif not self.cap_req_cap == []:
                        num = 0
                        self.first_exe_symbol = False
                        self.first_exe_time = None
                        self.nearest_distance = None
                        self.ros_node.get_logger().warn(f"receive cap req:{self.cap_req_cap}")
                        self.ros_node.get_logger().warn(f"receive cap req:{self.cap_req_center}")
                        for n in self.cap_req_cap:
                            self.ros_node.get_logger().warn(f"receive cap req number:{len(self.cap_req_cap)}")
                            self.distance_measure_result = self.distance_measure_cb(self.cap_req_center[num])
                            self.ros_node.get_logger().warn(f"Distance Calculation result for cap req{num} :{self.distance_measure_result} !")
                            self.ros_node.get_logger().warn(f"new req time:{self.cap_req_time[num]}")
                            # 记录第一个可执行协作
                            if num < len(self.cap_req_cap) and self.first_exe_symbol == False:
                                # 自己的直接去执行
                                # 只可能出现两种情况，1.在订阅当前所收到的所有任务过程中，收到自身可执行的任务  2.
                                if sum < self.receive_task_number and self.receive_task_center[sum]==self.ros_node.get_namespace():#self.cap_req_center[num]==self.ros_node.get_namespace():
                                    self.task_name =self.receive_task_list[sum]# v#self.cap_req_list = n
                                    self.task_center_name = self.ros_node.get_namespace()
                                    self.outputs["best_req_msg"] = CapabilityRequest()
                                    break
                                # 别人的稍等判断一下是否有并行请求
                                elif self.distance_measure_result<4.2 or self.distance_measure_result==4.2:  ## 原来是6.5
                                    self.ros_node.get_logger().warn(f"First time find task to exe !")
                                    self.msg_number = self.msg_number+1
                                    self.ros_node.get_logger().warn(f"msg num count +1, req count +1")
                                    self.__stop_move_pub.publish(Bool(data=True))
                                    ### change 1111

                                    self.first_exe_symbol = True
                                    self.nearest_distance = self.distance_measure_result
                                    num_record = num
                                    self.first_exe_time = self.cap_req_time[num]
                                    self.cap_req_list = n
                                    self.cap_depth = self.cap_req_depth[num]
                                    self.task_center_name = self.cap_req_center[num]
                                    num = num + 1
                                else:
                                    num = num + 1
                            # 出现并行的协作请求
                            elif self.cap_req_time[num].sec == self.first_exe_time.sec and self.cap_req_time[num].nanosec -self.first_exe_time.nanosec < 500000000:
                                self.__stop_move_pub.publish(Bool(data=True))
                                ### change 1111

                                # 其中有自己的任务，直接执行自己的任务
                                if self.cap_req_center[num] == self.ros_node.get_namespace():
                                    self.cap_req_list = n
                                    self.task_center_name = self.ros_node.get_namespace()
                                    sum = self.receive_task_number +1
                                    self.first_exe_symbol = False
                                    self.outputs["best_req_msg"] = CapabilityRequest()
                                    break
                                # 有并行的其他协作请求，找到与第一个请求并行的所有请求，比较距离和匹配程度，更新最佳可执行协作（按距离）
                                else:
                                    self.msg_number = self.msg_number+1
                                    self.ros_node.get_logger().warn(f"Find parallel cap req, distance:{self.distance_measure_result},nearest distance:{self.nearest_distance}")
                                    if self.distance_measure_result<self.nearest_distance:
                                        num_record = num
                                        self.nearest_distance = self.distance_measure_result
                                        self.first_exe_time = self.cap_req_time[num]
                                    num = num + 1 
                            # 可执行已记录完毕，并行协作请求处理完毕
                            else:
                                if num == len(self.cap_req_cap) and self.task_name == None:
                                    self.cap_req_cap = []
                                    return NodeMsg.FAILED
                                
                                # self.task_name = v
                                # self.task_center_name = self.receive_task_center[sum]
                                break

                            ### new add 0308
                            # if num == len(self.cap_req_cap) and self.task_name == None:
                            #         self.cap_req_cap = []
                            #         return NodeMsg.FAILED
                                
                                
                        self.ros_node.get_logger().warn(f"receive cap req:{self.cap_req_cap},num:{num}")
                        self.ros_node.get_logger().warn(f"receive req msg num: {self.msg_number}")
                            #num = num + 1
                        if self.first_exe_symbol==True and (num==len(self.cap_req_cap) or num>len(self.cap_req_cap)):
                            self.ros_node.get_logger().warn(f"Find task to exe !")
                            self.first_exe_time = self.cap_req_time[num_record]
                            self.cap_req_list = self.cap_req_cap[num_record]#CapList(cap_list=self.cap_req_cap[num_record])
                            self.cap_depth = self.cap_req_depth[num_record]
                            self.task_center_name = self.cap_req_center[num_record]
                            self.task_name = self.receive_task_list[self.receive_task_center.index(self.task_center_name)]
                            sum = self.receive_task_number + 1
                            self.outputs["best_req_msg"] = CapabilityRequest(depth=self.cap_depth,cap_req_list=[self.cap_req_list],manager=self.task_center_name)
                            break


                        ### add 0311
                        
                        if self.first_exe_symbol==False:
                            self.cap_req_cap = []
                            return NodeMsg.ASSIGNED#NodeMsg.FAILED
                        

                    
                        

                        

                    if sum < self.receive_task_number or sum == self.receive_task_number:
                        sum = sum+1
                        self.ros_node.get_logger().warn(f"Recheck cap req list(number{sum}task)...")
                        time.sleep(0.1)
                    ### new add 0308
                    # if sum == self.receive_task_number:
                    #     sum = sum+1
                    #     break

            else:
                time.sleep(0.1)








        


        
        ############ new public cap receive method
        
        ### delete 0310 ##########################################
        # while self.task_name is None:
        #     time.sleep(0.5)
        ### add 0310 ###############################
        if self.task_name is None:
            self.cap_req_cap = []
            ## add 0427
            self.receive_task_list = []
            self.receive_task_time = []
            self.receive_task_center = []
            self.receive_task_sub = []
            self.receive_task_number = 0
            self.cap_req_center = []
            self.cap_req_depth = []
            self.cap_req_time = []

            return NodeMsg.ASSIGNED#SUCCEEDED   ### change 0427
        ##############################################

        self.ros_node.get_logger().info(f"public task: {self.task_center_name}-{self.task_name}")
        self.outputs["task_name"] = self.task_name
        
        if self.task_number==0 and self.task_center_name == self.ros_node.get_namespace():
            

            self.ros_node.get_logger().info("Tick once")
            self.outputs["origional_cap_req"] = None
            self.ros_node.undeclare_parameter("origional_task_req")

            try:
                self.outputs["origional_cap_req"]=self.tasks[self.task_name]  ### list格式，还没有转换成CapReq格式
                self.origional_req = CapList(cap_list=self.outputs["origional_cap_req"])
                self.origional_req_pub.publish(CapabilityRequest(cap_req_list=[self.origional_req],manager=self.ros_node.get_namespace(),depth=0))
            except Exception as exc:
                pass
            self.ros_node.get_logger().info("Get Origional Requirement!")
            self.ros_node.declare_parameter("origional_task_req",self.outputs["origional_cap_req"])
            #self.ros_node.set_parameters([Parameter('origional_task_req',Parameter.Type.,False)]"origional_task_req",self.outputs["origional_cap_req"])

            self.task_number = 1
            
            return NodeMsg.ASSIGNED
            
        else:
            if self.receive_result_owner == self.ros_node.get_namespace() and self.receive_result_task == self.task_name:
                self.receive_result_owner = None
                self.receive_result_task = None
                return NodeMsg.SUCCEEDED
            if self.task_number == 1:
                return NodeMsg.ASSIGNED
            if not self.task_center_name == self.ros_node.get_namespace():
                





                

        
        
        
            #     self.transform = None
            #     self.peer_transform = None
            #     self.peer_pose_msg_sub = self.ros_node.create_subscription(
            #     tf2_msgs.msg.TFMessage,
            #     f"{self.task_center_name}/tf",   
            #     self.receive_peer_pose_msg_cb,
            #     qos_profile=QoSProfile(
            #         reliability=QoSReliabilityPolicy.RELIABLE,
            #         durability=QoSDurabilityPolicy.VOLATILE,
            #         history=QoSHistoryPolicy.KEEP_LAST,
            #         depth=20,),
            #     callback_group=ReentrantCallbackGroup()    ### change 0701
            #     )
            #     self.pose_msg_sub = self.ros_node.create_subscription(
            #     tf2_msgs.msg.TFMessage,
            #     f"{self.ros_node.get_namespace()}/tf",   
            #     self.receive_pose_msg_cb,
            #     qos_profile=QoSProfile(
            #         reliability=QoSReliabilityPolicy.RELIABLE,
            #         durability=QoSDurabilityPolicy.VOLATILE,
            #         history=QoSHistoryPolicy.KEEP_LAST,
            #         depth=20,),
            #     callback_group=ReentrantCallbackGroup()    ### change 0701
            #     )

            #     while self.transform is None or self.peer_transform is None:
            #         time.sleep(0.1)
                
            #     self.x = self.transform.transform.translation.x
            #     self.y = self.transform.transform.translation.y
            #     self.peer_x = self.peer_transform.transform.translation.x
            #     self.peer_y = self.peer_transform.transform.translation.y
            

            #     dx = self.x - self.peer_x
            #     dy = self.y - self.peer_y
            #     dist = math.hypot(dx, dy)
            #     self.ros_node.get_logger().warn(f"Distance between robot {self.task_center_name} and {self.ros_node.get_namespace()}: {dist}")

            #     if dist <= 2.5:
            # # 发布通信消息（示例）






                    self.distance_measure = self.ros_node.create_publisher(
                    DistanceCalculation,
                    f"{self.ros_node.get_namespace()}/assignment_manager/calculate_distance",  ## 待修改 {self.task_center_name}
                    qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=10,),
                    callback_group=ReentrantCallbackGroup()
                )
                    self.ros_node.get_logger().warn(f"Measuring distance between self and task center...")
                    self.distance_measure.publish(DistanceCalculation(own_name=self.ros_node.get_namespace(),other_name=self.task_center_name))
                
                
                
                    self.best_cap_req_pub.publish(CapabilityRequest(cap_req_list=[self.cap_req_list],depth=self.cap_depth,manager=self.task_center_name))
                
                
                
                
                
                ## 待修改
                    return NodeMsg.SUCCEEDED
                # else:
                #     return NodeMsg.FAILED
                

        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        # for n in self.receive_task_sub:
        #     self.ros_node.destroy_subscription(n)
        num_count = self.ros_node.get_parameter("msg_num_count").value
        self.ros_node.set_parameters([Parameter("msg_num_count",Parameter.Type.INTEGER,num_count+self.msg_number)])
        self.ros_node.get_logger().warn(f"Task Input Node Reset! current msg number: {num_count+self.msg_number}")
        self.msg_number = 0
        # self.distance_measure.destroy()
        # self.origional_req_pub.destroy()
        # self.best_cap_req_pub.destroy()
        # self.enter_task_exe_pub.destroy()
        self.task_number = 0
        self.task_name = None
        self.task_center_name = None
        self.receive_result_owner = None
        self.receive_result_task = None
        self.ros_node.undeclare_parameter("origional_task_req")
        self.receive_task_list = []
        self.receive_task_time = []
        self.receive_task_center = []
        self.receive_task_number = 0
        self.cap_req_cap = []
        self.cap_req_center = []
        self.cap_req_depth = []
        self.cap_req_time = []
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE





@define_bt_node(
    NodeConfig(
        options={"file_path":str},#"symbol_name":str,
        inputs={"target_color":str},
        outputs={"origional_cap_req":list},
        max_children=0,
        ))
class GetTaskList(Leaf):
    """Check the state of a symbol"""

    def _do_setup(self):    
        
        self.task_file = self.options["file_path"]
        #self.target = self.inputs["target_color"]
        with open(self.options["file_path"]) as f:
            self.tasks = yaml.load(f, Loader=yaml.FullLoader)
        # self.origional_req_pub = self.ros_node.create_publisher(
        #         CapabilityRequest, #list 和 传递次数 发送人名称
        #         f"{self.ros_node.get_namespace()}/BehaviorTreeNode/origional_capability_request",
        #         qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #         callback_group=ReentrantCallbackGroup()    ### change 0701
        # )

        
    def _do_tick(self):     
        """Output the value of a symbol"""
        self.ros_node.get_logger().info("Tick once")
        self.outputs["origional_cap_req"] = None

        try:
            self.outputs["origional_cap_req"]=self.tasks[self.inputs["target_color"]]  ### list格式，还没有转换成CapReq格式
            self.origional_req = CapList(cap_list=self.outputs["origional_cap_req"])
            self.origional_req_pub.publish(CapabilityRequest(cap_req_list=[self.origional_req],manager=self.ros_node.get_namespace(),depth=0))
        except Exception as exc:
            pass
        self.ros_node.get_logger().info("Get Origional Requirement!")
        set_param = self.ros_node.declare_parameter("origional_task_req",self.outputs["origional_cap_req"])
        if self.outputs["origional_cap_req"] is not None:
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        self.ros_node.undeclare_parameter("origional_task_req")
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE






@define_bt_node(
    NodeConfig(
        options={},
        inputs={},
        outputs={},#"param_value":OptionRef("param_type")
        max_children=0,
    ))
class ResetSymbol(Leaf):##Leaf？
    """Gets a location name from the queue"""


    def _do_setup(self):#setup(self):
        if not self.ros_node.has_parameter('assigned_symbol'):
            self.assigned_param = self.ros_node.declare_parameter('assigned_symbol',False)
        else:
            self.assigned_param = self.ros_node.set_parameters([Parameter('assigned_symbol',Parameter.Type.BOOL,False)])
        if not self.ros_node.has_parameter('received_symbol'):
            self.received_param = self.ros_node.declare_parameter('received_symbol',False)
        else:
            self.received_param = self.ros_node.set_parameters([Parameter('received_symbol',Parameter.Type.BOOL,False)])

        
        
    def _do_tick(self):##tick(self):      #update(self):    
        
        """Checks for the status of the navigation action"""
        # If there is a result, we can check the status of the action directly.
        # Otherwise, the action is still running.
        self.if_pub_success=self.ros_node.get_parameter('assigned_symbol')

        if self.if_pub_success is not None: #target_color is not None:
            
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        self.ros_node.set_parameters([Parameter('assigned_symbol',Parameter.Type.BOOL,False)])
        self.ros_node.set_parameters([Parameter('received_symbol',Parameter.Type.BOOL,False)])
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE





@define_bt_node(
    NodeConfig(
        options={"symbol_name":str,"symbol_type":type},
        inputs={},
        outputs={"symbol_value":OptionRef("symbol_type")},
        max_children=0,
        ))
class CheckSymbol(Leaf):
    """Check the state of a symbol"""


    @abstractmethod    
    def assignment_symbol_cb(self,msg):
        if msg.own_name is not None:
            self.assignment_symbol = True

    def _do_setup(self):    
        
        
        
        self.__distance_limit_calculate_sub = self.ros_node.create_subscription(
            DistanceCalculation,
            f"{self.ros_node.get_namespace()}/assignment_manager/calculate_distance",
            self.assignment_symbol_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            #latch=False,
            #queue_size=1,
        )
        
        
        self.symbol_param =self.ros_node.get_parameter(self.options["symbol_name"]).value

        

        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        
        self.outputs["symbol_value"]=self.symbol_param
        if self.symbol_param is not None:
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
        options={"symbol_type":type},#"symbol_name":str,
        inputs={},
        outputs={"symbol_value":OptionRef("symbol_type"),"feedback_name":str},
        max_children=0,
        ))
class CheckAssignedSymbol(Leaf):
    """Check the state of a symbol"""


    def assignment_symbol_cb(self,msg):
        if msg.own_name is not None:
            self.assigned_symbol = True
            self.own_name = msg.own_name
            self.other_name = msg.other_name
            self.ros_node.get_logger().warn(f"receive manager {self.other_name} msg!")


    # 用不上了
    # def capability_request_cb(self,msg):
    #     if msg is not None:
    #         self.depth = msg.depth
    #         self.cap_req = msg.cap_req_list
    #         ########################################## 注意
    #         self.cap_req = self.cap_req[0]
    #         self.ros_node.get_logger().warn(f"receive cap req msg!")



    def _do_setup(self):    
        
        #self.symbol_param =self.ros_node.get_parameter(self.options["symbol_name"]).value

        
        self.assigned_symbol = False
        
        self.other_name = None

        self.__distance_limit_calculate_sub = self.ros_node.create_subscription(
            DistanceCalculation,
            f"{self.ros_node.get_namespace()}/assignment_manager/calculate_distance",
            self.assignment_symbol_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )
        
        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        ###########################################################
        #进入的条件要改，需要接收到req之后才能进入assigned
        ###########################################################

        wait_time_sum = 0
        while self.other_name is None and self.ros_node.get_parameter("origional_task_req").value==0:
            time.sleep(0.2)
            wait_time_sum = wait_time_sum + 0.2
            self.ros_node.get_logger().warn("wait for req msg!")
            if wait_time_sum > 1.5:#5
                self.outputs["symbol_value"]=False
                self.outputs["feedback_name"]=self.ros_node.get_namespace()
                return NodeMsg.RUNNING
        

        # 用不上了
        # self.receive_cap_req_sub = self.ros_node.create_subscription(
        #     CapabilityRequest,
        #     f"{self.other_name}/BehaviorTreeNode/capability_request",
        #     self.capability_request_cb,
        #     qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #     callback_group=ReentrantCallbackGroup()    ### change 0701
        # )
        ####### 怎么开启多个订阅，且只当第一个msg receive的时候接受信息并停止其他接收
        # while self.other_name is None:
        #     time.sleep(0.5)
        #     self.ros_node.get_logger().warn("wait for req msg!")
        # set_param = self.ros_node.set_parameters([Parameter('assigned_symbol',Parameter.Type.BOOL,True)])






        
        # if self.ros_node.has_parameter("origional_task_req"):
        #     self.param2 = self.ros_node.get_parameter("origional_task_req")
        # else:
        #     self.param2 = None
        self.param2 = self.ros_node.get_parameter("origional_task_req").value
        self.param1 = self.ros_node.get_parameter("assigned_symbol")
        #【1】两种开启方式，一种获取距离位置信息，一种获取task list的原始cap req
        if not self.param2==0:
            set_param = self.ros_node.set_parameters([Parameter('assigned_symbol',Parameter.Type.BOOL,True)])
            self.assigned_symbol = True
            self.outputs["feedback_name"] = self.ros_node.get_namespace()
        else:
            set_param = self.ros_node.set_parameters([Parameter('assigned_symbol',Parameter.Type.BOOL,self.assigned_symbol)])
            self.outputs["feedback_name"]=self.other_name
        
        self.ros_node.get_logger().info("Update assigned symbol!")
            
        self.outputs["symbol_value"]=self.assigned_symbol
        
        if self.outputs["symbol_value"]: # is not None:  ### 同RUNNING一起修改
            #self.own_name == self.ros_node.get_namespace():
            ##################################### 可以改进一下，原来的判断条件也很好
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.RUNNING # FAILED        
        ##########################################################################
        ### 需要有一个running状态，此处改为running？
        ##########################################################################                                       

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        self.assigned_symbol = False
        self.outputs["symbol_value"] = False
        self.other_name = None
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE




@define_bt_node(
    NodeConfig(
        options={},
        inputs={"own_cap_list":list, "manager_name":str, "best_req_msg":CapabilityRequest},
        outputs={"find_all_symbol":bool, "new_cap_request":CapabilityRequest, "cap_record":list, "depth_record":int,"new_hierarchical_task":DistanceCalculation},
        max_children=0,
        ))
class CalculateCapMatch(Leaf):
    """Check the state of a symbol"""


    def capability_request_cb(self,msg):
        # number = 0
        # if msg is not None:
        #     for numbers in msg.cap_list:
        #         ### 迭代形成矩阵
        #         number = number+1
        # self.ros_node.get_logger().info("Update assigned symbol!")
        if msg is not None:
            self.depth = msg.depth + 1
            self.cap_req = msg.cap_req_list
            ########################################## 注意
            self.cap_req = self.cap_req[0].cap_list
            self.ros_node.get_logger().warn(f"receive cap req msg!")

    def _do_setup(self):    
        
        self.manager = self.inputs["manager_name"]
        #if self.manager is not None:
        #### 当input获取放在do_setup中时，获取到的input是空值，不能正确获取manager信息进入下面的选择
        

        
        
        
    def _do_tick(self):     
        """Output the value of a symbol"""

        self.manager = self.inputs["manager_name"]
        self.depth = None
        #if self.manager is not None:                ########################## 不确定
        if not self.manager == self.ros_node.get_namespace():


            ## 在任务筛选之前负责接受任务请求信息的
            # self.capability_request_sub = self.ros_node.create_subscription(
            #     CapabilityRequest, #list 和 传递次数 发送人名称
            #     f"{self.manager}/BehaviorTreeNode/Coop/capability_request",#f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_request",
            #     self.capability_request_cb,
            #     qos_profile=QoSProfile(
            #     reliability=QoSReliabilityPolicy.RELIABLE,
            #     durability=QoSDurabilityPolicy.VOLATILE,
            #     history=QoSHistoryPolicy.KEEP_LAST,
            #     depth=10,),
            #     callback_group=ReentrantCallbackGroup()    ### change 0701
            #     )

            self.capability_request_cb(self.inputs["best_req_msg"])


            self.ros_node.get_logger().warn(f"Subscribe Cap Req Manager {self.manager}!")
            while self.depth is None:
                time.sleep(0.1)
                self.ros_node.get_logger().warn("wait for req msg!")
        else:   ########################## 不确定
            self.capability_request_sub = self.ros_node.create_subscription(
                CapabilityRequest, #list 和 传递次数 发送人名称
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/origional_capability_request",#f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_request",
                self.capability_request_cb,
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()    ### change 0701
                )
            self.ros_node.get_logger().warn("Subscribe Cap Req Itself!")
            self.origional_cap_req = self.ros_node.get_parameter("origional_task_req").value
            self.depth = 0
            self.cap_req = self.origional_cap_req
            self.ros_node.get_logger().warn("Get Origional Cap Require!")
        #else:
        #    self.ros_node.get_logger().warn("Did not find any proper manager!")
        #    return NodeMsg.FAILED


        self.own_cap_list = self.inputs["own_cap_list"]
        self.sum: int = 0
        self.sum_v:int = 0
        self.new_req:list=[0,0,0]    ### 注意修改
        self.record:list = []
        #self.cap_record = {}  #list形式传到feedback部分再整合
        if self.depth == 0:
            self.center_symbol = self.ros_node.declare_parameter("center_symbol", True)
        else:
            self.center_symbol = self.ros_node.declare_parameter("center_symbol", False)

        self.ros_node.get_logger().warn(f"own_cap_list:{len(self.own_cap_list)}, cap_req:{len(self.cap_req)}")

        for v in range(len(self.own_cap_list)):
            if self.own_cap_list[v]*self.cap_req[v]==0:
                self.sum = self.sum+0
                self.new_req[v]=self.cap_req[v]
               
                ## record更新-nsp+能力名称
            if self.own_cap_list[v]*self.cap_req[v]>0:
                self.sum = self.sum+1
                self.new_req[v]=self.cap_req[v]-self.own_cap_list[v]
                self.record.append(v+1)  ### 能力序号列表
                
            self.sum_v=self.sum_v+self.new_req[v]
        #self.cap_record.pop(self.ros_node.get_namespace(),self.record)## record更新-nsp+能力名称



        #self.outputs["symbol_value"]=self.symbol_param
        if self.sum == 0:
            set_param = self.ros_node.set_parameters([Parameter('assigned_symbol', Parameter.Type.BOOL, False)])
            return NodeMsg.FAILED
        else:
            if self.sum_v == 0:
                self.outputs["find_all_symbol"]=True
                declare_param = self.ros_node.declare_parameter("find_all_symbol",True)
                self.ros_node.get_logger().warn(f"find all req!")
                self.outputs["new_cap_request"]=None
                self.outputs["new_hierarchical_task"]=None
                self.outputs["cap_record"]=self.record
                self.outputs["depth_record"]=self.depth
            else:
                self.outputs["find_all_symbol"]=False
                declare_param = self.ros_node.declare_parameter("find_all_symbol",False)
                self.ros_node.get_logger().warn(f"new req:{self.new_req}")
                self.outputs["new_cap_request"]=CapabilityRequest(
                    cap_req_list=[CapList(cap_list=self.new_req)], 
                    manager=self.ros_node.get_namespace(), 
                    depth=self.depth) # +1)
                if self.depth>0:
                    self.outputs["new_hierarchical_task"]=DistanceCalculation(
                        own_name=self.ros_node.get_namespace(),
                        other_name=f"new_task_{self.depth}")
                else:
                    self.outputs["new_hierarchical_task"]=DistanceCalculation()
                self.outputs["cap_record"]=self.record
                self.outputs["depth_record"]=self.depth
            return NodeMsg.SUCCEEDED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        self.capability_request_sub = None
        self.center_symbol = None
        if self.ros_node.has_parameter("center_symbol"):
            self.ros_node.undeclare_parameter("center_symbol")
        if self.ros_node.has_parameter("find_all_symbol"):
            self.ros_node.undeclare_parameter("find_all_symbol")
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    





@define_bt_node(
    NodeConfig(
        options={"symbol_type":type},#"symbol_name":str,
        inputs={"depth":int,"manager":str},
        outputs={"symbol_value":OptionRef("symbol_type"),"feedback_msg":CapabilityFeedback},
        max_children=0,
        ))
class CheckReceivedSymbol(Leaf):
    """Check the state of a symbol"""


    def received_symbol_cb(self,msg):
        #if not msg.depth == 0:   ###########################depth=0应该不会自身循环调用
            self.feedback_msg = msg
            self.ros_node.get_logger().warn(f"feedback msg:{self.feedback_msg}")

    def _do_setup(self):    
        
        #self.symbol_param =self.ros_node.get_parameter(self.options["symbol_name"]).value
        self.received_symbol = False
        self.feedback_msg = None

        


        self.__capability_list_feedback_sub = self.ros_node.create_subscription(
            CapabilityFeedback,
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/capability_feedback",
            self.received_symbol_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        
        manager_name = self.inputs["manager"]
        self.__capability_list_feedback_pub = self.ros_node.create_publisher(
            CapabilityFeedback,
            f"{manager_name}/BehaviorTreeNode/capability_feedback",
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )



        waitiong_time_sum = 0
        while self.feedback_msg is None:
            if self.feedback_msg is not None or self.ros_node.get_parameter('find_all_symbol').value==True:
                self.received_symbol = True
                set_param = self.ros_node.set_parameters([Parameter('received_symbol',Parameter.Type.BOOL,self.received_symbol)])
                set_param = self.ros_node.set_parameters([Parameter('find_all_symbol',Parameter.Type.BOOL,self.received_symbol)])
                if self.feedback_msg is None:
                    self.feedback_msg = CapabilityFeedback(
                    cap_list_record=[],
                    cap_owner_record=[],
                    manager_name=self.inputs["manager"], # ros_node.get_namespace(),   ################## 不确定
                    depth=self.inputs["depth"])
                break
            else:
                time.sleep(0.1)
                self.ros_node.get_logger().warn("waiting for feedback msg...")
                waitiong_time_sum = waitiong_time_sum+0.1
                if waitiong_time_sum > 7:
                    self.ros_node.get_logger().warn("Could not find proper team member!")
                    return NodeMsg.FAILED

            
        self.ros_node.get_logger().info("Update received symbol!")
        self.received_symbol = True
        set_param = self.ros_node.set_parameters([Parameter('received_symbol',Parameter.Type.BOOL,self.received_symbol)])
        set_param = self.ros_node.set_parameters([Parameter('find_all_symbol',Parameter.Type.BOOL,self.received_symbol)])
        #self.outputs["symbol_value"]=self.symbol_param

        self.outputs["symbol_value"]=self.received_symbol
        self.outputs["feedback_msg"]=self.feedback_msg
        if self.ros_node.get_parameter('find_all_symbol').value==True:
            self.outputs["feedback_msg"] = CapabilityFeedback(
                cap_list_record=self.feedback_msg.cap_list_record,
                cap_owner_record=self.feedback_msg.cap_owner_record,
                manager_name=self.inputs["manager"], # self.ros_node.get_namespace(),    ################## 不确定
                depth=self.inputs["depth"])
        #self.__capability_list_feedback_sub.destroy() ########## 不确定
        if self.received_symbol:
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        self.feedback_msg = None
        #self.__capability_list_feedback_pub.destroy()
        self.received_symbol = False
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE





@define_bt_node(
    NodeConfig(
        options={},
        inputs={"feedback_msg":CapabilityFeedback, "feedback":list, "manager":str},
        outputs={"team_list":list, "team_cap_list":list},
        max_children=0,
        ))
class CapabilityListRecord(Leaf):
    """Check the state of a symbol"""


    def _do_setup(self):    
        
        self.msg = self.inputs["feedback_msg"]
        self.msg_number = 0
        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        self.msg = self.inputs["feedback_msg"]
        self.ros_node.get_logger().warn(f"receive feedback_msg:{self.msg}")
        self.feedback_add_list = self.inputs["feedback"]
        self.new_manager_name = self.inputs["manager"]
        self.cap_list_record:list = self.msg.cap_list_record
        self.cap_owner_record:list = self.msg.cap_owner_record
        self.manager_name = self.msg.manager_name
        self.depth = self.msg.depth

        self.__capability_list_feedback_pub = self.ros_node.create_publisher(
            CapabilityFeedback,
            f"{self.new_manager_name}/BehaviorTreeNode/capability_feedback",
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        if self.depth == 0:
            self.__team_join_pub = self.ros_node.create_publisher(
                JoinTeam,
                f"{self.ros_node.get_namespace()}/BehaviorTreeNode/join_team",
                qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
                callback_group=ReentrantCallbackGroup()
            )

        self.cap_list_record.append(CapList(cap_list=self.feedback_add_list))
        self.new_cap_list_record = self.cap_list_record
        self.cap_owner_record.append(self.ros_node.get_namespace())
        self.new_cap_owner_record = self.cap_owner_record
        if self.depth > 0 : # and self.manager_name == self.ros_node.get_namespace():   ################## 不确定  
            self.__capability_list_feedback_pub.publish(
                CapabilityFeedback(
                    cap_list_record=self.new_cap_list_record,
                    cap_owner_record=self.new_cap_owner_record,
                    manager_name=self.new_manager_name,
                    depth=self.depth-1))
            self.ros_node.get_logger().info(f"Pass New Cap Feedback for {self.new_cap_owner_record} : {self.new_cap_list_record}! Feedback msg num: 1")
            self.outputs["team_list"] = self.new_cap_owner_record # []  ################## 不确定 
            self.msg_number = self.ros_node.get_parameter("msg_num_count").value
            self.ros_node.get_logger().info(f"Pass Feedback msg num: {self.msg_number}")
            self.ros_node.set_parameters([Parameter("msg_num_count",Parameter.Type.INTEGER,1+self.msg_number)])
            ### add 0311 不加的话new_cap_list_record中的元素都是CapList信息的格式，无法在GetCapList节点中测量其长度
            self.cap_list=[]
            for n in self.new_cap_list_record:
                self.cap_list.append(n.cap_list.tolist())


            self.outputs["team_cap_list"]=self.cap_list#self.new_cap_list_record
        else:
            if self.ros_node.get_parameter("center_symbol").value == True and self.new_manager_name == self.ros_node.get_namespace():
                ### 是否是new_manager_name不确定
                self.team_list: list=[]
                # for v in self.cap_list_record:
                #     self.team_list.append(v.key)
                ####################################################### 不确定
                self.team_list = self.new_cap_owner_record
                 
                self.ros_node.get_logger().warn(f"team member record: {self.team_list} ; team cap record: {self.new_cap_list_record}")
                self.outputs["team_list"] = self.team_list


                self.team_cap_record = []
                for v in self.new_cap_list_record:
                    self.team_cap_record.append(v.cap_list.tolist())
                self.outputs["team_cap_list"]=self.team_cap_record
                #self.ros_node.declare_parameter("team_cap_record",self.team_cap_record)
                self.ros_node.declare_parameter("team_member_record",self.new_cap_owner_record)
                #self.__team_join_pub.publish(JoinTeam(center_name=self.ros_node.get_namespace(),member_list=self.team_list, depth=0))
        #self.outputs["symbol_value"]=self.symbol_param

        #self.__capability_list_feedback_pub.destroy()########## 不确定
        if self.new_cap_list_record is not None:
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.FAILED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        if self.ros_node.has_parameter("team_member_record"):
            self.ros_node.undeclare_parameter("team_member_record")
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    




@define_bt_node(
    NodeConfig(
        options={},
        inputs={"manager":str, "team_list":list},
        outputs={"symbol_value":bool},
        max_children=0,
        ))
class JoinTeamReceiver(Leaf):
    """Check the state of a symbol"""


    def receive_join_team_msg(self,msg):
        if msg is not None:
            self.team_list = msg.member_list
            self.center_name = msg.center_name
            self.depth = msg.depth
        else:
            pass

    def _do_setup(self):    
        
        
        
        #if self.depth >0:
        self.__join_team_pub = self.ros_node.create_publisher(
            JoinTeam,
            f"{self.ros_node.get_namespace()}/BehaviorTreeNode/join_team",   ### 不确定！！！！！ new manager还是manager
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )
        # self.__stop_move_pub = self.ros_node.create_publisher(
        #     Bool,
        #     f"{self.ros_node.get_namespace()}/BehaviorTreeNode/stop_move",   ### 不确定！！！！！ new manager还是manager
        #     qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.VOLATILE,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,),
        #     callback_group=ReentrantCallbackGroup()    ### change 0701
        # )
        set_param = self.ros_node.declare_parameter("join_symbol",False)
        self.msg_number = 0
    

        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        self.manager_name = self.inputs["manager"]
        self.depth = None
        self.center_name = None


        self.__join_team_sub = self.ros_node.create_subscription(
            JoinTeam,
            f"{self.manager_name}/BehaviorTreeNode/join_team",   ### 不确定！！！！！ new manager还是manager
            self.receive_join_team_msg,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )

        if self.ros_node.get_parameter("center_symbol").value==True:
            self.depth = 0
            self.team_list = self.inputs["team_list"]
            self.__join_team_pub.publish(JoinTeam(center_name=self.ros_node.get_namespace(),member_list=self.team_list, depth=0))
            self.msg_number = len(self.team_list)-1
            self.ros_node.get_logger().warn(f"Center Send the Join Team Msg!Send msg num: {self.msg_number}")

            
        
        try:
            waitiong_time_sum = 0
            while self.center_name is None:
                time.sleep(0.1)
                waitiong_time_sum = waitiong_time_sum+0.1
                self.ros_node.get_logger().warn("waiting for join team msg...")
                if waitiong_time_sum > 5:
                    self.ros_node.get_logger().warn("Got an another better team!")
                    return NodeMsg.FAILED
            if self.depth is not None:
            
                self.__join_team_pub.publish(
                JoinTeam(
                    member_list=self.team_list,
                    center_name=self.center_name,
                    depth=self.depth+1))
            self.ros_node.get_logger().info(f"Pass Join Team Message! Team: {self.team_list}")
            ### add 0311
            for n in self.team_list:
                if n == self.ros_node.get_namespace():
                    self.outputs["symbol_value"]=True
                    set_param = self.ros_node.set_parameters([Parameter('join_symbol',Parameter.Type.BOOL,True)])                    
                    #declare_parameter("join_symbol",True)

                    # self.__stop_move_pub.publish(
                    #     Bool(data=True)
                    # )
                    self.pre_msg_num = self.ros_node.get_parameter("msg_num_count").value
                    self.ros_node.set_parameters([Parameter("msg_num_count",Parameter.Type.INTEGER,self.msg_number+self.pre_msg_num)])

                    #self.__join_team_pub.destroy()########## 不确定
                    return NodeMsg.SUCCEEDED
            self.outputs["symbol_value"]=False
            return NodeMsg.FAILED
        except BehaviorTreeException as exc:
            return NodeMsg.FAILED
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        if self.ros_node.has_parameter("join_symbol"):
            self.ros_node.set_parameters([Parameter('join_symbol',Parameter.Type.BOOL,False)])
        #    self.ros_node.undeclare_parameter("join_symbol")
        
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    



@define_bt_node(NodeConfig(
    options={"distance_limit":int},
    inputs={},  
    outputs={},
    max_children=0,
    ))
class GetDistance(Leaf):##Leaf？
    """Gets a location name from the queue"""

   
    def _do_setup(self):
        
        
        self.distance_limit = self.options["distance_limit"]
        self.peer_name = self.inputs["task_center_name"]

        # 订阅对方位置
        self.ros_node.create_subscription(
            PoseStamped, f'/{self.peer_name}/tf', self.peer_pose_cb, 10)

        # 订阅自己位置（也可用 tf，这里简单处理）
        self.ros_node.create_subscription(
            PoseStamped, f'/{self.ros_node.get_namespace()}/tf', self.own_pose_cb, 10)
        
        # 订阅对方位置
        

        # 通信话题（距离内才发布）
        self.in_range = self.ros_node.create_publisher(Bool, f'/{self.ros_node.get_namespace()}/to_{self.peer_name}', 10)

        self.own_pos = None
        self.peer_pos = None
        self.timer = self.ros_node.create_timer(0.1, self.check_distance)

    @abstractmethod
    def own_pose_cb(self, msg):
        self.own_pos = msg.pose.position

    @abstractmethod
    def peer_pose_cb(self, msg):
        self.peer_pos = msg.pose.position

    @abstractmethod
    def check_distance(self):




        try:
            transform = Buffer.lookup_transform(#tf_buffer.lookup_transform(
            'map',          # 参考坐标系
            'base_link',    # 机器人本体坐标系
            self.ros_node.get_clock().now()#time.Time()  # 获取最新变换
        )
    
    # 机器人在地图中的位置
            x = transform.transform.translation.x
            y = transform.transform.translation.y
            z = transform.transform.translation.z
    
            print(f"机器人位置: x={x:.2f}, y={y:.2f}, z={z:.2f}")
    
        except Exception as e:
            print(f"无法获取变换: {e}")





        if self.own_pos is None or self.peer_pos is None:
            return

        dx = self.own_pos.x - self.peer_pos.x
        dy = self.own_pos.y - self.peer_pos.y
        dist = math.hypot(dx, dy)

        if dist <= self.distance_limit:
            # 发布通信消息（示例）
            msg = Bool()
            msg.data = f'Hello from {self.ros_node.get_namespace()}'
            self.in_range.publish(msg)
            self.ros_node.get_logger().info(f'距离 {dist:.2f} m, 通信开启')
        else:
            self.ros_node.get_logger().info(f'距离 {dist:.2f} m, 通信断开')
        
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
        options={},
        inputs={},
        outputs={},
        max_children=0,
        ))
class DistanceTest(Leaf):
    """Check the state of a symbol"""


    def receive_pose_msg_cb(self,msg):
        if msg.transforms[0].header.frame_id == 'odom' and msg.transforms[0].child_frame_id == 'base_footprint':
            
            self.transform = msg.transforms[0]
            
        

    def _do_setup(self):    
        
        
        
        #if self.depth >0:
        self.pose_msg_sub = self.ros_node.create_subscription(
            tf2_msgs.msg.TFMessage,
            "/turtle0/tf",   
            self.receive_pose_msg_cb,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=20,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
        )
        self.transform = None
        
        
    

        
        
    def _do_tick(self):     
        """Output the value of a symbol"""
        
        x=0
        # self.ros_node.get_logger().info(f"start tf node 1 ...")
        # self.buffer = Buffer()
        # self.ros_node.get_logger().info(f"start tf node 2 ...")
        # try:
        #     transform = self.buffer.lookup_transform(#Buffer.lookup_transform(#tf_buffer.lookup_transform(
        #     'map',          # 参考坐标系
        #     'base_link',    # 机器人本体坐标系
        #     self.ros_node.get_clock().now()#time.Time()  # 获取最新变换
        # )
        #     self.ros_node.get_logger().info(f"start getting tf msg ...")


        if self.transform is not None:
            transform = self.transform


    
    # 机器人在地图中的位置
            x = transform.transform.translation.x
            y = transform.transform.translation.y
            z = transform.transform.translation.z
    
            self.ros_node.get_logger().warn(f"机器人位置: x={x:.2f}, y={y:.2f}, z={z:.2f}")
    
        # except Exception as e:
        #     print(f"无法获取变换: {e}")

        # if not x==0:
        
            return NodeMsg.SUCCEEDED
        else:
            return NodeMsg.RUNNING
        

    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE
    


# @define_bt_node(NodeConfig(
#     options={"distance_limit":int},
#     inputs={},  
#     outputs={},
#     max_children=0,
#     ))
# class GetDistance(Leaf):##Leaf？
#     """Gets a location name from the queue"""

   
#     def _do_setup(self):
        
        
#         self.distance_limit = self.options["distance_limit"]
#         self.peer_name = peer_name

#         # 订阅对方位置
#         self.ros_node.create_subscription(
#             PoseStamped, f'/{peer_name}/pose', self.peer_pose_cb, 10)

#         # 订阅自己位置（也可用 tf，这里简单处理）
#         self.create_subscription(
#             PoseStamped, f'/{self.ros_node.get_namespace()}/pose', self.own_pose_cb, 10)
        
#         # 订阅对方位置
        

#         # 通信话题（距离内才发布）
#         self.in_range = self.ros_node.create_publisher(Bool, f'/{self.ros_node.get_namespace()}/to_{peer_name}', 10)

#         self.own_pos = None
#         self.peer_pos = None
#         self.timer = self.ros_node.create_timer(0.1, self.check_distance)

#     @abstractmethod
#     def own_pose_cb(self, msg):
#         self.own_pos = msg.pose.position

#     @abstractmethod
#     def peer_pose_cb(self, msg):
#         self.peer_pos = msg.pose.position

#     @abstractmethod
#     def check_distance(self):
#         if self.own_pos is None or self.peer_pos is None:
#             return

#         dx = self.own_pos.x - self.peer_pos.x
#         dy = self.own_pos.y - self.peer_pos.y
#         dist = math.hypot(dx, dy)

#         if dist <= self.threshold:
#             # 发布通信消息（示例）
#             msg = Bool()
#             msg.data = f'Hello from {self.get_namespace()}'
#             self.pub.publish(msg)
#             self.get_logger().info_once(f'距离 {dist:.2f} m, 通信开启')
#         else:
#             self.get_logger().info_once(f'距离 {dist:.2f} m, 通信断开')
        
#         #self.input["get_msg"]=NodeData.set_updated("get_msg")
        
#     def _do_tick(self):##tick(self):     #update(self):
#         """Checks for the status of the navigation action"""
#         # If there is a result, we can check the status of the action directly.
#         # Otherwise, the action is still running.
        
        
#         #self.inputs.get_callback("get_msg")
#         #self.inputs.handle_subscriptions()
        
#         self.logwarn(self.inputs.is_updated("get_msg"))
#         self.logwarn(self.inputs["get_msg"])
#         self.outputs["target_color"] = 'blue'
#         self.outputs["next_target"] = 'red'
#         if self.inputs["get_msg"]==[]:
#             return NodeMsg.RUNNING
#         else:
#             if self.outputs["target_color"] is not None: #target_color is not None:
#                 self.logwarn(self.inputs["get_msg"])
#                 if len(self.inputs["get_msg"])<5:
#                     self.outputs["target_color"] = self.inputs["get_msg"]
#                 return NodeMsg.SUCCEEDED
#             else:
#                 return NodeMsg.FAILED
#     def _do_shutdown(self):
#         pass

#     def _do_reset(self):
#         return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE