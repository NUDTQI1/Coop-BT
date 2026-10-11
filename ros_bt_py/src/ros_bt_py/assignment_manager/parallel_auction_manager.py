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


"""Module implementing the ParallelAuctionManager.

This auction manager runs multiple single item auctions in parallel and assigns robots to
task based on the lowest bid.
"""
import dataclasses
import threading
import uuid
from typing import Dict

import rclpy
#from rclpy import ServiceException
from rclpy.action.server import ServerGoalHandle
import rclpy.logging
from rclpy.task import Future
from rclpy.client import Client
from rclpy.service import Service
from rclpy.subscription import Subscription
from rclpy.publisher import Publisher
from rclpy.action import ActionServer

from builtin_interfaces.msg import Time
from rclpy.duration import Duration
from rclpy.clock import Clock

from rclpy.node import Node, ReentrantCallbackGroup
from rclpy.qos import (
    QoSDurabilityPolicy,
    QoSHistoryPolicy,
    QoSProfile,
    QoSReliabilityPolicy,
)  #add

import time

from ros_bt_py_interfaces.msg import AuctionMessage
from ros_bt_py_interfaces.srv import (
    FindBestCapabilityExecutor,
    GetLocalBid,
    GetAvailableRemoteCapabilitySlots,
    ReserveRemoteCapabilitySlot,
    GetCapabilityInterfaces
)
from ros_bt_py.assignment_manager.assignment_manager import AssignmentManager
from ros_bt_py_interfaces.msg import DistanceCalculation
import math
from geometry_msgs.msg import PoseWithCovarianceStamped
from std_msgs.msg import Bool
import tf2_msgs.msg


@dataclasses.dataclass
class Bid:
    """Contains all information about a bid submitted by a robot."""

    bid: float
    """The bid value submitted by the robot."""

    executors: int
    """The number of available executors on the robot."""

    implementation: str
    """The implmentation on the robot the bid was genertated for."""


class AuctionStatus:
    """Status of a running auction.

    This class contains all information used by the AssignmentManager to track auctions.
    These auctions can be run by the robot themselves or a remote robot.
    """

    def __init__(self, auction_id: str, auctioneer_id: str, deadline: Time):
        """Create a new record of an auction for a specific auction.

        :param auction_id: The unique id of the auction within the current robot team.
        :type auction_id: str
        :param auctioneer_id: The name of the robot running the auction as the auctioneer.
        :type auctioneer_id: str
        :param deadline: The timestamp until bids can be submitted to the auction.
        :type deadline: rclpy.Time ##rospy.Time
        """
        self.__auction_id = auction_id
        self.__auctioneer_id = auctioneer_id
        self.__deadline = deadline
        self.__is_closed = False
        self.__bids: Dict[str, Bid] = {}
        self.__auction_status_lock = threading.Lock()

    @property
    def auction_id(self):
        """Unique id of the auction within the current robot team.

        :return: The id string of the auction.
        :rtype: str
        """
        return self.__auction_id

    @property
    def auctioneer_id(self):
        """Unique robot name of the auctioneer for the auction.

        :return: The name of the robot.
        :rtype: str
        """
        return self.__auctioneer_id

    @property
    def deadline(self):
        """The deadline of the auction until all bids have to be submitted.

        :return: The timestamp of the deadline.
        :rtype: rospy.Time
        """
        return self.__deadline

    @deadline.setter
    def deadline(self, new_deadline: Time):
        """Set the deadline of the auction until all bids have to be submitted.

        :param new_deadline: The new deadline timestamp for the auction.
        :type new_deadline: rospy.Time
        """
        with self.__auction_status_lock:
            self.__deadline = new_deadline

    @property
    def is_closed(self):
        """If the auction was closed by the auctioneer.

        :return: If the auction is closed.
        :rtype: bool
        """
        with self.__auction_status_lock:
            return self.__is_closed

    @is_closed.setter
    def is_closed(self, is_closed: bool):
        """Set if the auction is closed and no more bids can be accepted.

        :param is_closed: The new status of the auction.
        :type is_closed: bool
        """
        with self.__auction_status_lock:
            self.__is_closed = is_closed

    def get_valid_bids(self):
        """Get all valid bids submitted until now.

        A valid bid is a bid where the robot reported at least one available executor.
        This function is thread safe.

        :return: A list of tuples containing the id of the remote robot and is submitted bid.
        :rtype: List[Tuple[str, float]]
        """
        with self.__auction_status_lock:
            rclpy.logging.get_logger("parallel auction").warn(f"1111111111111133333333333 {self.__bids}")
            return list(
                map(
                    lambda y: (y, self.__bids[y].bid),
                    filter(lambda x: self.__bids[x].executors > 0, self.__bids),
                )
            )

    def get_bid(self, robot_name: str):
        """Get a specific bid submitted by a robot for this auciton.
            deadline
        :param robot_name: The name of the robot to retrieve the bid from.
        :type robot_name: str
        :raises exc: KeyError if the robot has not submitted a bid until now.
        :return: The bid value submitted by the robot.
        :rtype: float
        """
        with self.__auction_status_lock:
            try:
                return self.__bids[robot_name]
            except KeyError as exc:
                # rospy.logwarn(f"Failed to receive the bid for robot {robot_name}")
                raise exc

    def set_bid(
        self, robot_name: str, bid: float, no_executors: int, implementation_name: str
    ):
        """Set the bid value for a specifc robot.

        :param robot_name: Name of the robot submitting a bid.
        :type robot_name: str
        :param bid: The bid value for the robots best implmentation.
        :type bid: float
        :param no_executors: The number of executors available for the robot
        to run the implementation.
        :type no_executors: int
        :param implementation_name: The name of the implmentation the robot will use.
        :type implementation_name: str
        """
        with self.__auction_status_lock:
            self.__bids[robot_name] = Bid(
                bid=bid, executors=no_executors, implementation=implementation_name
            )
            rclpy.logging.get_logger("parallel action").warn(f"Set bid: {robot_name} {self.__bids[robot_name]}")
            # rospy.logdebug(f"Set bid: {robot_name} {self.__bids[robot_name]}")


class ParallelAuctionManager(AssignmentManager):
    """Parallel assignment manager class.

    This auction manager runs multiple single item auctions in parallel and assigns robots to
    task based on the lowest bid.
    """

    running_auctions: Dict[str, AuctionStatus] = {}

    def __init__(
        self,
        node:Node,
        local_topic_prefix: str,
        global_assignment_msg_topic_prefix: str,
    ):
        """Create a new ParallelAuctionManager using the given topic prefixes for its pub/sub's.

        :param local_topic_prefix: The prefix for communicating with the local robots.
        :type local_topic_prefix: str
        :param global_assignment_msg_topic_prefix: The prefix for communicating with other robots
        AssignmentManagers.
        :type global_assignment_msg_topic_prefix: str
        """
        super().__init__(
            node=node,
            local_topic_prefix=local_topic_prefix,
            global_assignment_msg_topic_prefix=global_assignment_msg_topic_prefix,
        )

        
        self.__local_topic_prefix = self.local_topic_prefix
        self.node.get_logger().warn(f"&&&&&&&&&&&&&&&&&&&&&& {self.__local_topic_prefix}")
        self.__auction_duration = Duration(seconds=15.0)#(seconds=rospy.get_param("auction_duration_sec", 15.0))

        self.__global_auction_message_pub: Publisher = self.node.create_publisher(
            AuctionMessage,
            self.global_assignment_msg_topic_prefix,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            #latch=False,
            #queue_size=1,
        )
        self.__global_auction_message_sub: Subscription = self.node.create_subscription(
            AuctionMessage,
            self.global_assignment_msg_topic_prefix,
            self.global_auction_messages_callback,
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            #queue_size=1,
        )





        ######################################################################################
        self.__distance_limit_calculate_pub: Publisher = self.node.create_publisher(
            DistanceCalculation,
            "~/calculate_distance",
            qos_profile=QoSProfile(
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE,
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,),
            callback_group=ReentrantCallbackGroup()    ### change 0701
            #latch=False,
            #queue_size=1,
        )
        self.annc_num = 0
        
        ######################################################################################






        local_bid_service_topic = f"{self.node.get_namespace()}/mission_control/get_local_bid"

        

        # self.__local_bid_service= self.node.create_client(#: Client 
        #     GetLocalBid,
        #     local_bid_service_topic,
        #     qos_profile=QoSProfile(
        #         reliability=QoSReliabilityPolicy.RELIABLE,
        #         durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        #         history=QoSHistoryPolicy.KEEP_LAST,
        #         depth=10,
        #     ),
        #     callback_group=ReentrantCallbackGroup()   ### change 0701
        #     )
        self.__local_bid_service_lock = threading.RLock()

        available_remote_slots_service_topic = f"{self.node.get_namespace()}/mission_control/get_available_remote_slots"

        self.__available_remote_capability_slots_service: Client = self.node.create_client(
            GetAvailableRemoteCapabilitySlots,
            available_remote_slots_service_topic,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
        )

        reserve_remote_slots_service_topic = f"{self.node.get_namespace()}/mission_control/reserve_remote_capability_slot"

        self.__reserve_remote_capability_slot_service = self.node.create_client(
            ReserveRemoteCapabilitySlot,
            reserve_remote_slots_service_topic,
            qos_profile=QoSProfile(
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
            history=QoSHistoryPolicy.KEEP_LAST,
            depth=10,
            ),
            callback_group=ReentrantCallbackGroup()
        )
        self.running_auctions_lock = threading.RLock()
        self.node.get_logger().warn("000000000000000000000000000")

    def __get_available_local_remote_capability_slots(self) -> int:
        try:
            self.available_req=(#response: GetAvailableRemoteCapabilitySlots.Response = (
                self.__available_remote_capability_slots_service.call_async(
                    GetAvailableRemoteCapabilitySlots.Request()   ### change 0701
                )#.result()
            )
            while not self.available_req.done():
                time.sleep(1.0)
                self.node.get_logger().warn("11111111111444444444444111111")
            response=self.available_req.result()
            return response.available_remote_capability_slots
        except Exception as exc:
            self.node.get_logger().error(f"Failed to receive the available remote slots: {exc}")
            return 0

    def _is_running_auction(self, auction_id: str):
        return not self.running_auctions[auction_id].is_closed








        # 订阅对方位置  #################################################################    
    def own_pose_cb(self, msg):
        ns1=self.node.get_namespace().lstrip('/')
        print(ns1)
        if msg.transforms[0].header.frame_id == f'{ns1}/odom' and msg.transforms[0].child_frame_id == f'{ns1}/base_footprint':#原来frame_id没有前缀
            self.own_pos = msg.transforms[0]
            
    def peer_pose_cb(self, msg):
        ns2=self.other_id.lstrip('/')
        print(ns2)
        if msg.transforms[0].header.frame_id == f'{ns2}/odom' and msg.transforms[0].child_frame_id == f'{ns2}/base_footprint':#原来frame_id没有前缀
            self.peer_pos = msg.transforms[0]

    def check_distance(self) -> bool:
        
        print("999999999 check")
        self.node.get_logger().info(f"999999999999 pose : own{self.own_pos}, other{self.peer_pos}!")
        while self.own_pos is None or self.peer_pos is None:
            time.sleep(0.1)
            self.node.get_logger().info("999999999999 pose is None !")
            # return False
        self.x = self.own_pos.transform.translation.x
        self.y = self.own_pos.transform.translation.y
        self.peer_x = self.peer_pos.transform.translation.x
        self.peer_y = self.peer_pos.transform.translation.y
        dx = self.x - self.peer_x
        dy = self.y - self.peer_y
        self.dist = math.hypot(dx, dy)
        self.node.get_logger().info(f"calculate distance: {self.dist}")

        if self.dist <= self.threshold:
            # # 发布通信消息（示例）
            # #assigned_symbol = self.node.set_parameters([('assigned_symbol',True)])
            # self.__distance_limit_calculate_pub.publish(
            #     DistanceCalculation(own_name=self.node.get_namespace(),other_name=self.other_id))
            # # msg = Bool()
            # # msg.data = f'Hello from {self.get_namespace()}'
            # # self.pub.publish(msg)
            self.node.get_logger().info(f'距离 {self.dist}, 通信开启, 可报价')
            self.annc_num = self.annc_num + 2
            return True
        else:
            self.node.get_logger().info(f'距离 {self.dist}, 通信断开, 不可报价')
            return False
        # 订阅对方位置  #################################################################
    
    def __handle_announcement_msg(self, msg: AuctionMessage):



        self.node.get_logger().warn(f"111155555555511 Start to handle announcement msg from {msg.sender_id} to {self.node.get_namespace()}!")
        self.other_id=msg.sender_id
        self.threshold = 4.2 #2.1
        self.if_assgned = False
        self.other_pose_sub = self.node.create_subscription(
            tf2_msgs.msg.TFMessage,
            "/tf",#f"{self.other_id}/tf",
            self.peer_pose_cb,
            qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=20,),
                callback_group=ReentrantCallbackGroup())

        # 订阅自己位置
        self.own_pose_sub = self.node.create_subscription(
            tf2_msgs.msg.TFMessage, 
            "/tf",#f"{self.node.get_namespace()}/tf", 
            self.own_pose_cb,
            qos_profile=QoSProfile(
                    reliability=QoSReliabilityPolicy.RELIABLE,
                    durability=QoSDurabilityPolicy.VOLATILE,
                    history=QoSHistoryPolicy.KEEP_LAST,
                    depth=20,),
                callback_group=ReentrantCallbackGroup() )
        self.node.get_logger().warn(f"1111555555 check1")
        self.if_assgned = self.check_distance()
        self.node.get_logger().warn(f"1111555555 check2")
        self.node.get_logger().info(f'annc num :{self.annc_num}')
        self.node.destroy_subscription(self.other_pose_sub)
        self.node.destroy_subscription(self.own_pose_sub)
        if not self.if_assgned:
            return 
    






        with self.running_auctions_lock:
            self.running_auctions[msg.auction_id] = AuctionStatus(
                auction_id=msg.auction_id,
                auctioneer_id=msg.sender_id,
                deadline=msg.deadline,
            )
            
        with self.__local_bid_service_lock:
            try:
                # bid_response: GetLocalBid.Response = self.__local_bid_service.call_async(
                #     GetLocalBid.Request(
                #         interface=msg.interface,
                #         node_id=msg.node_id,
                #         implementation_tags_dict=msg.tags_dict,
                #     )
                # )
                #self.__local_bid_service.wait_for_service(1.0)
                
                self.__local_bid_service= self.node.create_client(#: Client 
                    GetLocalBid,
                    f"{self.node.get_namespace()}/mission_control/get_local_bid",
                    qos_profile=QoSProfile(
                        reliability=QoSReliabilityPolicy.RELIABLE,
                        durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                        history=QoSHistoryPolicy.KEEP_LAST,
                        depth=10,
                    ),
                    callback_group=ReentrantCallbackGroup()   ### change 0701
                    )
                bid_request = GetLocalBid.Request()
                bid_request.interface=msg.interface
                bid_request.node_id=msg.node_id
                bid_request.implementation_tags_dict=msg.tags_dict
                bid_res = self.__local_bid_service.call_async(bid_request)
                self.node.get_logger().warn(f"111155555555511 already free : {bid_request}")
                while not bid_res.done():
                    time.sleep(1.0)
                    self.node.get_logger().warn(f"11115555555551111111111115555555555 {bid_res.result()}")
                bid_response=bid_res.result()
                if not bid_response.success:
                    self.node.get_logger().warn(
                        f"Failed to get local bid from: {self.__local_topic_prefix}, "
                        f"{bid_response.error_message}",
                        logger_name="assignment_system",
                    )
                    return
            except Exception as exc:
                self.node.get_logger().error(f"Cannot calculate local bid, service error: {exc}")
                return

        available_executors: int = self.__get_available_local_remote_capability_slots()

        self.node.get_logger().warn(f"111155555555511111111 available executor: {available_executors}")
        self.node.get_logger().warn(f"111155555555511111111 robot name:{self.__local_topic_prefix}, bid:{bid_response.bid} {bid_response.implementation_name}")

        with self.running_auctions_lock:
            self.running_auctions[msg.auction_id].set_bid(
                robot_name=self.__local_topic_prefix,
                bid=bid_response.bid + 0.1*self.dist,
                no_executors=available_executors,
                implementation_name=bid_response.implementation_name,
            )

        self.update_executor_numbers_for_running_auctions(
            available_executors=available_executors
        )
        self.node.get_logger().warn(f"111155555555511111111 update executor numbers")

        return

    def __handle_bid_msg(self, msg: AuctionMessage):
        try:
            with self.running_auctions_lock:
                auction_status = self.running_auctions[msg.auction_id]
                self.node.get_logger().warn(f"111111111133333333333333333 start to handle bid! auction: {msg}")
        except KeyError:
            self.node.get_logger().error(
                "Unknown auction detected! Ignoring as not all required information is present!"
            )
            return

        self.node.get_logger().warn(f"11111111113333333333333 cut1")
        if auction_status.auctioneer_id != self.__local_topic_prefix:
            self.node.get_logger().debug("Received bid for foreign auction, ignoring!")
            return

        self.node.get_logger().warn(f"11111111113333333333333 cut2 {auction_status.is_closed} {auction_status.deadline}")
        if auction_status.is_closed :#or msg.timestamp > auction_status.deadline:     



            ### 需要处理的！！！！！！！！！！！！！！！！！！！！！！！！！！
            ###两个时间格式不一样，timestamp是包含seconds和nanoseconds，deadline只包含nanoseconds
            ###具体设置是在find best executor格式部分



            self.node.get_logger().warn(
                f"Ignoring bid from {msg.sender_id} for auction {msg.auction_id}, "
                "as it arrived after the deadline!"
            )
            return

        self.node.get_logger().fatal(
            f"Received bid: {msg.auction_id} {msg.sender_id} {msg.bid} {msg.number_of_executors}"
        )

        auction_status.set_bid(
            robot_name=msg.sender_id,
            bid=msg.bid,
            implementation_name=msg.implementation_name,
            no_executors=msg.number_of_executors,
        )
        with self.running_auctions_lock:
            self.running_auctions[msg.auction_id] = auction_status
            self.node.get_logger().warn(f"already set bid!")
    def __handle_close_msg(self, msg: AuctionMessage):
        try:
            with self.running_auctions_lock:
                self.running_auctions[msg.auction_id].is_closed = True
        except KeyError:
            self.node.get_logger().error(f"Auction {msg.auction_id} not found, ignoring!")

    def __handle_abort_msg(self, msg: AuctionMessage):
        try:
            with self.running_auctions_lock:
                del self.running_auctions[msg.auction_id]
        except KeyError:
            self.node.get_logger().error(f"Auction {msg.auction_id} not found, ignoring!")

    def __handle_result_msg(self, msg: AuctionMessage):
        if msg.result_id == self.__local_topic_prefix:
            self.node.get_logger().info("Got awarded an auction!")

            available_slots = self.__get_available_local_remote_capability_slots()
            if available_slots >= 1:
                with self.running_auctions_lock:
                    for auction_id in filter(
                        self._is_running_auction, self.running_auctions
                    ):
                        status = self.running_auctions[auction_id]
                        try:
                            local_bid = status.get_bid(self.__local_topic_prefix)
                        except KeyError:
                            continue

                        bid_msg = AuctionMessage(
                            auction_id=auction_id,
                            timestamp=self.node.get_clock().now().to_msg(), #Clock.now(), ### change 0701 !!!!!!!!!!!!!!!!!不确定
                            sender_id=self.__local_topic_prefix,
                            message_type=AuctionMessage.BID,
                            bid=local_bid.bid,
                            number_of_executors=available_slots - 1,
                            implementation_name=local_bid.implementation,
                        )
                        self.__global_auction_message_pub.publish(bid_msg)

            local_bid = self.running_auctions[msg.auction_id].get_bid(
                self.__local_topic_prefix
            )

            self.node.get_logger().fatal(f"Top bid: {local_bid} Second Highest Bid: {msg.bid}")
            reserve_res=self.__reserve_remote_capability_slot_service.call_async(
                ReserveRemoteCapabilitySlot.Request(
                    remote_mission_control=msg.sender_id,
                    implementation_name=local_bid.implementation,
                    reauction_threshold=msg.bid,
                )
            )
            while not reserve_res.done():
                time.sleep(0.5)
                self.node.get_logger().warn(f"1111111111111166666666666")
            self.node.get_logger().warn(f"111111111111116666666666666 finish reserve remot slot: {reserve_res.result()}")

            del self.running_auctions[msg.auction_id]
            return

        with self.running_auctions_lock:
            try:
                local_bid = self.running_auctions[msg.auction_id].get_bid(
                    self.__local_topic_prefix
                )
            except KeyError:
                return
            if local_bid.executors > 0:
                self.node.get_logger().fatal(
                    f"Reallocating executor slots after losing {msg.auction_id} auction!"
                )

                available_executors = (
                    self.__get_available_local_remote_capability_slots()
                )

                self.update_executor_numbers_for_running_auctions(
                    available_executors=available_executors
                )

            del self.running_auctions[msg.auction_id]
            return

    def __handle_update_msg(self, msg: AuctionMessage):
        pass

    def update_executor_numbers_for_running_auctions(self, available_executors: int):
        """
        Update all running auctions with the new number of available executors.

        :param available_executors: The new number of available executors.
        :return: None
        """
        with self.running_auctions_lock:
            unsorted_running_auction_by_bid = []
            for auction_id in self.running_auctions:
                if self._is_running_auction(auction_id):
                    try:
                        self.running_auctions[auction_id].get_bid(
                            self.__local_topic_prefix
                        )
                    except KeyError:
                        continue
                    unsorted_running_auction_by_bid.append(auction_id)

            running_auctions_by_bid = list(
                sorted(
                    unsorted_running_auction_by_bid,
                    key=lambda x: self.running_auctions[x]
                    .get_bid(self.__local_topic_prefix)
                    .bid,
                )
            )

            possible_auctions = running_auctions_by_bid[:available_executors]
            impossible_auctions = running_auctions_by_bid[available_executors:]

            for auction_id in impossible_auctions:
                auction = self.running_auctions[auction_id]
                local_bid = auction.get_bid(self.__local_topic_prefix)

                self.node.get_logger().fatal(
                    f"Publishing bid: {auction.auction_id} {local_bid.bid} 0"
                )
                bid_msg = AuctionMessage(
                    auction_id=auction.auction_id,
                    timestamp=self.node.get_clock().now().to_msg(), #Clock.now(), ### change 0701 !!!!!!!!!!!!!!!!!不确定
                    sender_id=self.__local_topic_prefix,
                    message_type=AuctionMessage.BID,
                    bid=local_bid.bid,
                    number_of_executors=0,
                    implementation_name=local_bid.implementation,
                )
                self.__global_auction_message_pub.publish(bid_msg)

            for auction_id in possible_auctions:
                auction = self.running_auctions[auction_id]
                local_bid = auction.get_bid(self.__local_topic_prefix)

                self.node.get_logger().fatal(
                    f"Publishing bid: {auction.auction_id} {local_bid.bid} {available_executors}"
                )
                bid_msg = AuctionMessage(
                    auction_id=auction.auction_id,
                    timestamp=self.node.get_clock().now().to_msg(), #Clock.now(), ### change 0701 !!!!!!!!!!!!!!!!!不确定
                    sender_id=self.__local_topic_prefix,
                    message_type=AuctionMessage.BID,
                    bid=local_bid.bid,
                    number_of_executors=available_executors,
                    implementation_name=local_bid.implementation,
                )
                self.__global_auction_message_pub.publish(bid_msg)
                self.node.get_logger().warn("1111111111111111111 publish bid msg successfully")

    def global_auction_messages_callback(self, msg: AuctionMessage):
        """Handle messages on the global auction messages topic as a callback.

        This callback handles auction information coming from other robots, handling them
        appropriately.

        :param msg: The message received on the topic that should be handled.
        :type msg: AuctionMessage
        """
        if msg.sender_id == self.__local_topic_prefix:
            self.node.get_logger().debug_throttle(1, f"Ignoring local message: {msg.sender_id}")
            return

        if msg.message_type == msg.ANNOUNCEMENT:
            self.node.get_logger().debug(f"Handling announcement auction message {msg.auction_id}")
            self.__handle_announcement_msg(msg)
            return
        if msg.message_type == msg.BID:
            self.node.get_logger().debug(f"Handling bid auction message {msg.auction_id}")
            self.__handle_bid_msg(msg)
            return
        if msg.message_type == msg.CLOSE:
            self.node.get_logger().debug(f"Handling close auction message {msg.auction_id}")
            self.__handle_close_msg(msg)
            return
        if msg.message_type == msg.ABORT:
            self.node.get_logger().debug(f"Handling abort auction message {msg.auction_id}")
            self.__handle_abort_msg(msg)
            return
        if msg.message_type == msg.RESULT:
            self.node.get_logger().debug(f"Handling result auction message {msg.auction_id}")
            self.__handle_result_msg(msg)
            return
        if msg.message_type == msg.UPDATE:
            self.node.get_logger().debug(f"Handling update auction message {msg.auction_id}")
            self.__handle_update_msg(msg)
            return

    def find_best_capability_executor(
        self, goal: FindBestCapabilityExecutor.Request, response:FindBestCapabilityExecutor.Response    ### change 0701 : add response 原因?????????????????
    ) -> FindBestCapabilityExecutor.Response:
        """Find the best executor and implementation for a specifc capability interface.

        To this end an auction is stated and the best available team member is found to execute
        the interfaces action.

        :param goal: The goal containing the capability interface that should be executed.
        :type goal: FindBestCapabilityExecutorRequest
        :return: Response containing the optimal assignment.
        :rtype: FindBestCapabilityExecutorResponse
        """
        response = FindBestCapabilityExecutor.Response()

        self.node.get_logger().warn(f"00000000000000000000 start !")

        current_auction_id = str(uuid.uuid4())
        now_time=self.node.get_clock().now().to_msg
        current_deadline = self.node.get_clock().now()+self.__auction_duration   ### change 0701  格式是只有nano!!!!!!!!!!!!!!!!!! 不知道对时间计算会有什么影响
        #Clock.now() + self.__auction_duration
        self.node.get_logger().warn(f"00000000000000000000 cut0.5 !")

        with self.running_auctions_lock:
            self.running_auctions[current_auction_id] = AuctionStatus(
                auction_id=current_auction_id,
                auctioneer_id=self.__local_topic_prefix,
                deadline=current_deadline,
            )

            
        
        self.__global_auction_message_pub.publish(
            #self.msg1
            AuctionMessage(
                auction_id=current_auction_id,
                timestamp=self.node.get_clock().now().to_msg(),# Clock.now(),    ### change 0701  !!!!!!!!!!!!!!!!!! 不知道对时间计算会有什么影响
                sender_id=self.__local_topic_prefix,
                message_type=AuctionMessage.ANNOUNCEMENT,
                interface=goal.capability,
                tags_dict=goal.implementation_tags_dict,
                node_id=goal.node_id,
                deadline=current_deadline.to_msg(),
            )
        )
        self.node.get_logger().warn(f"00000000000000000000 cut {self.__auction_duration} !")

        time.sleep(10)#(self.__auction_duration + Duration(seconds=0.02)) #  change 0701 包括255行
        ################## 时间已改，如果需要多机拍卖需要改回更长的等待时间

        self.__global_auction_message_pub.publish(
            AuctionMessage(
                auction_id=current_auction_id,
                timestamp=self.node.get_clock().now().to_msg(),# Clock.now(),    ### change 0701  !!!!!!!!!!!!!!!!!! 不知道对时间计算会有什么影响
                sender_id=self.__local_topic_prefix,
                message_type=AuctionMessage.CLOSE,
            )
        )
        self.node.get_logger().warn(f"00000000000000000000 cut3 !")
        with self.running_auctions_lock:
            bids = self.running_auctions[current_auction_id].get_valid_bids()
            self.node.get_logger().warn(f"00000000000000000000 cut4 ! bids: {bids}")

        if len(bids) < 1:
            response.success = False
            response.error_message = "No valid bids received! Auction has failed!"
            self.node.get_logger().error(response.error_message)
            return response

        sorted_bids = sorted(bids, key=lambda x: x[1])
        top_bid = sorted_bids[0]
        top_bid_id = top_bid[0]

        if len(sorted_bids) > 1:
            second_highest_bid = sorted_bids[0][1] * 1.2
        else:
            second_highest_bid = top_bid[1] * 1.2

        self.node.get_logger().fatal(
            f"Auction result {current_auction_id} {top_bid_id} {top_bid[1]}!"
        )

        top_bid_info = self.running_auctions[current_auction_id].get_bid(top_bid_id)

        self.__global_auction_message_pub.publish(
            AuctionMessage(
                auction_id=current_auction_id,
                timestamp=self.node.get_clock().now().to_msg(), #Clock.now(), ### change 0701 !!!!!!!!!!!!!!!!!不确定
                sender_id=self.__local_topic_prefix,
                message_type=AuctionMessage.RESULT,
                bid=second_highest_bid,
                result_id=top_bid_id,
                implementation_name=top_bid_info.implementation,
            )
        )
        self.node.get_logger().warn(f"00000000000000000000 cut7 !")

        time.sleep(1.0) #(Duration(seconds=1))  ### change 0701

        response.success = True
        response.implementation_name = top_bid_info.implementation
        response.execute_local = top_bid_id == self.__local_topic_prefix
        response.executor_mission_control_topic = top_bid_id

        return response
