"""
Vision behaviors for TurtleBot3
"""

import cv2
import cv_bridge
import rclpy
from rclpy.duration import Duration
from sensor_msgs.msg import Image


from rclpy.qos import  QoSProfile, ReliabilityPolicy#QoSDurabilityPolicy,QoSReliabilityPolicy




import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

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
        options={"color_type":type,"namespace":str},   ############ add namespace
        inputs={"get_color": OptionRef("color_type")},
        outputs={"result":bool},
        max_children=0,
    ))
class LookForObject(Leaf):
    """
    Gets images from the robot and looks for object using
    simple HSV color space thresholding and blob detection.
    """
    @abstractmethod 
    def img_callback(self, msg):
        
        self.latest_img_msg = msg
        if msg==None:
            self.logwarn("Image not received")
    
    
    def _do_setup(self):

        self.namespace=self.options["namespace"]

        # Define HSV color space thresholds
        self.hsv_threshold_dict = {
        "red": ((160, 220, 0), (180, 255, 255)),
        "green": ((40, 220, 0), (90, 255, 255)),
        "blue": ((100, 220, 0), (150, 255, 255)),
        }
        #img_timeout=3.0
        self.qos_profile = QoSProfile( reliability=ReliabilityPolicy.BEST_EFFORT,#QoS,RMW_QOS_POLICY_RELIABILITY_RELIABLE,
        depth=10)
        #durability=QoSDurabilityPolicy.RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL)

        
        self.img_timeout = Duration(nanoseconds=3.0*1e9)#img_timeout * 1e9)
        self.viz_window_name = "Image with Detections"
        self.visualize = True
        if self.visualize:
            plt.figure(1)
            plt.axis("off")
            plt.title(self.viz_window_name)
            plt.ion()
            
        self.bridge = cv_bridge.CvBridge()
        params = cv2.SimpleBlobDetector_Params()
        params.minArea = 100
        params.maxArea = 100000
        params.filterByArea = True
        params.filterByColor = False
        params.filterByInertia = False
        params.filterByConvexity = False
        params.thresholdStep = 50
        self.detector = cv2.SimpleBlobDetector_create(params)

        self.start_time = self._ros_node.get_clock().now()
        self.latest_img_msg = None
        self.img_sub = self._ros_node.create_subscription(
            Image, "/"+self.namespace+"/camera/image_raw", self.img_callback, self.qos_profile#10
        )

        

    def _do_tick(self):
        if self.inputs["get_color"]==None:
            self.color=self.color
        else:
            self.color = self.inputs["get_color"]
        """Starts all the vision related objects"""
        
        
        """Looks for at least one object detection using HSV thresholding"""
        # Wait for an image message and handle failure case
        now = self._ros_node.get_clock().now()
        
        while self.latest_img_msg is None:
            if now - self.start_time < self.img_timeout:
                now = self._ros_node.get_clock().now()
                #return NodeMsg.RUNNING
            #if self.latest_img_msg is not None:
            #    self.logwarn("1")
                
            #if now - self.start_time < self.img_timeout:
            #    return NodeMsg.RUNNING
            #else:
            
            #if self.latest_img_msg is None:    
            #    self.logwarn("Image timeout exceeded")
            #    return NodeMsg.FAILED



        self.hsv_min = self.hsv_threshold_dict[self.color][0]
        self.hsv_max = self.hsv_threshold_dict[self.color][1]
        
        # Process the image
        img = self.bridge.imgmsg_to_cv2(self.latest_img_msg, desired_encoding="bgr8")
        hsv = cv2.cvtColor(img, cv2.COLOR_BGR2HSV)
        self.logwarn(hsv)
        mask = cv2.inRange(hsv, self.hsv_min, self.hsv_max)
        keypoints = self.detector.detect(mask)

        # Visualize, if enabled
        if self.visualize:
            labeled_img = cv2.drawKeypoints(
                img,
                keypoints,
                None,
                (255, 0, 0),
                cv2.DRAW_MATCHES_FLAGS_DRAW_RICH_KEYPOINTS,
            )
            # OpenCV visualization
            cv2.destroyAllWindows()
            cv2.imshow(self.viz_window_name, labeled_img)
            cv2.waitKey(100)

            # Matplotlib visualization
            # plt.imshow(labeled_img[:,:,::-1])
            # plt.pause(0.1)

        # If there were no failures along the way, the behavior was successful
        if len(keypoints) == 0:
            self.logwarn("No objects detected")
            self.outputs["result"]=False
            return NodeMsg.FAILED
            
        for k in keypoints:
            self.logwarn(f"Detected object at [{k.pt[0]}, {k.pt[1]}]")
            self.outputs["result"]=True
        return NodeMsg.SUCCEEDED
        


            
    def _do_shutdown(self):
        pass

    def _do_reset(self):
        return NodeMsg.IDLE

    def _do_untick(self):
        return NodeMsg.IDLE

    
    #def terminate(self, new_status):
    #    self.logger.info(f"Terminated with status {new_status}")
    #    self.img_sub = None
    #    self.latest_img_msg = None

    
