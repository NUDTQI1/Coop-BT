import rclpy
from rclpy.action import ActionClient
from ros_bt_py_interfaces.action import ExecuteRemoteCapability
from rclpy.node import Node
from exceptions import BehaviorTreeException
from rclpy.task import Future
from rclpy.action.client import ClientGoalHandle
import asyncio
import time
from action_msgs.msg import GoalStatus
from ros_bt_py_interfaces.srv import PrepareLocalImplementation

class ActionClientNode(Node):
    def __init__(self):
        super().__init__('action_client_node')
        
        self._action_client = ActionClient(self, ExecuteRemoteCapability, 'your_action_name')

        self.cliethandle = None

        # prepare_local_capability_implementation_service_proxy = self.create_client(
        #     PrepareLocalImplementation,
        #     "~prepare_local_implementation"
        # )
        # response: PrepareLocalImplementation.Response = prepare_local_capability_implementation_service_proxy.call(PrepareLocalImplementation.Request())

        # response.implementation_subtree
        # response.error_message
        # response.success

    def send_goal(self, messages):
        print(messages)
        goal_msg = ExecuteRemoteCapability.Goal()
        goal_msg.implementation_name = "some_value"
        self.get_logger().info('111')
        self._action_client.wait_for_server()
        self.get_logger().info('222')
        goal_handle_future = self._action_client.send_goal_async(goal_msg ,feedback_callback=self.feedback_callback)
        
        goal_handle_future.add_done_callback(self.goal_response_callback)
        
    
    def feedback_callback(self, feedback_msg):
        feedback = feedback_msg.feedback
        print(feedback.completion_progress, end='', flush=True)

    def goal_response_callback(self, future: Future):
        goal_handle: ClientGoalHandle = future.result()

        self.cliethandle = goal_handle

        future = goal_handle.get_result_async()

        future.add_done_callback(self.result_callback)
    
    def result_callback(self, future: Future):
        res = future.result()
        if res.status == GoalStatus.STATUS_SUCCEEDED:
            print("result is:", res.result.error_message)
        rclpy.shutdown()

    def get_result_callback(self, future):
        print()
        self.send_goal(input("Input: "))


def main(args=None):
    rclpy.init(args=args)

    test_list=[]
    

    action_client = ActionClientNode()

    Node.get_logger(action_client).warn(f"test_list:{test_list}")
    test_list.append("aaa")
    Node.get_logger(action_client).warn(f"test_list:{test_list}")


    res =  action_client.send_goal(input("Input: "))


    rclpy.spin(action_client)
        

if __name__ == '__main__':
    main()
