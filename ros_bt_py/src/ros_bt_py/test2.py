import rclpy
from rclpy.action import ActionServer
from rclpy.action.server import ServerGoalHandle
from ros_bt_py_interfaces.action import ExecuteRemoteCapability
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
import time
import queue
import threading


class ActionServerNode(Node):
    def __init__(self):
        super().__init__('action_server_node')

        # 创建 ActionServer，绑定目标处理函数
        self._action_server = ActionServer(
            self,
            ExecuteRemoteCapability,
            'your_action_name',  # 服务名
            self.execute_callback,  # 目标执行回调函数
            goal_callback=self.goal_callback
        )
        self.get_logger().info('Action Server is ready and listening for goals.')
        self.result_queue = queue.Queue()

    #async 
    def execute_callback(self, goal_handle: ServerGoalHandle):
        """
        处理客户端发送的目标请求，并返回反馈和结果。
        :param goal_handle: 客户端请求的目标句柄
        """
        messages=goal_handle.implementation_name#messages = goal_handle.request.implementation_name
        self.get_logger().info('Received goal request:' + messages)
        self.get_logger().info(f'goal type: {type(goal_handle)}')

        # 创建反馈消息
        #feedback_msg = ExecuteRemoteCapability.Feedback()
        #feedback_msg.completion_progress = 0.0

        # 发送初步的反馈
        #goal_handle.publish_feedback(feedback_msg)

        # 模拟处理任务，反馈进度
        for i in range(1, 6):
            #feedback_msg.completion_progress = i * 1.0
            #goal_handle.publish_feedback(feedback_msg)
            self.get_logger().info(f'Feedback: {i * 20}% completed')
            time.sleep(1)  # 模拟延迟

        #goal_handle.succeed()
        # 最终返回结果
        result = ExecuteRemoteCapability.Result()
        result.error_message = 'Action completed successfully.'
        self.get_logger().info('结果为' + result.error_message)
        self.result_queue.put(result)
        return result
    
    async def goal_callback(self, goal_handle: ServerGoalHandle):
        """
        处理客户端发送的目标请求，并返回反馈和结果。
        :param goal_handle: 客户端请求的目标句柄
        """
        self.get_logger().info('11111111111111111111111')
        messages=goal_handle.implementation_name#messages = goal_handle.request.implementation_name
        self.get_logger().info('Received goal request:' + messages)
        self.get_logger().info(f'goal type: {type(goal_handle)}')
        thread = threading.Thread(
                name=f"Thread-RemoteCapabilitySlot",
                target=self.execute_callback,
                args=(goal_handle),   ### change 0701
            )
        thread.start()
        result=self.result_queue.get()
        return result
#execute_remote_capability_exec

def main(args=None):
    rclpy.init(args=args)

    # 创建 ActionServerNode 节点
    action_server_node = ActionServerNode()

    # 使用多线程执行器
    executor = MultiThreadedExecutor()
    executor.add_node(action_server_node)

    try:
        executor.spin()  # 启动事件循环，等待客户端请求
    except KeyboardInterrupt:
        pass
    finally:
        action_server_node.destroy_node()
        rclpy.shutdown()


if __name__ == '__main__':
    main()
