# Copyright 2023 FZI Forschungszentrum Informatik
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
cmake_minimum_required(VERSION 3.8)
project(ros_bt_py_interfaces)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
find_package(std_msgs REQUIRED)
find_package(geometry_msgs REQUIRED)
find_package(unique_identifier_msgs REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(builtin_interfaces REQUIRED)
#find_package(rosidl_default_runtime REQUIRED)

install(DIRECTORY
  capabilities
  DESTINATION share/${PROJECT_NAME}
)


rosidl_generate_interfaces(${PROJECT_NAME}
  #"action/ExecuteCapability.action"
  "action/ExecuteRemoteCapability.action"
  #"action/FindBestCapabilityImplementation.action"
  "action/RunTree.action"
  "action/FindBestExecutor.action"

  "msg/AuctionMessage.msg"
  "msg/CapabilityExecutionStatus.msg"
  "msg/CapabilityFeedback.msg"
  "msg/CapabilityImplementation.msg"
  "msg/CapabilityInterface.msg"
  "msg/CapabilityIOBridgeData.msg"
  "msg/CapabilityRequest.msg"
  "msg/CapList.msg"
  "msg/DebugInfo.msg"
  "msg/DebugSettings.msg"
  "msg/DistanceCalculation.msg"
  "msg/DocumentedNode.msg"
  "msg/JoinTeam.msg"
  "msg/Message.msg"
  "msg/Messages.msg"
  "msg/Node.msg"
  "msg/NodeData.msg"
  "msg/NodeDataLocation.msg"
  "msg/NodeDataWiring.msg"
  "msg/NodeDiagnostics.msg"
  "msg/Package.msg"
  "msg/Packages.msg"
  "msg/PingMsg.msg"
  "msg/Precondition.msg"
  "msg/RemoteCapabilitySlotStatus.msg"
  "msg/RemoteSlotState.msg"
  "msg/Tree.msg"
  "msg/TreeDataUpdate.msg"
  "msg/UtilityBounds.msg"

  "srv/AddAssignmentRequest.srv"
  "srv/AddNode.srv"
  "srv/AddNodeAtIndex.srv"
  "srv/CancelRemoteCapabilitySlot.srv"
  "srv/CheckPreconditionStatus.srv"
  "srv/DeleteCapabilityImplementation.srv"
  "srv/FindBestCapabilityExecutor.srv"
  "srv/GetAvailableRemoteCapabilitySlots.srv"
  "srv/GetCapabilityImplementations.srv"
  "srv/GetCapabilityInterfaces.srv"
  "srv/GetLocalBid.srv"
  "srv/LoadCapabilities.srv"
  "srv/PrepareLocalImplementation.srv"
  "srv/PutCapabilityImplementation.srv"
  "srv/PutCapabilityInterfaces.srv"
  "srv/RequestCapabilityExecution.srv"
  "srv/ReserveRemoteCapabilitySlot.srv"
  "srv/RunRemoteCapabilitySlot.srv"
  "srv/SaveCapabilities.srv"
  "srv/CancelAssignmentRequest.srv"
  "srv/ChangeTreeName.srv"
  "srv/ClearTree.srv"
  "srv/Continue.srv"
  "srv/ControlTreeExecution.srv"
  "srv/EvaluateUtility.srv"
  "srv/FixYaml.srv"
  "srv/GenerateSubtree.srv"
  "srv/GetAssignmentRequestResult.srv"
  "srv/GetAssignmentRequestStatus.srv"
  "srv/GetAvailableNodes.srv"
  "srv/GetAvailableSubtrees.srv"
  "srv/GetFolderStructure.srv"
  "srv/GetMessageFields.srv"
  "srv/GetPackageStructure.srv"
  "srv/GetStorageFolders.srv"
  "srv/GetSubtree.srv"
  "srv/InsertNode.srv"
  "srv/LoadCapabilityMatrix.srv"
  "srv/LoadTree.srv"
  "srv/LoadTreeFromPath.srv"
  "srv/MigrateTree.srv"
  "srv/ModifyBreakpoints.srv"
  "srv/MorphNode.srv"
  "srv/MoveNode.srv"
  "srv/SaveTree.srv"
  "srv/SetExecutionMode.srv"
  "srv/SetOptions.srv"
  "srv/SetSimulateTick.srv"
  "srv/TestService.srv"
  "srv/WireNodeData.srv"
  "srv/ReloadTree.srv"
  "srv/RemoveNode.srv"
  "srv/ReplaceNode.srv"

  #DEPENDENCIES builtin_interfaces     #<depend>builtin_interfaces</depend>
)


ament_package()
