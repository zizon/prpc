# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: applicationclient_protocol.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)


import Security_pb2
import yarn_service_protos_pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='applicationclient_protocol.proto',
  package='hadoop.yarn',
  serialized_pb='\n applicationclient_protocol.proto\x12\x0bhadoop.yarn\x1a\x0eSecurity.proto\x1a\x19yarn_service_protos.proto2\xb4\x1a\n ApplicationClientProtocolService\x12l\n\x11getNewApplication\x12*.hadoop.yarn.GetNewApplicationRequestProto\x1a+.hadoop.yarn.GetNewApplicationResponseProto\x12u\n\x14getApplicationReport\x12-.hadoop.yarn.GetApplicationReportRequestProto\x1a..hadoop.yarn.GetApplicationReportResponseProto\x12l\n\x11submitApplication\x12*.hadoop.yarn.SubmitApplicationRequestProto\x1a+.hadoop.yarn.SubmitApplicationResponseProto\x12{\n\x16\x66\x61ilApplicationAttempt\x12/.hadoop.yarn.FailApplicationAttemptRequestProto\x1a\x30.hadoop.yarn.FailApplicationAttemptResponseProto\x12k\n\x14\x66orceKillApplication\x12(.hadoop.yarn.KillApplicationRequestProto\x1a).hadoop.yarn.KillApplicationResponseProto\x12l\n\x11getClusterMetrics\x12*.hadoop.yarn.GetClusterMetricsRequestProto\x1a+.hadoop.yarn.GetClusterMetricsResponseProto\x12\x66\n\x0fgetApplications\x12(.hadoop.yarn.GetApplicationsRequestProto\x1a).hadoop.yarn.GetApplicationsResponseProto\x12\x66\n\x0fgetClusterNodes\x12(.hadoop.yarn.GetClusterNodesRequestProto\x1a).hadoop.yarn.GetClusterNodesResponseProto\x12]\n\x0cgetQueueInfo\x12%.hadoop.yarn.GetQueueInfoRequestProto\x1a&.hadoop.yarn.GetQueueInfoResponseProto\x12q\n\x10getQueueUserAcls\x12-.hadoop.yarn.GetQueueUserAclsInfoRequestProto\x1a..hadoop.yarn.GetQueueUserAclsInfoResponseProto\x12s\n\x12getDelegationToken\x12-.hadoop.common.GetDelegationTokenRequestProto\x1a..hadoop.common.GetDelegationTokenResponseProto\x12y\n\x14renewDelegationToken\x12/.hadoop.common.RenewDelegationTokenRequestProto\x1a\x30.hadoop.common.RenewDelegationTokenResponseProto\x12|\n\x15\x63\x61ncelDelegationToken\x12\x30.hadoop.common.CancelDelegationTokenRequestProto\x1a\x31.hadoop.common.CancelDelegationTokenResponseProto\x12\x8a\x01\n\x1bmoveApplicationAcrossQueues\x12\x34.hadoop.yarn.MoveApplicationAcrossQueuesRequestProto\x1a\x35.hadoop.yarn.MoveApplicationAcrossQueuesResponseProto\x12\x8a\x01\n\x1bgetApplicationAttemptReport\x12\x34.hadoop.yarn.GetApplicationAttemptReportRequestProto\x1a\x35.hadoop.yarn.GetApplicationAttemptReportResponseProto\x12{\n\x16getApplicationAttempts\x12/.hadoop.yarn.GetApplicationAttemptsRequestProto\x1a\x30.hadoop.yarn.GetApplicationAttemptsResponseProto\x12o\n\x12getContainerReport\x12+.hadoop.yarn.GetContainerReportRequestProto\x1a,.hadoop.yarn.GetContainerReportResponseProto\x12`\n\rgetContainers\x12&.hadoop.yarn.GetContainersRequestProto\x1a\'.hadoop.yarn.GetContainersResponseProto\x12l\n\x11getNewReservation\x12*.hadoop.yarn.GetNewReservationRequestProto\x1a+.hadoop.yarn.GetNewReservationResponseProto\x12t\n\x11submitReservation\x12..hadoop.yarn.ReservationSubmissionRequestProto\x1a/.hadoop.yarn.ReservationSubmissionResponseProto\x12l\n\x11updateReservation\x12*.hadoop.yarn.ReservationUpdateRequestProto\x1a+.hadoop.yarn.ReservationUpdateResponseProto\x12l\n\x11\x64\x65leteReservation\x12*.hadoop.yarn.ReservationDeleteRequestProto\x1a+.hadoop.yarn.ReservationDeleteResponseProto\x12g\n\x10listReservations\x12(.hadoop.yarn.ReservationListRequestProto\x1a).hadoop.yarn.ReservationListResponseProto\x12h\n\x0fgetNodeToLabels\x12).hadoop.yarn.GetNodesToLabelsRequestProto\x1a*.hadoop.yarn.GetNodesToLabelsResponseProto\x12i\n\x10getLabelsToNodes\x12).hadoop.yarn.GetLabelsToNodesRequestProto\x1a*.hadoop.yarn.GetLabelsToNodesResponseProto\x12u\n\x14getClusterNodeLabels\x12-.hadoop.yarn.GetClusterNodeLabelsRequestProto\x1a..hadoop.yarn.GetClusterNodeLabelsResponseProto\x12\x84\x01\n\x19updateApplicationPriority\x12\x32.hadoop.yarn.UpdateApplicationPriorityRequestProto\x1a\x33.hadoop.yarn.UpdateApplicationPriorityResponseProto\x12h\n\x11signalToContainer\x12(.hadoop.yarn.SignalContainerRequestProto\x1a).hadoop.yarn.SignalContainerResponseProto\x12\x84\x01\n\x19updateApplicationTimeouts\x12\x32.hadoop.yarn.UpdateApplicationTimeoutsRequestProto\x1a\x33.hadoop.yarn.UpdateApplicationTimeoutsResponseProtoB?\n\x1corg.apache.hadoop.yarn.protoB\x19\x41pplicationClientProtocol\x88\x01\x01\xa0\x01\x01')





DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), '\n\034org.apache.hadoop.yarn.protoB\031ApplicationClientProtocol\210\001\001\240\001\001')
# @@protoc_insertion_point(module_scope)