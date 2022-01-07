// Code generated by protoc-gen-go. DO NOT EDIT.
// source: proto/job/job.proto

package job

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type SubmitRequest struct {
	// job名称
	Name string `protobuf:"bytes,1,opt,name=Name,proto3" json:"Name"`
	// job类型
	Type string `protobuf:"bytes,2,opt,name=Type,proto3" json:"Type"`
	// 插件集合
	PluginSet []string `protobuf:"bytes,3,rep,name=PluginSet,proto3" json:"PluginSet"`
	// job数据
	Data                 string   `protobuf:"bytes,4,opt,name=Data,proto3" json:"Data"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SubmitRequest) Reset()         { *m = SubmitRequest{} }
func (m *SubmitRequest) String() string { return proto.CompactTextString(m) }
func (*SubmitRequest) ProtoMessage()    {}
func (*SubmitRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_4f60868841c20a44, []int{0}
}

func (m *SubmitRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SubmitRequest.Unmarshal(m, b)
}
func (m *SubmitRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SubmitRequest.Marshal(b, m, deterministic)
}
func (m *SubmitRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SubmitRequest.Merge(m, src)
}
func (m *SubmitRequest) XXX_Size() int {
	return xxx_messageInfo_SubmitRequest.Size(m)
}
func (m *SubmitRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_SubmitRequest.DiscardUnknown(m)
}

var xxx_messageInfo_SubmitRequest proto.InternalMessageInfo

func (m *SubmitRequest) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *SubmitRequest) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *SubmitRequest) GetPluginSet() []string {
	if m != nil {
		return m.PluginSet
	}
	return nil
}

func (m *SubmitRequest) GetData() string {
	if m != nil {
		return m.Data
	}
	return ""
}

type AsyncSubmitResponse struct {
	// job ID需要记录
	Id                   int64    `protobuf:"varint,1,opt,name=Id,proto3" json:"Id"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *AsyncSubmitResponse) Reset()         { *m = AsyncSubmitResponse{} }
func (m *AsyncSubmitResponse) String() string { return proto.CompactTextString(m) }
func (*AsyncSubmitResponse) ProtoMessage()    {}
func (*AsyncSubmitResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_4f60868841c20a44, []int{1}
}

func (m *AsyncSubmitResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_AsyncSubmitResponse.Unmarshal(m, b)
}
func (m *AsyncSubmitResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_AsyncSubmitResponse.Marshal(b, m, deterministic)
}
func (m *AsyncSubmitResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_AsyncSubmitResponse.Merge(m, src)
}
func (m *AsyncSubmitResponse) XXX_Size() int {
	return xxx_messageInfo_AsyncSubmitResponse.Size(m)
}
func (m *AsyncSubmitResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_AsyncSubmitResponse.DiscardUnknown(m)
}

var xxx_messageInfo_AsyncSubmitResponse proto.InternalMessageInfo

func (m *AsyncSubmitResponse) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

type SyncSubmitResponse struct {
	// job ID
	Id int64 `protobuf:"varint,1,opt,name=Id,proto3" json:"Id"`
	// job状态,2：执行完成，3：异常退出，4：已取消执行，5：推送失败
	Status int32 `protobuf:"varint,2,opt,name=status,proto3" json:"status"`
	// 处理结果
	Result               string   `protobuf:"bytes,3,opt,name=Result,proto3" json:"Result"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SyncSubmitResponse) Reset()         { *m = SyncSubmitResponse{} }
func (m *SyncSubmitResponse) String() string { return proto.CompactTextString(m) }
func (*SyncSubmitResponse) ProtoMessage()    {}
func (*SyncSubmitResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_4f60868841c20a44, []int{2}
}

func (m *SyncSubmitResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SyncSubmitResponse.Unmarshal(m, b)
}
func (m *SyncSubmitResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SyncSubmitResponse.Marshal(b, m, deterministic)
}
func (m *SyncSubmitResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SyncSubmitResponse.Merge(m, src)
}
func (m *SyncSubmitResponse) XXX_Size() int {
	return xxx_messageInfo_SyncSubmitResponse.Size(m)
}
func (m *SyncSubmitResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_SyncSubmitResponse.DiscardUnknown(m)
}

var xxx_messageInfo_SyncSubmitResponse proto.InternalMessageInfo

func (m *SyncSubmitResponse) GetId() int64 {
	if m != nil {
		return m.Id
	}
	return 0
}

func (m *SyncSubmitResponse) GetStatus() int32 {
	if m != nil {
		return m.Status
	}
	return 0
}

func (m *SyncSubmitResponse) GetResult() string {
	if m != nil {
		return m.Result
	}
	return ""
}

func init() {
	proto.RegisterType((*SubmitRequest)(nil), "job.SubmitRequest")
	proto.RegisterType((*AsyncSubmitResponse)(nil), "job.AsyncSubmitResponse")
	proto.RegisterType((*SyncSubmitResponse)(nil), "job.SyncSubmitResponse")
}

func init() { proto.RegisterFile("proto/job/job.proto", fileDescriptor_4f60868841c20a44) }

var fileDescriptor_4f60868841c20a44 = []byte{
	// 244 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x84, 0x90, 0x41, 0x4b, 0x03, 0x31,
	0x10, 0x85, 0xd9, 0x4d, 0x2d, 0xec, 0x88, 0x1e, 0xa6, 0xa0, 0x41, 0x3c, 0x94, 0x05, 0x61, 0x4f,
	0x15, 0xf4, 0x28, 0x1e, 0x04, 0x2f, 0xf5, 0x20, 0x92, 0xf4, 0x0f, 0x24, 0xed, 0x20, 0x29, 0xed,
	0x66, 0x6d, 0x12, 0xa1, 0xff, 0xc0, 0x9f, 0x2d, 0x93, 0xad, 0xa8, 0xb0, 0xe8, 0x21, 0x30, 0xef,
	0x23, 0x8f, 0xf7, 0x78, 0x30, 0xe9, 0x76, 0x3e, 0xfa, 0xeb, 0xb5, 0xb7, 0xfc, 0x66, 0x59, 0xa1,
	0x58, 0x7b, 0x5b, 0x3b, 0x38, 0xd1, 0xc9, 0x6e, 0x5d, 0x54, 0xf4, 0x96, 0x28, 0x44, 0x44, 0x18,
	0x3d, 0x9b, 0x2d, 0xc9, 0x62, 0x5a, 0x34, 0x95, 0xca, 0x37, 0xb3, 0xc5, 0xbe, 0x23, 0x59, 0xf6,
	0x8c, 0x6f, 0xbc, 0x84, 0xea, 0x65, 0x93, 0x5e, 0x5d, 0xab, 0x29, 0x4a, 0x31, 0x15, 0x4d, 0xa5,
	0xbe, 0x01, 0x3b, 0x1e, 0x4d, 0x34, 0x72, 0xd4, 0x3b, 0xf8, 0xae, 0xaf, 0x60, 0xf2, 0x10, 0xf6,
	0xed, 0xf2, 0x2b, 0x2f, 0x74, 0xbe, 0x0d, 0x84, 0xa7, 0x50, 0xce, 0x57, 0x39, 0x4e, 0xa8, 0x72,
	0xbe, 0xaa, 0x17, 0x80, 0xfa, 0xdf, 0x5f, 0x78, 0x06, 0xe3, 0x10, 0x4d, 0x4c, 0x21, 0x97, 0x3a,
	0x52, 0x07, 0xc5, 0x5c, 0x51, 0x48, 0x1b, 0xee, 0xc4, 0xd1, 0x07, 0x75, 0xf3, 0x51, 0x00, 0x3c,
	0x79, 0xab, 0x69, 0xf7, 0xee, 0x96, 0x84, 0xf7, 0x70, 0xfc, 0xa3, 0x0b, 0xe2, 0x8c, 0x67, 0xf9,
	0x35, 0xc4, 0x85, 0xcc, 0x6c, 0xa0, 0x71, 0x53, 0xe0, 0x1d, 0x80, 0xfe, 0xdb, 0x7d, 0xde, 0xb3,
	0x01, 0xb3, 0x1d, 0xe7, 0xf9, 0x6f, 0x3f, 0x03, 0x00, 0x00, 0xff, 0xff, 0x69, 0x84, 0xa5, 0xd0,
	0x95, 0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// JobServiceClient is the client API for JobService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type JobServiceClient interface {
	// 异步提交job
	AsyncSubmit(ctx context.Context, opts ...grpc.CallOption) (JobService_AsyncSubmitClient, error)
	// 同步提交job
	SyncSubmit(ctx context.Context, opts ...grpc.CallOption) (JobService_SyncSubmitClient, error)
}

type jobServiceClient struct {
	cc *grpc.ClientConn
}

func NewJobServiceClient(cc *grpc.ClientConn) JobServiceClient {
	return &jobServiceClient{cc}
}

func (c *jobServiceClient) AsyncSubmit(ctx context.Context, opts ...grpc.CallOption) (JobService_AsyncSubmitClient, error) {
	stream, err := c.cc.NewStream(ctx, &_JobService_serviceDesc.Streams[0], "/job.JobService/AsyncSubmit", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobServiceAsyncSubmitClient{stream}
	return x, nil
}

type JobService_AsyncSubmitClient interface {
	Send(*SubmitRequest) error
	CloseAndRecv() (*AsyncSubmitResponse, error)
	grpc.ClientStream
}

type jobServiceAsyncSubmitClient struct {
	grpc.ClientStream
}

func (x *jobServiceAsyncSubmitClient) Send(m *SubmitRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *jobServiceAsyncSubmitClient) CloseAndRecv() (*AsyncSubmitResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(AsyncSubmitResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *jobServiceClient) SyncSubmit(ctx context.Context, opts ...grpc.CallOption) (JobService_SyncSubmitClient, error) {
	stream, err := c.cc.NewStream(ctx, &_JobService_serviceDesc.Streams[1], "/job.JobService/SyncSubmit", opts...)
	if err != nil {
		return nil, err
	}
	x := &jobServiceSyncSubmitClient{stream}
	return x, nil
}

type JobService_SyncSubmitClient interface {
	Send(*SubmitRequest) error
	CloseAndRecv() (*SyncSubmitResponse, error)
	grpc.ClientStream
}

type jobServiceSyncSubmitClient struct {
	grpc.ClientStream
}

func (x *jobServiceSyncSubmitClient) Send(m *SubmitRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *jobServiceSyncSubmitClient) CloseAndRecv() (*SyncSubmitResponse, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(SyncSubmitResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// JobServiceServer is the server API for JobService service.
type JobServiceServer interface {
	// 异步提交job
	AsyncSubmit(JobService_AsyncSubmitServer) error
	// 同步提交job
	SyncSubmit(JobService_SyncSubmitServer) error
}

// UnimplementedJobServiceServer can be embedded to have forward compatible implementations.
type UnimplementedJobServiceServer struct {
}

func (*UnimplementedJobServiceServer) AsyncSubmit(srv JobService_AsyncSubmitServer) error {
	return status.Errorf(codes.Unimplemented, "method AsyncSubmit not implemented")
}
func (*UnimplementedJobServiceServer) SyncSubmit(srv JobService_SyncSubmitServer) error {
	return status.Errorf(codes.Unimplemented, "method SyncSubmit not implemented")
}

func RegisterJobServiceServer(s *grpc.Server, srv JobServiceServer) {
	s.RegisterService(&_JobService_serviceDesc, srv)
}

func _JobService_AsyncSubmit_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(JobServiceServer).AsyncSubmit(&jobServiceAsyncSubmitServer{stream})
}

type JobService_AsyncSubmitServer interface {
	SendAndClose(*AsyncSubmitResponse) error
	Recv() (*SubmitRequest, error)
	grpc.ServerStream
}

type jobServiceAsyncSubmitServer struct {
	grpc.ServerStream
}

func (x *jobServiceAsyncSubmitServer) SendAndClose(m *AsyncSubmitResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *jobServiceAsyncSubmitServer) Recv() (*SubmitRequest, error) {
	m := new(SubmitRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _JobService_SyncSubmit_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(JobServiceServer).SyncSubmit(&jobServiceSyncSubmitServer{stream})
}

type JobService_SyncSubmitServer interface {
	SendAndClose(*SyncSubmitResponse) error
	Recv() (*SubmitRequest, error)
	grpc.ServerStream
}

type jobServiceSyncSubmitServer struct {
	grpc.ServerStream
}

func (x *jobServiceSyncSubmitServer) SendAndClose(m *SyncSubmitResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *jobServiceSyncSubmitServer) Recv() (*SubmitRequest, error) {
	m := new(SubmitRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _JobService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "job.JobService",
	HandlerType: (*JobServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "AsyncSubmit",
			Handler:       _JobService_AsyncSubmit_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "SyncSubmit",
			Handler:       _JobService_SyncSubmit_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "proto/job/job.proto",
}
