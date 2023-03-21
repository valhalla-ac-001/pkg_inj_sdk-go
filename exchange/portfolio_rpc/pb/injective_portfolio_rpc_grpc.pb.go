// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package injective_portfolio_rpcpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// InjectivePortfolioRPCClient is the client API for InjectivePortfolioRPC service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type InjectivePortfolioRPCClient interface {
	// Provide the account's portfolio
	AccountPortfolio(ctx context.Context, in *AccountPortfolioRequest, opts ...grpc.CallOption) (*AccountPortfolioResponse, error)
	// Stream the account's portfolio
	StreamAccountPortfolio(ctx context.Context, in *StreamAccountPortfolioRequest, opts ...grpc.CallOption) (InjectivePortfolioRPC_StreamAccountPortfolioClient, error)
}

type injectivePortfolioRPCClient struct {
	cc grpc.ClientConnInterface
}

func NewInjectivePortfolioRPCClient(cc grpc.ClientConnInterface) InjectivePortfolioRPCClient {
	return &injectivePortfolioRPCClient{cc}
}

func (c *injectivePortfolioRPCClient) AccountPortfolio(ctx context.Context, in *AccountPortfolioRequest, opts ...grpc.CallOption) (*AccountPortfolioResponse, error) {
	out := new(AccountPortfolioResponse)
	err := c.cc.Invoke(ctx, "/injective_portfolio_rpc.InjectivePortfolioRPC/AccountPortfolio", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *injectivePortfolioRPCClient) StreamAccountPortfolio(ctx context.Context, in *StreamAccountPortfolioRequest, opts ...grpc.CallOption) (InjectivePortfolioRPC_StreamAccountPortfolioClient, error) {
	stream, err := c.cc.NewStream(ctx, &InjectivePortfolioRPC_ServiceDesc.Streams[0], "/injective_portfolio_rpc.InjectivePortfolioRPC/StreamAccountPortfolio", opts...)
	if err != nil {
		return nil, err
	}
	x := &injectivePortfolioRPCStreamAccountPortfolioClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type InjectivePortfolioRPC_StreamAccountPortfolioClient interface {
	Recv() (*StreamAccountPortfolioResponse, error)
	grpc.ClientStream
}

type injectivePortfolioRPCStreamAccountPortfolioClient struct {
	grpc.ClientStream
}

func (x *injectivePortfolioRPCStreamAccountPortfolioClient) Recv() (*StreamAccountPortfolioResponse, error) {
	m := new(StreamAccountPortfolioResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// InjectivePortfolioRPCServer is the server API for InjectivePortfolioRPC service.
// All implementations must embed UnimplementedInjectivePortfolioRPCServer
// for forward compatibility
type InjectivePortfolioRPCServer interface {
	// Provide the account's portfolio
	AccountPortfolio(context.Context, *AccountPortfolioRequest) (*AccountPortfolioResponse, error)
	// Stream the account's portfolio
	StreamAccountPortfolio(*StreamAccountPortfolioRequest, InjectivePortfolioRPC_StreamAccountPortfolioServer) error
	mustEmbedUnimplementedInjectivePortfolioRPCServer()
}

// UnimplementedInjectivePortfolioRPCServer must be embedded to have forward compatible implementations.
type UnimplementedInjectivePortfolioRPCServer struct {
}

func (UnimplementedInjectivePortfolioRPCServer) AccountPortfolio(context.Context, *AccountPortfolioRequest) (*AccountPortfolioResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AccountPortfolio not implemented")
}
func (UnimplementedInjectivePortfolioRPCServer) StreamAccountPortfolio(*StreamAccountPortfolioRequest, InjectivePortfolioRPC_StreamAccountPortfolioServer) error {
	return status.Errorf(codes.Unimplemented, "method StreamAccountPortfolio not implemented")
}
func (UnimplementedInjectivePortfolioRPCServer) mustEmbedUnimplementedInjectivePortfolioRPCServer() {}

// UnsafeInjectivePortfolioRPCServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to InjectivePortfolioRPCServer will
// result in compilation errors.
type UnsafeInjectivePortfolioRPCServer interface {
	mustEmbedUnimplementedInjectivePortfolioRPCServer()
}

func RegisterInjectivePortfolioRPCServer(s grpc.ServiceRegistrar, srv InjectivePortfolioRPCServer) {
	s.RegisterService(&InjectivePortfolioRPC_ServiceDesc, srv)
}

func _InjectivePortfolioRPC_AccountPortfolio_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AccountPortfolioRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(InjectivePortfolioRPCServer).AccountPortfolio(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/injective_portfolio_rpc.InjectivePortfolioRPC/AccountPortfolio",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(InjectivePortfolioRPCServer).AccountPortfolio(ctx, req.(*AccountPortfolioRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _InjectivePortfolioRPC_StreamAccountPortfolio_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StreamAccountPortfolioRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(InjectivePortfolioRPCServer).StreamAccountPortfolio(m, &injectivePortfolioRPCStreamAccountPortfolioServer{stream})
}

type InjectivePortfolioRPC_StreamAccountPortfolioServer interface {
	Send(*StreamAccountPortfolioResponse) error
	grpc.ServerStream
}

type injectivePortfolioRPCStreamAccountPortfolioServer struct {
	grpc.ServerStream
}

func (x *injectivePortfolioRPCStreamAccountPortfolioServer) Send(m *StreamAccountPortfolioResponse) error {
	return x.ServerStream.SendMsg(m)
}

// InjectivePortfolioRPC_ServiceDesc is the grpc.ServiceDesc for InjectivePortfolioRPC service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var InjectivePortfolioRPC_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "injective_portfolio_rpc.InjectivePortfolioRPC",
	HandlerType: (*InjectivePortfolioRPCServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "AccountPortfolio",
			Handler:    _InjectivePortfolioRPC_AccountPortfolio_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StreamAccountPortfolio",
			Handler:       _InjectivePortfolioRPC_StreamAccountPortfolio_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "injective_portfolio_rpc.proto",
}
