package grpc_net_conn

import (
	"sync/atomic"

	"connectrpc.com/connect"
)

// ConnectClientStream wraps a connectrpc BidiStreamForClient to satisfy
// the Stream and CloseSender interfaces.
type ConnectClientStream[Req, Res any] struct {
	stream   *connect.BidiStreamForClientSimple[Req, Res]
	initCall atomic.Bool
}

func NewConnectClientStream[Req, Res any](s *connect.BidiStreamForClientSimple[Req, Res]) *ConnectClientStream[Req, Res] {
	return &ConnectClientStream[Req, Res]{stream: s}
}

func (s *ConnectClientStream[Req, Res]) SendMsg(m any) error {
	return s.stream.Send(m.(*Req))
}

func (s *ConnectClientStream[Req, Res]) RecvMsg(m any) error {
	if s.initCall.CompareAndSwap(false, true) {
		// Initiate the HTTP request by sending headers only.
		if err := s.stream.Send(nil); err != nil {
			return err
		}
	}
	msg, err := s.stream.Receive()
	if err != nil {
		return err
	}
	*(m.(*Res)) = *msg
	return nil
}

func (s *ConnectClientStream[Req, Res]) CloseSend() error {
	return s.stream.CloseRequest()
}

// ConnectServerStream wraps a connectrpc BidiStream to satisfy the Stream interface.
type ConnectServerStream[Req, Res any] struct {
	stream *connect.BidiStream[Req, Res]
}

func NewConnectServerStream[Req, Res any](s *connect.BidiStream[Req, Res]) *ConnectServerStream[Req, Res] {
	return &ConnectServerStream[Req, Res]{stream: s}
}

func (s *ConnectServerStream[Req, Res]) SendMsg(m any) error {
	return s.stream.Send(m.(*Res))
}

func (s *ConnectServerStream[Req, Res]) RecvMsg(m any) error {
	msg, err := s.stream.Receive()
	if err != nil {
		return err
	}
	*(m.(*Req)) = *msg
	return nil
}
