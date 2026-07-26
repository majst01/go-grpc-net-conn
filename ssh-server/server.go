package sshserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	grpc_net_conn "github.com/majst01/go-grpc-net-conn"
	"github.com/majst01/go-grpc-net-conn/testproto"
)

type Server struct {
	sshListener  net.Listener
	grpcServer   *grpc.Server
	consumerCh   chan testproto.TestService_StreamServer
	sshAddr      string
	grpcListener net.Listener
	once         sync.Once
	done         chan struct{}
}

func New() *Server {
	return &Server{
		consumerCh: make(chan testproto.TestService_StreamServer, 1),
		done:       make(chan struct{}),
	}
}

func (s *Server) Start(ctx context.Context, sshAddr, grpcAddr string) error {
	var err error

	s.grpcListener, err = net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	s.grpcServer = grpc.NewServer()
	testproto.RegisterTestServiceServer(s.grpcServer, s)
	go func() {
		_ = s.grpcServer.Serve(s.grpcListener)
	}()

	s.sshListener, err = net.Listen("tcp", sshAddr)
	if err != nil {
		return fmt.Errorf("ssh listen: %w", err)
	}

	s.sshAddr = s.sshListener.Addr().String()

	go s.acceptLoop(ctx)

	return nil
}

func (s *Server) Stream(stream testproto.TestService_StreamServer) error {
	select {
	case s.consumerCh <- stream:
		<-stream.Context().Done()
		return nil
	case <-s.done:
		return nil
	}
}

func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.sshListener.Accept()
		if err != nil {
			return
		}

		go s.handleConn(ctx, conn)
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	select {
	case consumerStream := <-s.consumerCh:
		s.pipe(ctx, conn, consumerStream)
	case <-ctx.Done():
	case <-s.done:
	}
}

func (s *Server) pipe(ctx context.Context, sshConn net.Conn, consumerStream testproto.TestService_StreamServer) {
	fieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*testproto.Bytes).Data
	}

	grpcConn := &grpc_net_conn.Conn{
		Stream:   consumerStream,
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode:   grpc_net_conn.SimpleEncoder(fieldFunc),
		Decode:   grpc_net_conn.SimpleDecoder(fieldFunc),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(grpcConn, sshConn)
		_ = grpcConn.Close()
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(sshConn, grpcConn)
	}()

	wg.Wait()
}

func (s *Server) SSHAddr() string {
	return s.sshAddr
}

func (s *Server) GRPCAddr() string {
	if s.grpcListener != nil {
		return s.grpcListener.Addr().String()
	}
	return ""
}

func (s *Server) Stop() {
	s.once.Do(func() {
		close(s.done)
		if s.grpcServer != nil {
			s.grpcServer.Stop()
		}
		if s.sshListener != nil {
			_ = s.sshListener.Close()
		}
	})
}
