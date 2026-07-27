package sshserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"connectrpc.com/connect"

	grpc_net_conn "github.com/majst01/go-grpc-net-conn"
	"github.com/majst01/go-grpc-net-conn/testproto"
	"github.com/majst01/go-grpc-net-conn/testproto/testprotoconnect"
)

type Server struct {
	sshListener  net.Listener
	httpServer   *http.Server
	consumerCh   chan *connect.BidiStream[testproto.Bytes, testproto.Bytes]
	sshAddr      string
	grpcListener net.Listener
	once         sync.Once
	done         chan struct{}
}

func New() *Server {
	return &Server{
		consumerCh: make(chan *connect.BidiStream[testproto.Bytes, testproto.Bytes], 1),
		done:       make(chan struct{}),
	}
}

func (s *Server) Start(ctx context.Context, sshAddr, grpcAddr string) error {
	var err error

	s.grpcListener, err = net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}

	mux := http.NewServeMux()
	path, handler := testprotoconnect.NewTestServiceHandler(s)
	mux.Handle(path, handler)

	p := &http.Protocols{}
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	// For gRPC clients, it's convenient to support HTTP/2 without TLS.
	p.SetUnencryptedHTTP2(true)

	s.httpServer = &http.Server{
		Handler:   mux,
		Protocols: p,
	}
	go func() {
		_ = s.httpServer.Serve(s.grpcListener)
	}()

	s.sshListener, err = net.Listen("tcp", sshAddr)
	if err != nil {
		return fmt.Errorf("ssh listen: %w", err)
	}

	s.sshAddr = s.sshListener.Addr().String()

	go s.acceptLoop(ctx)

	return nil
}

func (s *Server) Stream(ctx context.Context, stream *connect.BidiStream[testproto.Bytes, testproto.Bytes]) error {
	select {
	case s.consumerCh <- stream:
		<-ctx.Done()
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
	defer func() {
		_ = conn.Close()
	}()

	select {
	case consumerStream := <-s.consumerCh:
		s.pipe(ctx, conn, consumerStream)
	case <-ctx.Done():
	case <-s.done:
	}
}

func (s *Server) pipe(_ context.Context, sshConn net.Conn, consumerStream *connect.BidiStream[testproto.Bytes, testproto.Bytes]) {
	grpcConn := &grpc_net_conn.Conn{
		Stream:   grpc_net_conn.NewConnectServerStream(consumerStream),
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode:   grpc_net_conn.SimpleEncoder(grpc_net_conn.BytesField),
		Decode:   grpc_net_conn.SimpleDecoder(grpc_net_conn.BytesField),
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
		if s.httpServer != nil {
			_ = s.httpServer.Close()
		}
		if s.sshListener != nil {
			_ = s.sshListener.Close()
		}
	})
}
