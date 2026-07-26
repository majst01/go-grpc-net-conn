package sshconsumer

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	grpc_net_conn "github.com/majst01/go-grpc-net-conn"
	"github.com/majst01/go-grpc-net-conn/testproto"
)

type Consumer struct {
	sshTargetAddr string
	grpcServerAddr string
}

func New(grpcServerAddr, sshTargetAddr string) *Consumer {
	return &Consumer{
		grpcServerAddr: grpcServerAddr,
		sshTargetAddr:  sshTargetAddr,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	conn, err := grpc.DialContext(ctx, c.grpcServerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("dial grpc server: %w", err)
	}
	defer conn.Close()

	client := testproto.NewTestServiceClient(conn)
	stream, err := client.Stream(ctx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	targetConn, err := net.Dial("tcp", c.sshTargetAddr)
	if err != nil {
		return fmt.Errorf("dial ssh target: %w", err)
	}
	defer targetConn.Close()

	fieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*testproto.Bytes).Data
	}

	grpcConn := &grpc_net_conn.Conn{
		Stream:   stream,
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode:   grpc_net_conn.SimpleEncoder(fieldFunc),
		Decode:   grpc_net_conn.SimpleDecoder(fieldFunc),
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(grpcConn, targetConn)
		_ = grpcConn.Close()
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(targetConn, grpcConn)
	}()

	wg.Wait()
	return nil
}
