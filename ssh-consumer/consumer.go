package sshconsumer

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"golang.org/x/net/http2"

	grpc_net_conn "github.com/majst01/go-grpc-net-conn"
	"github.com/majst01/go-grpc-net-conn/testproto"
	"github.com/majst01/go-grpc-net-conn/testproto/testprotoconnect"
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
	client := testprotoconnect.NewTestServiceClient(
		&http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}},
		"http://"+c.grpcServerAddr,
	)

	stream := client.Stream(ctx)

	targetConn, err := net.Dial("tcp", c.sshTargetAddr)
	if err != nil {
		return fmt.Errorf("dial ssh target: %w", err)
	}
	defer targetConn.Close()

	grpcConn := &grpc_net_conn.Conn{
		Stream:   grpc_net_conn.NewConnectClientStream[testproto.Bytes, testproto.Bytes](stream),
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode:   grpc_net_conn.SimpleEncoder(grpc_net_conn.BytesField),
		Decode:   grpc_net_conn.SimpleDecoder(grpc_net_conn.BytesField),
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
