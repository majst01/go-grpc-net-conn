package grpc_net_conn

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"

	"github.com/majst01/go-grpc-net-conn/testproto"
	"github.com/majst01/go-grpc-net-conn/testproto/testprotoconnect"
)

func testStreamConn(
	stream *connect.BidiStreamForClient[testproto.Bytes, testproto.Bytes],
) *Conn {
	dataFieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*testproto.Bytes).Data
	}

	return &Conn{
		Stream:   NewConnectClientStream[testproto.Bytes, testproto.Bytes](stream),
		Request:  &testproto.Bytes{},
		Response: &testproto.Bytes{},
		Encode:   SimpleEncoder(dataFieldFunc),
		Decode:   SimpleDecoder(dataFieldFunc),
	}
}

// testStreamClient returns a fully connected stream client.
func testStreamClient(
	t *testing.T,
	impl testprotoconnect.TestServiceHandler,
) *connect.BidiStreamForClient[testproto.Bytes, testproto.Bytes] {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	path, handler := testprotoconnect.NewTestServiceHandler(impl)
	mux.Handle(path, handler)

	srv := &http.Server{
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(func() { _ = srv.Close() })

	client := testprotoconnect.NewTestServiceClient(
		&http.Client{Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		}},
		"http://"+l.Addr().String(),
	)

	stream := client.Stream(context.Background())
	t.Cleanup(func() { _ = stream.CloseRequest() })

	return stream
}
