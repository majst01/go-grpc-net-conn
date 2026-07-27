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
	"google.golang.org/protobuf/proto"

	"github.com/majst01/go-grpc-net-conn/testproto"
	"github.com/majst01/go-grpc-net-conn/testproto/testprotoconnect"
)

func testStreamConn(
	stream *connect.BidiStreamForClientSimple[testproto.Bytes, testproto.Bytes],
) *Conn {
	dataFieldFunc := func(msg proto.Message) *[]byte {
		return &msg.(*testproto.Bytes).Data
	}

	return &Conn{
		Stream:   NewConnectClientStream(stream),
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
) *connect.BidiStreamForClientSimple[testproto.Bytes, testproto.Bytes] {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	mux := http.NewServeMux()
	path, handler := testprotoconnect.NewTestServiceHandler(impl)
	mux.Handle(path, handler)

	p := &http.Protocols{}
	p.SetHTTP1(true)
	p.SetHTTP2(true)
	// For gRPC clients, it's convenient to support HTTP/2 without TLS.
	p.SetUnencryptedHTTP2(true)

	srv := &http.Server{
		Handler:   mux,
		Protocols: p,
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

	stream, err := client.Stream(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.CloseRequest() })

	return stream
}
