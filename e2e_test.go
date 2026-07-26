package grpc_net_conn_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	sshconsumer "github.com/majst01/go-grpc-net-conn/ssh-consumer"
	sshserver "github.com/majst01/go-grpc-net-conn/ssh-server"
)

func generateSigner(t *testing.T) ssh.Signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)
	return signer
}

func TestEndToEnd_rawEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	echoLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer echoLn.Close()

	go func() {
		for {
			conn, err := echoLn.Accept()
			if err != nil {
				return
			}
			go func() {
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					line := scanner.Text()
					conn.Write([]byte("echo: " + line + "\n"))
				}
				conn.Close()
			}()
		}
	}()

	sshSrv := sshserver.New()
	err = sshSrv.Start(ctx, "127.0.0.1:0", "127.0.0.1:0")
	require.NoError(t, err)
	defer sshSrv.Stop()

	go func() {
		consumer := sshconsumer.New(sshSrv.GRPCAddr(), echoLn.Addr().String())
		_ = consumer.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	proxyConn, err := net.Dial("tcp", sshSrv.SSHAddr())
	require.NoError(t, err)
	defer proxyConn.Close()

	_, err = proxyConn.Write([]byte("hello\n"))
	require.NoError(t, err)

	buf := make([]byte, 1024)
	n, err := proxyConn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, "echo: hello\n", string(buf[:n]))
}

func TestEndToEnd_ssh(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	targetSigner := generateSigner(t)
	clientSigner := generateSigner(t)

	targetLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer targetLn.Close()

	targetConfig := &ssh.ServerConfig{
		NoClientAuth: true,
	}
	targetConfig.AddHostKey(targetSigner)

	go func() {
		for {
			conn, err := targetLn.Accept()
			if err != nil {
				return
			}
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, targetConfig)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for newChan := range chans {
					ch, reqs, err := newChan.Accept()
					if err != nil {
						return
					}
					go func() {
						defer ch.Close()
						for req := range reqs {
							if req.Type == "exec" {
								_ = req.Reply(true, nil)
								ch.Write([]byte("hello from target\n"))
								ch.SendRequest("exit-status", false, ssh.Marshal(struct{ ExitStatus uint32 }{0}))
								return
							}
							if req.WantReply {
								_ = req.Reply(false, nil)
							}
						}
					}()
				}
			}()
		}
	}()

	sshSrv := sshserver.New()
	err = sshSrv.Start(ctx, "127.0.0.1:0", "127.0.0.1:0")
	require.NoError(t, err)
	defer sshSrv.Stop()

	go func() {
		consumer := sshconsumer.New(sshSrv.GRPCAddr(), targetLn.Addr().String())
		_ = consumer.Run(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	clientConfig := &ssh.ClientConfig{
		User:            "testuser",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(clientSigner)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         20 * time.Second,
	}

	clientConn, err := ssh.Dial("tcp", sshSrv.SSHAddr(), clientConfig)
	require.NoError(t, err)
	defer clientConn.Close()

	session, err := clientConn.NewSession()
	require.NoError(t, err)
	defer session.Close()

	type result struct {
		output []byte
		err    error
	}
	done := make(chan result, 1)
	go func() {
		out, err := session.CombinedOutput("echo hello")
		done <- result{out, err}
	}()

	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Contains(t, string(r.output), "hello from target")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for session output")
	}
}
