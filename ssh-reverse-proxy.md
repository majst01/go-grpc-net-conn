# SSH Reverse Proxy

This document describes the architecture of the SSH reverse proxy built on top of `go-grpc-net-conn`. The proxy tunnels SSH traffic through a reverse gRPC stream so the consumer (the party that connects to the real SSH target) can live behind NAT or firewalls and still initiate the control channel.

## Overview

The architecture consists of four roles:

```
┌─────────────┐     TCP/SSH      ┌─────────────────┐
│ SSH Client  │ ──────────────▶  │  SSH Server     │
│             │                  │  (Proxy Front)  │
└─────────────┘                  └────────┬────────┘
                                          │ gRPC bidi stream
                                          │ (reverse connection)
                                          ▼
┌─────────────┐     TCP/SSH      ┌─────────────────┐
│ SSH Target  │ ◀────────────── │  SSH Consumer   │
│ (real SSH   │                  │  (Proxy Back)   │
│  endpoint)  │                  │                 │
└─────────────┘                  └─────────────────┘
```

1. **SSH Client** — a standard SSH client (`ssh` CLI, `golang.org/x/crypto/ssh`, etc.) that wants to reach an SSH target.
2. **SSH Server** — the publicly reachable front-end. It accepts SSH connections on one port and gRPC connections on another.
3. **SSH Consumer** — a privately running agent that initiates a reverse gRPC stream back to the SSH Server. On the other side it connects via TCP to the real SSH target.
4. **SSH Target** — the actual SSH daemon (or custom SSH server) that the client ultimately wants to reach.

The key insight: the **consumer dials the server**, not the other way around. This is the **reverse** part — it lets the consumer live behind NAT or a firewall while still providing SSH access through a public endpoint.

## Data Flow

### Connection establishment

```
SSH Client          SSH Server          SSH Consumer         SSH Target
    │                    │                    │                   │
    │                    │    gRPC bidi       │                   │
    │                    │ ◄───────────────── │                   │
    │                    │    Stream()        │                   │
    │                    │                    │── TCP connect ──▶│
    │                    │                    │                   │
    │── TCP connect ───▶│                    │                   │
    │                    │── pipe to ────────▶│── forward ──────▶│
    │                    │    gRPC stream     │    (TCP bytes)   │
    │                    │                    │                   │
    │◀── SSH handshake ──│────────────────────│◀────────────────▶│
    │     (transparent)  │                    │                   │
```

The gRPC stream uses the proto service defined in `testproto/test.proto`:

```protobuf
service TestService {
  rpc Stream(stream Bytes) returns (stream Bytes);
}
message Bytes {
  bytes data = 1;
}
```

### Packet forwarding

Once the stream is established, each side runs two goroutines:

**SSH Server** (`ssh-server/server.go`):

- Goroutine 1: `io.Copy(grpcConn, sshConn)` — reads from the SSH client's TCP connection, writes into the gRPC stream (server-side `SendMsg`)
- Goroutine 2: `io.Copy(sshConn, grpcConn)` — reads from the gRPC stream (server-side `RecvMsg`), writes to the SSH client's TCP connection

**SSH Consumer** (`ssh-consumer/consumer.go`):

- Goroutine 1: `io.Copy(grpcConn, targetConn)` — reads from the SSH target's TCP connection, writes into the gRPC stream (client-side `SendMsg`)
- Goroutine 2: `io.Copy(targetConn, grpcConn)` — reads from the gRPC stream (client-side `RecvMsg`), writes to the SSH target's TCP connection

Both sides use `grpc_net_conn.Conn` to wrap the gRPC stream into a `net.Conn`.

## Components

### SSH Server (`ssh-server/`)

| Method                          | Description                                             |
|---------------------------------|---------------------------------------------------------|
| `New()`                         | Creates a new server with a 1-capacity consumer channel |
| `Start(ctx, sshAddr, grpcAddr)` | Starts the SSH TCP listener and gRPC listener           |
| `Stop()`                        | Gracefully stops both listeners                         |
| `SSHAddr()`                     | Returns the bound SSH listener address                  |
| `GRPCAddr()`                    | Returns the bound gRPC listener address                 |
| `Stream(stream)`                | gRPC handler — registers the consumer stream            |

The server maintains a buffered channel (`consumerCh`) of size 1. When a consumer connects via gRPC, the `Stream` handler deposits the server-side stream into this channel. When an SSH client connects, the `acceptLoop` picks up the connection, reads from `consumerCh`, and pipes the two together.

If no consumer is registered yet, the SSH client's `handleConn` blocks on `consumerCh` until a consumer arrives.

### SSH Consumer (`ssh-consumer/`)

| Method                               | Description                                                                                     |
|--------------------------------------|-------------------------------------------------------------------------------------------------|
| `New(grpcServerAddr, sshTargetAddr)` | Creates a consumer targeting the given server and SSH target                                    |
| `Run(ctx)`                           | Connects to the server via gRPC, connects to the SSH target via TCP, then pipes bidirectionally |

`Run` blocks until the stream is complete (i.e., one side closes the connection or an error occurs).

## Usage Example

```go
// Start the SSH server
srv := sshserver.New()
srv.Start(ctx, "0.0.0.0:2222", "0.0.0.0:9090")
defer srv.Stop()

// Start the consumer (in a separate process or goroutine)
consumer := sshconsumer.New(srv.GRPCAddr(), "internal-host:22")
go consumer.Run(ctx)

// Now clients can SSH to port 2222 and reach internal-host:22
// $ ssh -p 2222 user@proxy-host
```

## Limitations

- Only one SSH client connection is serviced per consumer. For concurrent clients, the consumer channel size or connection pooling must be extended.
- gRPC `CloseSend` semantics on the server-side stream are a no-op; the stream terminates when the context is cancelled or the consumer disconnects.
- `net.Conn` methods `LocalAddr`, `RemoteAddr`, and deadline setters are non-functional (see `grpc_net_conn.Conn` documentation).
