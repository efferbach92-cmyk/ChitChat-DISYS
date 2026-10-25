# ChitChat-DISYS
Mandatory Activity 3 - Distributed Systems, IT University of Copenhagen

ChitChat is a distributed chat application implemented in Go using gRPC and Protocol Buffers.
Multiple clients can join, exchange messages, and leave a shared chat session.

Requirements:
- Go 1.27.0 (specified in go.mod)
- Dependencies are managed through go.mod and go.sum

## How to Run

Open a terminal in the project root directory.

1. Start the server using the following command:
    ```bash
    go run ./server
    ```
    The server listens on port 5050. Stop it with Ctrl+C, which logs a SHUTDOWN event.

2. Open a separate terminal for each client you want to connect.
    For example, to start three clients:
    ```bash
    go run ./client -name Bob
    ```
    ```bash
    go run ./client -name Maria
    ```
    ```bash
    go run ./client -name Pedro
    ```

    

Each command should be run in a separate terminal. Additional clients can be
connected in the same way using different names. A name that is already in use
is rejected by the server. 

The program also supports anonymous clients with the following command:
```bash
go run ./client
```
This will create a client named "anonymous" followed by a four-digit random int.
All clients connect to localhost:5050 by default.

## Using ChitChat

- Type a message and press Enter to publish it.
- Messages are broadcast to all connected clients.
- Messages are shown as `[L=5] Bob: hello`, where `L` is the Lamport timestamp of the message.
- Type `/leave` to leave the chat.
- Press Ctrl+C to disconnect. The client still tells the server that it leaves, so the other participants are notified.

Messages are limited to 128 characters.

## Project Structure
```text
ChitChat-DISYS/
├── client/
│   └── client.go
├── grpc/
│   ├── Chat.proto
│   ├── Chat.pb.go
│   └── Chat_grpc.pb.go
├── server/
│   ├── server.go
│   └── lamport.go
├── server.log
├── client-Maria.log
├── client-Pedro.log
├── client-Bob.log
├── .gitignore
├── go.mod
├── go.sum
└── readme.md
```

## Logging

The server logs events to the terminal and to `server.log`.
Clients write events to individual log files named `client-<name>.log`.
Both are created in the directory from which the program is started.

The system logs documenting the assignment requirements are included in the repository root (server.log, client-Maria.log, client-Pedro.log and client-Bob.log) and in the report appendix. They come from a single demo run with three clients.

Log files are appended to on every run. To keep the demo logs unchanged, run the
programs from a copy of the repository or delete the log files first.
