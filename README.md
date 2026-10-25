# ChitChat-DISYS
 Mandatory Activity 3 - Distributed Systems, IT University of Copenhagen

 ChitChat is a distributed chat application implemented i Go usen gRPC and Protocol Buffers. 
 Multiple clients can join, exchange messages, and leave a shared chat session. 

 Requirements: 
-Go 1.27.0 (specified in go.mod)
-Dependencies are managed through go.mod and go.sum

## How to Run

Open a terminal in the project root directory. 
1. Start the server using the following command: 
    ```bash
    go run ./Server
    ```
The server listens on port 5050. 

2. Open a separate terminal for each client you want to connect.
For example, to start three clients: 

    ```bash
    go run ./Client -name Bob
    ```
    ```bash 
    go run ./Client -name Maria 
    ```
    ```bash
    go run ./Client -name Pedro
    ```
Each command should be run in a seperate terminal. Additional clients can be
connected in the same way using different names. 
The program also supports anonymous clients with the following command: 
    ```bash
    go run ./Client 
    ```
This will create a client named "anonymous" followed by a four-digit random int. 
All clients connect to localhost:5050 by default. 

## Using ChitChat

- Type a message and press Enter to publish it. 
- Messages are broadcast to connected clients.
- Type /leave to leave the chat.
- Press Ctrl+C to disconnect. 
Messages are limited to 128 characters.

## Project Structure
```text
ChitChat-DISYS/
├── Client/
│   └── client.go
├── grpc/
│   ├── Chat.proto
│   ├── Chat.pb.go
│   └── Chat_grpc.pb.go
├── Server/
│   └── server.go
├── .gitignore
├── go.mod
├── go.sum
└── README.md
```

## Logging 

Clients write events to individual log files named client-<name>.log created in 
the directory from which the client is started. 

System logs documenting the assignment requirements are included in the repository
and the report appendix. 
