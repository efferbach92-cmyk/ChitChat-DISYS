package main

import (
	"context"
	"flag"
	"log"

	proto "github.com/efferbach92-cmyk/ChitChat-DISYS/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	name := flag.String("name", "anonymous", "your name in the chat")
	addr := flag.String("addr", "localhost:5050", "server address")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Client %s] event=ERROR msg=%v", *name, err)
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)

	stream, err := client.Chat(context.Background())
	if err != nil {
		log.Fatalf("[Client %s] event=ERROR msg=%v", *name, err)
	}

	err = stream.Send(&proto.ClientMessage{Type: proto.EventType_JOIN, Name: *name})
	if err != nil {
		log.Fatalf("[Client %s] event=ERROR msg=%v", *name, err)
	}
	log.Printf("[Client %s] event=JOIN addr=%s", *name, *addr)

	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Printf("[Client %s] event=DISCONNECT msg=%v", *name, err)
			return
		}
		log.Printf("[Client %s] event=DELIVER type=%s lamport=%d content=%q",
			*name, msg.GetType(), msg.GetLogicalTimestamp(), msg.GetContent())
	}
}
