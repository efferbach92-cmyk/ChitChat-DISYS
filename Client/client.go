package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	proto "github.com/efferbach92-cmyk/ChitChat-DISYS/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LamportClock struct {
	mu   sync.Mutex
	time int64
}

func (c *LamportClock) Tick() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time++
	return c.time
}

func (c *LamportClock) Update(received int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.time {
		c.time = received
	}
	c.time++
	return c.time
}

func main() {
	name := flag.String("name", "anonymous", "your name in the chat")
	addr := flag.String("addr", "localhost:5050", "server address")
	flag.Parse()

	logFile, err := os.OpenFile("client-"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}
	defer logFile.Close()
	logger := log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

	clock := &LamportClock{}

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
	logger.Printf("[Client %s] event=CONNECT addr=%s", *name, *addr)

	t := clock.Tick()
	if err := stream.Send(&proto.ClientMessage{Type: proto.EventType_JOIN, Name: *name}); err != nil {
		log.Fatalf("[Client %s] event=ERROR msg=%v", *name, err)
	}
	logger.Printf("[Client %s] event=JOIN local=%d", *name, t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				logger.Printf("[Client %s] event=ERROR msg=%v", *name, err)
				fmt.Println("Lost connection to the server.")
				return
			}
			local := clock.Update(msg.GetLogicalTimestamp())
			fmt.Printf("[L=%d] %s\n", msg.GetLogicalTimestamp(), msg.GetContent())
			logger.Printf("[Client %s] event=DELIVER type=%s lamport=%d local=%d content=%q",
				*name, msg.GetType(), msg.GetLogicalTimestamp(), local, msg.GetContent())
		}
	}()

	lines := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("Joined as %s. Type a message and press Enter, or /leave to quit.\n", *name)

	leave := func() {
		t := clock.Tick()
		stream.Send(&proto.ClientMessage{Type: proto.EventType_LEAVE, Name: *name})
		stream.CloseSend()
		logger.Printf("[Client %s] event=LEAVE local=%d", *name, t)
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		logger.Printf("[Client %s] event=DISCONNECT", *name)
		fmt.Println("You left Chit Chat.")
	}

	for {
		select {
		case line, ok := <-lines:
			if !ok || strings.TrimSpace(line) == "/leave" {
				leave()
				return
			}
			if strings.TrimSpace(line) == "" {
				continue
			}
			t := clock.Tick()
			if err := stream.Send(&proto.ClientMessage{Type: proto.EventType_MESSAGE, Name: *name, Content: line}); err != nil {
				logger.Printf("[Client %s] event=ERROR msg=%v", *name, err)
				return
			}
			logger.Printf("[Client %s] event=PUBLISH local=%d content=%q", *name, t, line)
		case <-stop:
			leave()
			return
		case <-done:
			logger.Printf("[Client %s] event=DISCONNECT", *name)
			return
		}
	}
}
