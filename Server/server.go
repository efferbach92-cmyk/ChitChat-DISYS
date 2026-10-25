package main

import (
	"io"
	"log"
	"net"
	"sync"
	"unicode/utf8"

	proto "github.com/efferbach92-cmyk/ChitChat-DISYS/grpc"
	"google.golang.org/grpc"
)

type ChitChatServer struct {
	proto.UnimplementedChitChatServer
	Users map[string]*Client
	mu    sync.Mutex
}

type Client struct {
	sendChannel       chan *proto.ChatMessage
	disconnectChannel chan struct{}
}

type recvResult struct {
	message *proto.ClientMessage
	err     error
}

func main() {
	server := &ChitChatServer{Users: make(map[string]*Client)}

	server.start_server()
}

func (s *ChitChatServer) start_server() {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("Server did not start: %v", err)
		//%v indsætter den konkrete fejl og fejlen bliver logget
	}

	proto.RegisterChitChatServer(grpcServer, s)

	log.Printf("[Server] event=STARTUP port=5050")

	err = grpcServer.Serve(listener)

	if err != nil {
		log.Fatalf("Server did not start: %v", err)
	}
}

func (s *ChitChatServer) Chat(stream proto.ChitChat_ChatServer) error {
	var name string
	var sendChannel chan *proto.ChatMessage
	var disconnectChannel chan struct{}
	recvChannel := make(chan recvResult, 1)
	go func() {
		for {
			in, err := stream.Recv()
			select {
			case recvChannel <- recvResult{message: in, err: err}:
			case <-stream.Context().Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	for {
		var in *proto.ClientMessage
		var err error
		select {
		case result := <-recvChannel:
			in, err = result.message, result.err
		case <-disconnectChannel:
			return nil
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch in.GetType() {

		case proto.EventType_JOIN:
			name = in.GetName()
			s.mu.Lock()

			if _, exists := s.Users[name]; exists || name == "" {
				s.mu.Unlock()
				return stream.Send(&proto.ChatMessage{
					Type:    proto.EventType_ERROR,
					Content: "Username already exists or is empty",
				})
			}

			sendChannel = make(chan *proto.ChatMessage, 50)
			disconnectChannel = make(chan struct{}, 1)
			s.Users[name] = &Client{
				sendChannel:       sendChannel,
				disconnectChannel: disconnectChannel,
			}
			s.mu.Unlock()

			defer s.cleanup(name, sendChannel)

			go sendServerMessages(stream, sendChannel)

			joinMessage := &proto.ChatMessage{
				Type:    proto.EventType_JOIN,
				Content: "User: " + name + " joined the chat",
			}
			s.mu.Lock()
			s.broadcast(joinMessage)
			s.mu.Unlock()

		case proto.EventType_MESSAGE:
			if sendChannel == nil {
				return nil
			}
			message := in.GetContent()

			if !utf8.ValidString(message) || utf8.RuneCountInString(message) > 128 {
				select {
				case sendChannel <- &proto.ChatMessage{
					Type:    proto.EventType_ERROR,
					Content: "Invalid message, Maximum 128 characters.",
				}:
				default:
					return nil
				}
				continue
			}

			chatMessage := &proto.ChatMessage{
				Type:    proto.EventType_MESSAGE,
				Content: name + ": " + message,
			}
			s.mu.Lock()
			s.broadcast(chatMessage)
			s.mu.Unlock()

		case proto.EventType_LEAVE:
			return nil
		}
	}
}

// sender beskeder fra serveren til brugeren.
func sendServerMessages(stream proto.ChitChat_ChatServer, sendChannel chan *proto.ChatMessage) {
	for message := range sendChannel {
		if err := stream.Send(message); err != nil {
			return
		}
	}
}

func (s *ChitChatServer) cleanup(name string, sendChannel chan *proto.ChatMessage) {
	s.mu.Lock()
	delete(s.Users, name)
	close(sendChannel)

	leaveMessage := &proto.ChatMessage{
		Type:    proto.EventType_LEAVE,
		Content: "User: " + name + " has left the chat",
	}
	s.broadcast(leaveMessage)
	s.mu.Unlock()
}

func (s *ChitChatServer) broadcast(message *proto.ChatMessage) {
	for name, client := range s.Users {
		select {
		case client.sendChannel <- message:
		default:
			log.Printf("[Server] event=BUFFER_FULL client=%s", name)
			select {
			case client.disconnectChannel <- struct{}{}:
			default:
			}
		}
	}
}
