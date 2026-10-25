package main

import (
	"io"
	"log"
	"net"
	"sync"
	"unicode/utf8"
	"os"

	proto "github.com/efferbach92-cmyk/ChitChat-DISYS/grpc"
	"google.golang.org/grpc"
)

type ChitChatServer struct {
	proto.UnimplementedChitChatServer
	Users map[string]chan *proto.ChatMessage
	mu    sync.Mutex
}

func main() {
	server := &ChitChatServer{Users: make(map[string]chan *proto.ChatMessage)}

	server.start_server()
}

func (s *ChitChatServer) start_server() {
	 // logs skal ligge i repo og appendix. Det er et krav.
	 // O_APPend bevarer gamle logs ved restart.
	f, err := os.OpenFile("server.log", os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0644)
	if err !=nil {
		log.Fatalf("dang can't open log file: %v",err)
	}
	defer f.Close()

	// MuliWriter sender alt log.print klade til term og fil.
	// hvilket betyder at vi ikke skal ændre de eksisterende log linjer.
	log.SetOutput(io.MultiWriter(os.Stdout, f))
	
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

	for {
		in, err := stream.Recv()
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

			sendChannel = make(chan *proto.ChatMessage, 10)
			s.Users[name] = sendChannel
			s.mu.Unlock()
			// tilføj her lamport=%d på server her.
			log.Printf("[Server] event=CLIENT_CONNECT client=%s", name)
			go sendServerMessages(stream, sendChannel)

			joinMessage := &proto.ChatMessage{
				Type:    proto.EventType_JOIN,
				Content: "User: " + name + " joined the chat",
			}
			s.mu.Lock()
			for _, sendChannel := range s.Users {
				sendChannel <- joinMessage
			}
			s.mu.Unlock()

		case proto.EventType_MESSAGE:

			message := in.GetContent()

			if !utf8.ValidString(message) || utf8.RuneCountInString(message) > 128 {
				sendChannel <- &proto.ChatMessage{
					Type:    proto.EventType_ERROR,
					Content: "Invalid message. Maximum 128 characters.",
				}
				continue
			}

			chatMessage := &proto.ChatMessage{
				Type:    proto.EventType_MESSAGE,
				Content: name + ": " + message,
			}

			// Publish logs en gang når modtager besked
			// Deliver logs en gang pr. modtager. altså 3 clients = 3 linjer
			log.Printf("[Server] event=PUBLISH client=%s content=%q", name, message)
			s.mu.Lock()
			for to, sendChannel := range s.Users {
				sendChannel <- chatMessage
				log.Printf("[Server] event=DELIVER to=%s from=%s content=%q", to, name, message)
			}
			s.mu.Unlock()

		case proto.EventType_LEAVE:

			s.mu.Lock()
			delete(s.Users, name)
			s.mu.Unlock()
			close(sendChannel)
			log.Printf("[Server] event=CLIENT_DISCONNECT client=%s", name)
			

			leaveMessage := &proto.ChatMessage{
				Type:    proto.EventType_LEAVE,
				Content: "User: " + name + " has left the chat",
			}

			s.mu.Lock()
			for _, sendChannel := range s.Users {
				sendChannel <- leaveMessage
			}
			s.mu.Unlock()
			return nil

		default:

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
