package main

import (
	"io"
	"log"
	"net"
	"sync"
	"unicode/utf8"
	"os"
	"os/signal"
	"syscall"

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

	// opretter en kanal, som operativsystemets signal bliver sendt.
	// samme mønster som klienten.
	stop := make (chan os.Signal, 1)


	// sender Ctrl + C og SIGTERM til kanal istedet for at dræbe program.
	// samme som klinten.
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop // her venter, indtil Ctrl+C trykkes
		log.Printf("[Server] event=SHUTDOWN port=5050") //logger luk til term og server log

		grpcServer.Stop() // lukker serv, så return uden fejl.
	}() // kører den


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
			// tilføj her lamport=%d på server her.
			log.Printf("[Server] event=CLIENT_CONNECT client=%s", name)

			defer s.cleanup(name, sendChannel)
	
			go sendServerMessages(stream, sendChannel)

			joinMessage := &proto.ChatMessage{
				Type:    proto.EventType_JOIN,
				Content: "User: " + name + " joined the chat",
			}
			log.Printf("[Server] event=BROADCAST_JOIN client=%s", name)
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

			// Publish logs en gang når modtager besked
			// Deliver logs en gang pr. modtager. altså 3 clients = 3 linjer
			log.Printf("[Server] event=PUBLISH client=%s content=%q", name, message)
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

	// FLYTTET: CLIENT_DISCONNECT lå før i LEAVE-casen. Her logges den uanset
	// hvordan klienten forsvinder (LEAVE, EOF eller crash).
	log.Printf("[Server] event=CLIENT_DISCONNECT client=%s", name)

	leaveMessage := &proto.ChatMessage{
		Type:    proto.EventType_LEAVE,
		Content: "User: " + name + " has left the chat",
	}
	log.Printf("[Server] event=BROADCAST_LEAVE client=%s", name)
	s.broadcast(leaveMessage)
	s.mu.Unlock()
}

func (s *ChitChatServer) broadcast(message *proto.ChatMessage) {
	for name, client := range s.Users {
		select {
		case client.sendChannel <- message:
			log.Printf("[Server] event=DELIVER to=%s content=%q", name, message.GetContent())
		default:
			log.Printf("[Server] event=BUFFER_FULL client=%s", name)
			select {
			case client.disconnectChannel <- struct{}{}:
			default:
			}
		}
	}
}
