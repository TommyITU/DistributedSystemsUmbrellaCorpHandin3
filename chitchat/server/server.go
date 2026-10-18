package main

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	chatStreams []pb.ChatService_ChatStreamServer
	mu          sync.Mutex
}

func main() {
	fmt.Println("Server started")

	server := &Server{}

	listener, _ := net.Listen("tcp", "localhost:5050")
	grpcServer := grpc.NewServer()

	pb.RegisterChatServiceServer(grpcServer, server)

	grpcServer.Serve(listener)
}

func (s *Server) ChatStream(chatStream pb.ChatService_ChatStreamServer) error {
	//get userName from metaData
	md, _ := metadata.FromIncomingContext(chatStream.Context())
	userName := md.Get("username")[0]

	s.mu.Lock()
	s.chatStreams = append(s.chatStreams, chatStream)
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, st := range s.chatStreams {
			if st == chatStream {
				s.chatStreams = append(s.chatStreams[:i], s.chatStreams[i+1:]...)
				break
			}
		}
		fmt.Println("Client '" + userName + "' disconnected")
		fmt.Println("nb users:" + strconv.Itoa(len(s.chatStreams)))
		msg := &pb.ChatMessage{
			Username:  "Server",
			Message:   "Participant '" + userName + "' left Chit Chat",
			Timestamp: "L",
		}
		sendMessageToAll(s, msg)
	}()

	fmt.Println("Client '" + userName + "' connected")
	fmt.Println("nb users:" + strconv.Itoa(len(s.chatStreams)))
	msg := &pb.ChatMessage{
		Username:  "Server",
		Message:   "Participant '" + userName + "' joined Chit Chat",
		Timestamp: "L",
	}
	sendMessageToAll(s, msg)

	for {
		msg, err := chatStream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		fmt.Println(msg.Username + ": " + msg.Message + " at logical time " + msg.Timestamp)

		s.mu.Lock()
		for _, clientStream := range s.chatStreams {
			clientStream.Send(msg)
		}
		s.mu.Unlock()
	}
}

func sendMessageToAll(s *Server, msg *pb.ChatMessage) {
	fmt.Println("sendMessageToAll... nb users:" + strconv.Itoa(len(s.chatStreams)))
	s.mu.Lock()
	for _, clientStream := range s.chatStreams {
		clientStream.Send(msg)
	}
	s.mu.Unlock()
}
