package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	chatUsers   []*pb.ChatUser
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

func (s *Server) JoinChat(ctx context.Context, desiredName *pb.StringValue) (*pb.ChatUser, error) {
	for _, u := range s.chatUsers {
		if u.Name == desiredName.Value {
			return nil, status.Error(codes.AlreadyExists, "User exists")
		}
	}

	chat_user := pb.ChatUser{
		Name: desiredName.Value,
	}
	s.chatUsers = append(s.chatUsers, &chat_user)
	fmt.Println("Participant '" + chat_user.Name + "' joined Chit Chat at logical time L")
	fmt.Println("nb users:" + strconv.Itoa(len(s.chatUsers)))
	fmt.Println()

	return &chat_user, nil
}

func (s *Server) LeaveChat(ctx context.Context, req *pb.ChatUser) (*pb.Nothing, error) {
	for i, u := range s.chatUsers {
		if u.Name == req.Name {
			s.chatUsers = append(s.chatUsers[:i], s.chatUsers[i+1:]...)
			fmt.Println("Participant '" + req.Name + "' left Chit Chat at logical time L")
			fmt.Println("nb users:" + strconv.Itoa(len(s.chatUsers)))
			break
		}
	}

	fmt.Println()
	return &pb.Nothing{}, nil
}

func (s *Server) ChatStream(chatStream pb.ChatService_ChatStreamServer) error {
	s.mu.Lock()
	s.chatStreams = append(s.chatStreams, chatStream)
	s.mu.Unlock()

	fmt.Println("Client connected")

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, st := range s.chatStreams {
			if st == chatStream {
				s.chatStreams = append(s.chatStreams[:i], s.chatStreams[i+1:]...)
				break
			}
		}
		fmt.Println("Client disconnected")
	}()

	for {
		msg, err := chatStream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		fmt.Printf("%s: %s\n", msg.Message, msg.Timestamp)

		s.mu.Lock()
		for _, clientStream := range s.chatStreams {
			clientStream.Send(msg)
		}
		s.mu.Unlock()
	}
}
