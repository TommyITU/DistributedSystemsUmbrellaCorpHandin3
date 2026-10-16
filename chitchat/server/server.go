package main

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "chitchat/grpc"
)

type Server struct {
	pb.UnimplementedChatServiceServer
	chatUsers []*pb.ChatUser
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
	fmt.Println("Participant " + chat_user.Name + " joined Chit Chat at logical time L")
	fmt.Println("nb users:" + strconv.Itoa(len(s.chatUsers)))
	fmt.Println()

	return &chat_user, nil
}

func (s *Server) LeaveChat(ctx context.Context, req *pb.ChatUser) (*pb.Nothing, error) {
	for i, u := range s.chatUsers {
		if u.Name == req.Name {
			s.chatUsers = append(s.chatUsers[:i], s.chatUsers[i+1:]...)
			fmt.Println("Participant " + req.Name + " left Chit Chat at logical time L")
			fmt.Println("nb users:" + strconv.Itoa(len(s.chatUsers)))
			break
		}
	}

	fmt.Println()
	return &pb.Nothing{}, nil
}
