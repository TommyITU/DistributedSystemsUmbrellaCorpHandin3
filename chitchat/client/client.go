package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "chitchat/grpc"
)

func main() {

	conn, _ := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(
		insecure.NewCredentials()))

	defer conn.Close()

	client := pb.NewChatServiceClient(conn)

	userName := pb.StringValue{
		Value: "",
	}
	var chatUser *pb.ChatUser
	var err error
	for {
		fmt.Print("Enter username: ")
		fmt.Scanln(&userName.Value)

		chatUser, err = client.JoinChat(context.Background(), &userName)
		if err == nil {
			fmt.Println("User created:" + chatUser.Name)
			break
		}

		fmt.Println(err)
	}

	//Handle application exit
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-exitChannel
		client.LeaveChat(context.Background(), chatUser)
		fmt.Println("You left.. Name:" + chatUser.Name)
		os.Exit(0)
	}()

	//connect to chatStream
	chatStream, err := client.ChatStream(context.Background())
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			msg, err := chatStream.Recv()
			if err != nil {
				fmt.Println("Disconnected")
				return
			}

			fmt.Println(msg.Username + ": " + msg.Message + " @ " + msg.Timestamp)
		}
	}()

	for {
		var text string

		//fmt.Print("> ")
		fmt.Scanln(&text)

		msg := &pb.ChatMessage{
			Username:  chatUser.Name,
			Message:   text,
			Timestamp: "0",
		}

		err := chatStream.Send(msg)

		/*err := chatStream.Send(&pb.ChatMessage{
			Message:   text,
			Timestamp: "temp",
		})*/

		if err != nil {
			fmt.Println(err)
			break
		}
	}

}
