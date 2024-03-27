package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.close()
	for
		{
			connection,err := listener.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				os.Exit(1)
			}
			go handleConnection(connection)
		}
	}
	func handleConnection(connection net.Conn) {
		buff := make([]byte, 1024)
		_, err := connection.Read(buff)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
	}
	const (
		PING = "*1\r\n$4\r\nping\r\n"
	)
	var response string
	// data := string(buff)
	response = "+PONG\r\n"
	connection.Write([]byte(response))
}
