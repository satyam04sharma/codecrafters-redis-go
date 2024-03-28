package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
	"errors"
	"io"
	"strings"
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
	// defer l.close()

	// conn, err := l.Accept()
	// Adding Support for multiple clients
	for { 
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		fmt.Println("Accepted connection:", conn.RemoteAddr().String())
		go Handle(conn)
	}
}
func Handle(conn net.Conn) {
	defer conn.Close()
	var res string
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println(buf)
		parts := strings.Split(string(buf), "\r\n")
		cmd := strings.ToLower(parts[2])
		if cmd == "ping" {
			res = "+PONG\r\n"
		} else if cmd == "echo" {
			res = fmt.Sprintf("$%d\r\n%s\r\n", len(parts[4]), parts[4])
		} else {
			res = "Unknown command: " + cmd
		}
		conn.Write([]byte(res))	}
}
