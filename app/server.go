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
	var store = make(map[string]string)
	for { 
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}
		fmt.Println("Accepted connection:", conn.RemoteAddr().String())
		go Handle(conn, store)
	}
}
func Handle(conn net.Conn, store map[string]string) {
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
		fmt.Println(string(buf))
		parts := strings.Split(string(buf), "\r\n")
		cmd := strings.ToLower(parts[2])
		switch cmd {
			case "ping":
				res = "+PONG\r\n"
			case "echo":
				res = fmt.Sprintf("$%d\r\n%s\r\n", len(parts[4]), parts[4])
			case "set":
				res = handleSet(parts[4], parts[6], store)
			case "get":
				res, _ = handleGet(parts[4], store)
			default:
				res = "Unknown command: " + cmd
		}

		conn.Write([]byte(res))	}
	}
func handleSet(k, v string, m map[string]string) string {
	m[k] = v
	return "+OK\r\n"
}
func handleGet(k string, m map[string]string) (string, error) {
	v, ok := m[k]
	if !ok {
		return "$-1\r\n", fmt.Errorf("unknown key: %s", k)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v), nil
}
