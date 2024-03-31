package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
	"strconv"
)

func main() {
	fmt.Println("Starting Redis server...")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err)
		os.Exit(1)
	}
	defer l.Close()

	var store = make(map[string]string)
	var expirations = make(map[string]time.Time)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Accepted connection:", conn.RemoteAddr().String())
		go handleConnection(conn, store, expirations)
	}
}

func handleConnection(conn net.Conn, store map[string]string, expirations map[string]time.Time) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf[:])
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("Error reading from connection:", err)
			}
			break
		}

		request := strings.TrimSpace(string(buf))
		fmt.Println("Received command:", request)
		parts := strings.Split(request, "\r\n")
		cmd := strings.ToLower(parts[2])
		var response string

		switch cmd {
		case "ping":
			response = "+PONG\r\n"
		case "echo":
			response = fmt.Sprintf("$%d\r\n%s\r\n", len(parts[4]), parts[4])
		case "set":
			if len(parts) < 7 {
				response = "-ERR wrong number of arguments for 'set' command\r\n"
			} else {
				response = handleSet(parts[4], parts[6], store, expirations)
			}
		case "get":
			if len(parts) < 5 {
				response = "-ERR wrong number of arguments for 'get' command\r\n"
			} else {
				response, _ = handleGet(parts[4], store, expirations)
			}
		default:
			response = "-ERR unknown command '" + cmd + "'\r\n"
		}

		conn.Write([]byte(response))
	}
}

func handleSet(parts []string, store map[string]string, expirations map[string]time.Time) string {
	key := parts[4]
	value := parts[6]
	store[key] = value

	// Check for expiration time (PX option)
	if len(parts) >= 9 && strings.ToLower(parts[8]) == "px" {
		if expiryMs, err := strconv.Atoi(parts[9]); err == nil {
			expiration := time.Now().Add(time.Millisecond * time.Duration(expiryMs))
			expirations[key] = expiration
		} else {
			return "-ERR value is not an integer or out of range\r\n"
		}
	}

	return "+OK\r\n"
}


func handleGet(key string, store map[string]string, expirations map[string]time.Time) (string, error) {
	if expiration, ok := expirations[key]; ok && time.Now().After(expiration) {
		delete(store, key)
		delete(expirations, key)
		return "$-1\r\n", fmt.Errorf("key expired: %s", key)
	}

	value, ok := store[key]
	if !ok {
		return "$-1\r\n", fmt.Errorf("unknown key: %s", key)
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value), nil
}
