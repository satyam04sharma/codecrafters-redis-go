package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
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
	var mutex sync.Mutex

	// Start a goroutine to handle expiration
	go handleExpiration(&store, &expirations, &mutex)

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Accepted connection:", conn.RemoteAddr().String())
		go handleConnection(conn, &store, &expirations, &mutex)
	}
}

func handleConnection(conn net.Conn, store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) {
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

		parts := strings.Split(request, "\r\n")[1:] // Remove the array length part
		cmd := strings.ToLower(parts[1])
		args := make([]string, 0)
		for i := 2; i < len(parts); i += 2 {
			args = append(args, parts[i+1])
		}

		var response string

		switch cmd {
		case "ping":
			response = "+PONG\r\n"
		case "echo":
			if len(args) > 0 {
				response = fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
			} else {
				response = "$-1\r\n"
			}
		case "set":
			response = handleSet(args, store, expirations, mutex)
		case "get":
			response = handleGet(args[0], store, expirations, mutex)
		default:
			response = "$-1\r\n"
		}

		conn.Write([]byte(response))
	}
}

func handleSet(parts []string, store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) string {
	mutex.Lock()
	defer mutex.Unlock()

	key := parts[0]
	value := parts[1]
	(*store)[key] = value

	if len(parts) >= 3 && strings.ToLower(parts[2]) == "px" {
		if expiryMs, err := strconv.Atoi(parts[3]); err == nil {
			expiration := time.Now().Add(time.Millisecond * time.Duration(expiryMs))
			(*expirations)[key] = expiration
		} else {
			return "-ERR value is not an integer or out of range\r\n"
		}
	}

	return "+OK\r\n"
}

func handleGet(key string, store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) string {
	mutex.Lock()
	defer mutex.Unlock()

	if expiration, ok := (*expirations)[key]; ok && time.Now().After(expiration) {
		delete(*store, key)
		delete(*expirations, key)
		return "$-1\r\n"
	}

	value, ok := (*store)[key]
	if !ok {
		return "$-1\r\n"
	}
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func handleExpiration(store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) {
	for {
		time.Sleep(time.Second)
		mutex.Lock()
		for key, expiration := range *expirations {
			if time.Now().After(expiration) {
				delete(*store, key)
				delete(*expirations, key)
			}
		}
		mutex.Unlock()
	}
}
