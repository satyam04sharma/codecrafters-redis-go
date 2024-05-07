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
	args := parseArgs()
	port := getPort(args)
	var replication = make(map[string]string)
	replication["role"] = "master"
	replication["master_replid"] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	replication["master_repl_offset"] = "0"
	if _, ok := args["--replicaof"]; ok {
		replication["role"] = "slave"
	}
	l, err := net.Listen("tcp", "0.0.0.0:"+port)
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
	fmt.Println("Starting handling Expiration")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		fmt.Println("Accepted connection:", conn.RemoteAddr().String())
		go handleConnection(conn, replication,&store, &expirations, &mutex)
	}
}
func handleConnection(conn net.Conn, replication map[string]string,store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) {
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
		if len(parts) < 3 {
			conn.Write([]byte("$-1\r\n"))
			continue
		}
		cmd := strings.ToLower(parts[2])
		args := make([]string, 0)
		for i := 3; i < len(parts); i += 2 {
			if i+1 < len(parts) {
				args = append(args, parts[i+1])
			}
		}
		var response string
		fmt.Println("Recieved command final:", cmd)
		switch cmd {
		case "ping":
			response = "+PONG\r\n"
		case "echo":
				response = fmt.Sprintf("$%d\r\n%s\r\n", len(args[0]), args[0])
		case "set":
			response = handleSet(args, store, expirations, mutex)
		case "get":
				response = handleGet(args[0], store, expirations, mutex)
		case "info":
				if (len(args)>0 && strings.ToLower(args[0]) == "replication"){
					// fmt.Println(replication,"value is")
					if replication["role"] != "master"{
						response = fmt.Sprintf("$%d\r\nrole:%s\r\n",len("role")+len(replication["role"])+1,replication["role"])
					}else{
					 // Constructing individual parts of the response
					 roleStr := fmt.Sprintf("role:%s", replication["role"])
					 replidStr := fmt.Sprintf("master_replid:%s", replication["master_replid"])
					 offsetStr := fmt.Sprintf("master_repl_offset:%s", replication["master_repl_offset"])
			 
					 // Calculate lengths individually
					 roleLen := len(roleStr)
					 replidLen := len(replidStr)
					 offsetLen := len(offsetStr)
			 
					 // Format the bulk string response with proper length calculation
					 totalLength := roleLen + replidLen + offsetLen + 4 // +4 for CRLF after each part
					 response = fmt.Sprintf("$%d\r\n%s\r\n%s\r\n%s\r\n", totalLength, roleStr, replidStr, offsetStr)
							 
					}
						
				}else{
					response = "$-1\r\n"
				}
		default:
			response = "$-1\r\n"
		}

		conn.Write([]byte(response))
	}
}


func getPort(args map[string]string) string {
	value, ok := args["--port"]
	if !ok {
		return "6379"
	} else {
		return value
	}
}
func parseArgs() map[string]string {
	result := make(map[string]string)
	for i := 1; i < len(os.Args); i += 2 {
		if os.Args[i] == "--replicaof" && i+2<len(os.Args){
			result[os.Args[i]] = os.Args[i+1]+":"+os.Args[i+2]
			i+=1
		}else{
			result[os.Args[i]] = os.Args[i+1]
		}
	}
	return result
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
