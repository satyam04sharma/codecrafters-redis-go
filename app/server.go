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
type Replication struct {
    Role            string
    MasterReplID    string
    MasterReplOffset string
    MasterHost      string  // Hostname or IP of the master
    MasterPort      string  // Port of the master
}
func main() {
	fmt.Println("Starting Redis server...")
	args := parseArgs()
	port := getPort(args)
	replication := Replication{
        Role: "master",
        MasterReplID: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        MasterReplOffset: "0",
    }
	if replica, ok := args["--replicaof"]; ok {
		replication.Role = "slave" 
		parts := strings.Split(replica, " ")
        replication.MasterHost = parts[0]
        replication.MasterPort = parts[1]
		go connectToMaster(&replication)  // Connect to master and send PING
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
		go handleConnection(conn, &replication,&store, &expirations, &mutex)
	}
}
func handleConnection(conn net.Conn,replication *Replication ,store *map[string]string, expirations *map[string]time.Time, mutex *sync.Mutex) {
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
					if replication.Role != "master"{
						response = fmt.Sprintf("$%d\r\nrole:%s\r\n",len("role")+len(replication.Role)+1,replication.Role)
					}else{
					 // Constructing individual parts of the response
					 roleStr := fmt.Sprintf("role:%s", replication.Role)
					 replidStr := fmt.Sprintf("master_replid:%s", replication.MasterReplID)
					 offsetStr := fmt.Sprintf("master_repl_offset:%s", replication.MasterReplOffset)
			 
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
func connectToMaster(replication *Replication) error {
    addr := replication.MasterHost + ":" + replication.MasterPort
    conn, err := net.Dial("tcp", addr)
    if err != nil {
        fmt.Println("Failed to connect to master at", addr, ":", err)
        return err
    }
    // defer conn.Close()

    // Send a PING command using simple string formatting
    pingCommand := "*1\r\n$4\r\nPING\r\n"
    if _, err := conn.Write([]byte(pingCommand)); err != nil {
        return err
    }

    // Read response from master
    response := make([]byte, 1024)
    n, err := conn.Read(response)
    if err != nil {
        return err
    }
    fmt.Println("Received from master:", string(response[:n]))
	config1Command := "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
	if _, err := conn.Write([]byte(config1Command)); err != nil {
        return err
    }
	fmt.Println("Received from master:", string(response[:n]))
	// time.Sleep(500 * time.Millisecond) // Allow time for the master to process the command and respond

	config2Command := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	if _, err := conn.Write([]byte(config2Command)); err != nil {
        return err
    }
	fmt.Println("Received from master:", string(response[:n]))

    return nil
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
    i := 1
    for i < len(os.Args) {
        key := os.Args[i]
        if i+1 < len(os.Args) { // Check if there is a next element
            value := os.Args[i+1]
            result[key] = value
            i += 2 // Move to the next key
        } else {
            result[key] = "" // No value for this key, assign an empty string or handle differently
            i += 1 // Proceed to the next item to avoid infinite loop
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
