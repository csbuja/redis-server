package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"strings"

	// Uncomment this block to pass the first stage

	"net"
	"os"
	"strconv"
	"time"
)

type RedisStrValue struct {
	has_expiry bool
	expiry     string
	value      string
}

// Gets the number of paramters
func handleFirstLine(conn net.Conn) int {
	data := "a"
	data_bytes := []byte(data)
	num_parameters := []byte("")
	first_read := true
	for {
		_, err := conn.Read(data_bytes)
		if err == io.EOF {
			fmt.Println("Received EOF on first line of input")
			return -2
		}
		if err != nil {
			fmt.Println("Failure to read from connection: ", err.Error())
			os.Exit(1)
		}
		if first_read {
			if string(data_bytes[0]) != "*" {
				fmt.Println("First char must be '*', not: ", string(data_bytes[0]))
				os.Exit(1)
			}
			first_read = false
			continue
		}
		if data_bytes[0] == []byte("\n")[0] {
			i, err := strconv.Atoi(string(num_parameters))
			if err != nil {
				return -1
			}
			return i
		}
		if string(data_bytes[0]) != "\r" {
			num_parameters = append(num_parameters, data_bytes[0])
		}
	}
}

func ReadNumCharsNextLine(conn net.Conn) int {
	data := "a"
	data_bytes := []byte(data)
	num_chars := []byte("")
	first_read := true
	for {
		_, err := conn.Read(data_bytes)
		if err != nil {
			fmt.Println("Failure to read from connection: ", err.Error())
			os.Exit(1)
		}
		if first_read {
			if string(data_bytes[0]) != "$" {
				fmt.Println("First char must be '$'")
				os.Exit(1)
			}
			first_read = false
			continue
		}
		if data_bytes[0] == []byte("\n")[0] {
			i, err := strconv.Atoi(string(num_chars))
			if err != nil {
				return -1
			}
			return i
		}
		if string(data_bytes[0]) != "\r" {
			num_chars = append(num_chars, data_bytes[0])
		}
	}
}

func parseCommand(conn net.Conn, num_chars int) string {
	return parseString(conn, num_chars)
}

func parseString(conn net.Conn, num_chars int) string {
	data := "a"
	data_bytes := []byte(data)
	command := []byte("")
	for i := 0; i < num_chars+2; i++ {
		_, err := conn.Read(data_bytes)
		if err != nil {
			fmt.Println("Failure to read from connection: ", err.Error())
			os.Exit(1)
		}
		if data_bytes[0] == []byte("\n")[0] {
			return strings.ToLower(string(command))
		}
		if i < num_chars {
			command = append(command, data_bytes[0])
		}
	}
	return "invalid command"
}

func writeConn(conn net.Conn, s string) {
	response_str := "+" + s + "\r\n"
	response_bytes := []byte(response_str)
	_, err := conn.Write(response_bytes)
	if err != nil {
		fmt.Println("Failure to write response: ", err.Error())
		os.Exit(1)
	}
}

func writeConnNullBulkString(conn net.Conn) {
	response_str := "$-1\r\n"
	response_bytes := []byte(response_str)
	_, err := conn.Write(response_bytes)
	if err != nil {
		fmt.Println("Failure to write response: ", err.Error())
		os.Exit(1)
	}
}

func writeConnWithMap(conn net.Conn, m map[string]string) {
	for k, v := range m {
		outStr := fmt.Sprintf("%s:%s", k, v)
		writeConn(conn, outStr)
	}
}

func writeConnWithServerState(conn net.Conn, ss ServerState) {
	response_str := ss.BulkString()
	response_bytes := []byte(response_str)
	_, err := conn.Write(response_bytes)
	if err != nil {
		fmt.Println("Failure to write response: ", err.Error())
		os.Exit(1)
	}
}

func handleConn(conn net.Conn, m map[string]RedisStrValue, serverState ServerState) {
	for {
		num_params := handleFirstLine(conn)
		fmt.Println("Read this number of params: ", strconv.Itoa(num_params))
		if num_params == -1 {
			fmt.Println("Invalid num params input")
			os.Exit(1)
		}
		if num_params == -2 {
			return
		}
		num_chars := ReadNumCharsNextLine(conn)
		fmt.Println("Read this number of chars: ", strconv.Itoa(num_chars))
		if num_chars == -1 {
			fmt.Println("Invalid num charsinput")
			os.Exit(1)
		}
		command := parseCommand(conn, num_chars)
		fmt.Println("Read this command: ", command)

		switch command {
		case "echo":
			num_chars = ReadNumCharsNextLine(conn)
			s := parseString(conn, num_chars)
			writeConn(conn, s)
		case "ping":
			writeConn(conn, "PONG")
		case "info":
			num_chars = ReadNumCharsNextLine(conn)
			fmt.Println("Read this number of chars: ", strconv.Itoa(num_chars))
			arg := parseString(conn, num_chars)
			fmt.Println("Read this arg: ", arg)
			writeConnWithServerState(conn, serverState)
		case "get":
			num_chars = ReadNumCharsNextLine(conn)
			s := parseString(conn, num_chars)
			val, ok := m[s]
			if ok {
				if !val.has_expiry {
					writeConn(conn, val.value)
					return
				}
				parsedTime, err := time.Parse(time.RFC3339Nano, val.expiry)
				if err != nil {
					fmt.Println("Error parsing ISO 8601 timestamp:", err)
					os.Exit(1)
				}
				now := time.Now().UTC()
				if now.Before(parsedTime) {
					writeConn(conn, val.value)
				} else {
					writeConnNullBulkString(conn)
				}
			} else {
				fmt.Println("Invalid get command")
				os.Exit(1)
			}
		case "replconf":
			writeConn(conn, "OK")
			num_chars = ReadNumCharsNextLine(conn)
			if num_params%2 == 0 {
				fmt.Println("Cannot have even num params")
				os.Exit(1)
			}
			for i := 0; i < ((num_params - 1) / 2); i++ {
				arg := parseString(conn, num_chars)
				switch arg {
				case "listening-port":
					num_chars = ReadNumCharsNextLine(conn)
					argvalue := parseString(conn, num_chars)
					fmt.Println(argvalue)

				case "capa":
					num_chars = ReadNumCharsNextLine(conn)
					argvalue := parseString(conn, num_chars)
					fmt.Println(argvalue)

				default:
					fmt.Println("invalid argument: ", arg)
					os.Exit(1)
				}
			}
		case "set":
			if !(num_params == 3 || num_params == 5) {
				fmt.Println("invalid # of params for set command")
				os.Exit(1)
			}
			num_chars = ReadNumCharsNextLine(conn)
			s1 := parseString(conn, num_chars)
			num_chars = ReadNumCharsNextLine(conn)
			s2 := parseString(conn, num_chars)
			if num_params == 3 {
				m[s1] = RedisStrValue{value: s2}
				writeConn(conn, "OK")
			} else {
				num_chars = ReadNumCharsNextLine(conn)
				fmt.Println("Read this number of chars: ", strconv.Itoa(num_chars))
				arg := parseString(conn, num_chars)
				fmt.Println("Read this arg: ", arg)
				switch arg {
				case "px":
					num_chars = ReadNumCharsNextLine(conn)
					fmt.Println("Read this number of chars: ", strconv.Itoa(num_chars))
					expiry_ms := parseString(conn, num_chars)
					fmt.Println("Read this arg: ", expiry_ms)
					// Get the current time
					now := time.Now().UTC()

					ms, err := strconv.Atoi(expiry_ms)
					if err != nil {
						fmt.Println("invalid expiry time: ", err)
						os.Exit(1)
					}
					futureTime := now.Add(time.Duration(ms) * time.Millisecond)

					// Serialize to ISO 8601 format
					isoTimestamp := futureTime.Format(time.RFC3339Nano)
					m[s1] = RedisStrValue{has_expiry: true, expiry: isoTimestamp, value: s2}
					fmt.Println("Saving this entry:")
					fmt.Printf("%+v\n", m[s1])
					writeConn(conn, "OK")
				default:
					fmt.Println("invalid argument: ", arg)
					os.Exit(1)
				}
			}
		default:
			fmt.Println("Invalid command")
			os.Exit(1)
		}
	}
}

func (ss *ServerState) Map() map[string]string {
	m := make(map[string]string, 0)
	m["role"] = ss.role
	m["master_replid"] = ss.master_replid
	m["master_repl_offset"] = ss.master_repl_offset
	return m
}

func (ss *ServerState) BulkString() string {
	s := ""
	s += "master_replid:" + ss.master_replid + "\r\n"
	s += "master_repl_offset:" + ss.master_repl_offset + "\r\n"
	s += "role:" + ss.role

	response_bytes := []byte(s)
	length := len(response_bytes)

	resp := fmt.Sprintf("$%s\r\n%s\r\n", strconv.Itoa(length), s)
	return resp
}

type ServerState struct {
	role               string
	master_replid      string
	master_repl_offset string
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	var port = flag.String("port", "6379", "help message for flag port")
	var replicaOf = flag.String("replicaof", "", "help message for flag replicaof")
	flag.Parse()

	role := "master"
	var serverState ServerState
	if *replicaOf != "" {
		role = "slave"
		serverState = ServerState{
			role:          role,
			master_replid: "xxx",
		}
		replicaInfo := strings.Split(*replicaOf, " ")
		var host, port string
		if len(replicaInfo) == 2 {
			host = replicaInfo[0]
			port = replicaInfo[1]
		} else {
			fmt.Println("Error: badly formatted replica data")
			os.Exit(1)
		}
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", host, port))

		if err != nil {
			fmt.Println("Error connecting:", err)
			return
		}

		defer conn.Close()

		// Message to be sent
		message := "*1\r\n$4\r\nPING\r\n"

		// Send the message to the server
		_, err = fmt.Fprintf(conn, message)
		if err != nil {
			fmt.Println("Error sending message:", err)
			return
		}

		// Read the response from the server
		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			return
		}

		fmt.Println("Server response :", response)

		listening_port := "6380"
		response_str := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n%s\r\n", listening_port)
		response_bytes := []byte(response_str)
		_, err = conn.Write(response_bytes)
		if err != nil {
			fmt.Println("Failure to write response: ", err.Error())
			os.Exit(1)
		}

		// Read the response from the server
		response, err = bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			return
		}

		fmt.Println("Server response :", response)

		response_str = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
		response_bytes = []byte(response_str)
		_, err = conn.Write(response_bytes)
		if err != nil {
			fmt.Println("Failure to write response: ", err.Error())
			os.Exit(1)
		}

		// Read the response from the server
		response, err = bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			return
		}

		// PSYNC!!

		response_str = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
		response_bytes = []byte(response_str)
		_, err = conn.Write(response_bytes)
		if err != nil {
			fmt.Println("Failure to write response: ", err.Error())
			os.Exit(1)
		}

		// Read the response from the server
		response, err = bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			return
		}
	} else {
		masterReplid := "8321b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

		serverState = ServerState{
			role:               role,
			master_replid:      masterReplid,
			master_repl_offset: "0",
		}
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *port))
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", *port)
		os.Exit(1)
	}
	m := make(map[string]RedisStrValue)
	for {
		fmt.Println("Listening for next connection ...")
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Accepted connection")
		go handleConn(conn, m, serverState)
	}

}
