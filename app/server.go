package main

import (
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

func handleConn(conn net.Conn, m map[string]RedisStrValue) {
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
		case "get":
			num_chars = ReadNumCharsNextLine(conn)
			s := parseString(conn, num_chars)
			val, ok := m[s]
			if ok {

				if !val.has_expiry {
					writeConn(conn, val.value)
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

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	var port = flag.String("port", "6379", "help message for flag port")
	flag.Parse()

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
		go handleConn(conn, m)
	}

}
