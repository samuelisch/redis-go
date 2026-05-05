package main

import (
	"fmt"
	"net"
	"os"
	"bufio"
	"strconv"
	"strings"
)

func parseCommand(conn net.Conn) ([]string, error) {
	reader := bufio.NewReader(conn)

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	argCount, _ := strconv.Atoi(strings.TrimSpace(line[1:]))

	args := make([]string, 0, argCount)
	for i := 0; i < argCount; i++ {
		reader.ReadString('\n')
		value, _ := reader.ReadString('\n')
		args = append(args, strings.TrimSpace(value))
	}
	return args, nil
}

func handleConn(conn net.Conn) {
	defer conn.Close()

	for {
		args, err := parseCommand(conn)
		if err != nil {
			return
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])))
		}
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	listener, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}
