package main

import (
	"fmt"
	"net"
	"os"
)

func handleConn(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err ! nil {
		fmt.Println("Error reading input: ", err.Error())
		os.Exit(1)
	}
	fmt.Println(string(buf[:n]))

	conn.Write([]byte("+PONG\r\n"))
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
