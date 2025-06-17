package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
)

func handleConnection (conn net.Conn){
	defer conn.Close()
	for{
	received := bytes.Buffer{}
	buff := make([]byte, 1024)

	_,err := conn.Read(buff)
	if err != nil {
		fmt.Println("Error reading buffer: ", err.Error())
		break
	}

	apiVersion:= buff[6:8]
	correlationId := buff[8:12]
	message_size := []byte{0,0,0,0}

	response:= append(message_size, correlationId...)

	if bytes.Compare(apiVersion, []byte{0,4}) == 1 {
		errorCode := []byte{0,35}
		response = append(response, errorCode...)
	}
	received.Write(response)
	conn.Write(response)
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
