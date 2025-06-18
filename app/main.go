package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handleRequest (
	request []byte,
	conn net.Conn,
) {
	responseHeader := []byte{}
	messageSize := make([]byte, 4)
	errorCode := []byte{0,0}
	apiVersion := binary.BigEndian.Uint16(request[6:8])
	if apiVersion > 4{
		errorCode = []byte{0,35}
	}

	responseHeader = append(responseHeader, request[8:12]...)
	responseHeader = append(responseHeader, errorCode...)
	responseHeader = append(responseHeader, 2)
	responseHeader = append(responseHeader, []byte{0,18}...) 
	responseHeader = append(responseHeader, []byte{0,0}...) 
	responseHeader = append(responseHeader, []byte{0,4}...)
	responseHeader = append(responseHeader, 0)
	responseHeader = append(responseHeader, []byte{0,0,0,0}...)
	responseHeader = append(responseHeader, 0)
	binary.BigEndian.PutUint32(messageSize, uint32(len(responseHeader)))
	conn.Write(messageSize)
	conn.Write(responseHeader)
}

func handleConnection (conn net.Conn){
	defer conn.Close()
	for{

	request := make([]byte, 1024)

	_,err := conn.Read(request)

	if err != nil {
		fmt.Println("Error reading buffer: ", err.Error())
		break
	}
	handleRequest(request, conn)
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
