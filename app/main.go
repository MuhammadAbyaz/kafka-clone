package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func toResponse (
	request []byte,
) []byte{
	response := []byte{}
	responseHeader := []byte{}
	messageSize := make([]byte, 4)
	errorCode := uint16(0)
	apiVersion := binary.BigEndian.Uint16(request[6:8])
	apiKey := uint16(18)
	correlationId := binary.BigEndian.Uint32(request[8:12])
	if apiVersion > 4{
		errorCode = uint16(35)
	}
	responseHeader = append(responseHeader, messageSize...)
	responseHeader = append(responseHeader, byte(apiKey), byte(apiVersion), byte(correlationId), byte((errorCode)))
	binary.BigEndian.PutUint32(messageSize, uint32(len(responseHeader)))
	response = append(response, messageSize...)
	response = append(response, responseHeader...)
	return response
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
	response := toResponse(request)
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
