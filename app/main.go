package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

func handleRequest(request []byte, conn net.Conn) {
	responseHeader := []byte{}
	messageSize := make([]byte, 4)
	correlationId := request[8:12]
	responseHeader = append(responseHeader, correlationId...)

	if binary.BigEndian.Uint16(request[6:]) == 4 {
		apiKey := binary.BigEndian.Uint16(request[4:])

		responseHeader = append(responseHeader, []byte{0,0}...) // error code
		responseHeader = append(responseHeader, 0x03) // array length for api keys
		responseHeader = append(responseHeader, []byte{0,byte(apiKey)}...) // first api key
		responseHeader = append(responseHeader, []byte{0,4}...) // min ver
		responseHeader = append(responseHeader, []byte{0,4}...) // max ver
		responseHeader = append(responseHeader, 0) // tag buffer

		responseHeader = append(responseHeader, []byte{0,75}...) // api key
		responseHeader = append(responseHeader, []byte{0,0}...) // min ver
		responseHeader = append(responseHeader, []byte{0,0}...) // max ver
		responseHeader = append(responseHeader, 0) // tag buffer

		responseHeader = append(responseHeader, []byte{0,0,0,0}...) // throttle
		responseHeader = append(responseHeader, 0) // tag buffer
		binary.BigEndian.PutUint32(messageSize, uint32(len(responseHeader)))

	} else if binary.BigEndian.Uint16(request[6:]) == 0 {
		responseHeader = append(responseHeader, 0x00)                    // tag buffer
		responseHeader = append(responseHeader, []byte{0, 0, 0, 0}...)   // throttle
		
		offset := 12
		offset++
		
		clientIdLen := int(request[offset])
		offset++ // skip length byte
		offset += clientIdLen // skip client ID
		offset++ // skip tag buffer after client ID
		
		// Parse topic array (compact array)  
		topicArrayLength := int(request[offset]) - 1
		offset++
		
		responseHeader = append(responseHeader, byte(topicArrayLength+1)) // topic array length
		
		for range topicArrayLength {
			topicLen := int(request[offset]) - 1  // topic name length
			offset++
			topicName := request[offset : offset+topicLen]
			offset += topicLen
			
			responseHeader = append(responseHeader, []byte{0, 3}...)           // error code
			responseHeader = append(responseHeader, byte(topicLen+1))          // topic name length
			responseHeader = append(responseHeader, topicName...)              // topic name
			responseHeader = append(responseHeader, make([]byte, 16)...)       // topic ID
			responseHeader = append(responseHeader, 0)                         // is internal
			responseHeader = append(responseHeader, 0x01)                      // partition array length
			responseHeader = append(responseHeader, []byte{0, 0, 0, 0}...)     // topic auth op
			responseHeader = append(responseHeader, 0)                         // tag buffer
		}
		
		responseHeader = append(responseHeader, 0xff)  // next cursor
		responseHeader = append(responseHeader, 0)     // tag buffer
		binary.BigEndian.PutUint32(messageSize, uint32(len(responseHeader)))
	} else{
		responseHeader = append(responseHeader, []byte{0,35}...)
	}
	
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
