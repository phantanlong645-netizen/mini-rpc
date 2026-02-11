package main

import (
	Jeerpc "Day1-codec"
	"Day1-codec/codec"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

func StartServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Println("Error starting server:", err)
	}
	log.Println("Server listening on", l.Addr())
	addr <- l.Addr().String()
	Jeerpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go StartServer(addr)

	conn, _ := net.Dial("tcp", <-addr)
	defer func() {
		_ = conn.Close()
	}()
	time.Sleep(time.Second)
	_ = json.NewEncoder(conn).Encode(Jeerpc.DefaultOption)
	cc := codec.NewGobCodec(conn)
	for i := 0; i < 10; i++ {
		header := &codec.Header{
			ServiceMethod: "foo sum",
			Seq:           uint64(i),
		}
		cc.Write(header, fmt.Sprintf("geerpc req %d", header.Seq))
		cc.ReadHeader(header)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}

}
