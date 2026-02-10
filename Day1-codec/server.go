package Jeerpc

import (
	"Day1-codec/codec"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
)

const MagicNumber = 0x3bef5c

type Option struct {
	magicNumber int
	Codetype    codec.Type
}

var DefaultOption = &Option{
	magicNumber: MagicNumber,
	Codetype:    codec.GobType,
}

type Server struct{}

func NewServer() *Server {
	return &Server{}
}

type request struct {
	header       *codec.Header
	argv, replyv reflect.Value
}

var DefaultServer = NewServer()

func (server *Server) serverConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()
	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}
	if opt.magicNumber != MagicNumber {
		log.Println("invalid magicnumber error: ")
		return
	}
	f := codec.NewCodecFuncMap[opt.Codetype]
	if f == nil {
		log.Println("invalid codetype: ", opt.Codetype)
		return
	}
	server.serveCodec(f(conn))
}

func (server *Server) serveCodec(cc codec.Codec) {}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var header codec.Header
	if err := cc.ReadHeader(&header); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("read request header error: %s \n", err)
			return nil, err
		}
	}
	return &header, nil
}

func (server *Server) readRequest(codec codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(codec)
	if err != nil {
		return nil, err
	}
	req := &request{
		header: h,
	}
	req.argv = reflect.New(reflect.TypeOf(""))
	if err := codec.ReadBody(req.argv.Interface()); err != nil {
		log.Printf("read request body error: %s \n", err)
	}
	return req, nil
}
func (server *Server) sendResponse(codec codec.Codec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := codec.Write(header, body); err != nil {
		log.Printf("send response error: %s \n", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.header, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("geerpc resp %d", req.header.Seq))
	server.sendResponse(cc, req.header, req.argv.Elem(), sending)
}
func (server *Server) Accept(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go server.serverConn(conn)
	}
}
func Accept(lis net.Listener) {
	DefaultServer.Accept(lis)
}
