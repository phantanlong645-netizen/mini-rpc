package Jeerpc

import (
	"Day1-codec/codec"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"sync"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber int
	Codetype    codec.Type
}

var DefaultOption = &Option{
	MagicNumber: MagicNumber,
	Codetype:    codec.GobType,
}

type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

type request struct {
	header       *codec.Header
	argv, replyv reflect.Value
	service      *Service
	mtype        *MethodType
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
	if opt.MagicNumber != MagicNumber {
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

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

func (server *Server) serveCodec(cc codec.Codec) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)

	for {
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				break
			}
			req.header.Err = err.Error()
			server.SendResponse(cc, req.header, invalidRequest, sending)
		}
		wg.Add(1)
		go server.handleRequest(cc, req, sending, wg)
		continue
	}
	wg.Wait()
	_ = cc.Close()

}

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

func (server *Server) FindService(MethodName string) (svr *Service, mtype *MethodType, err error) {
	dot := strings.LastIndex(MethodName, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + MethodName)
		return
	}
	methodName := MethodName[dot+1:]
	servicename := MethodName[:dot]
	svci, ok := server.serviceMap.Load(servicename)
	if !ok {
		err = errors.New("rpc server: can't find service " + servicename)
		return
	}
	svr = svci.(*Service)
	mtype = svr.methods[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

func (server *Server) readRequest(codec codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(codec)
	if err != nil {
		return nil, err
	}
	req := &request{
		header: h,
	}
	svr, mtype, err := server.FindService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	req.argv = mtype.NewArgv()
	req.replyv = mtype.NewReplyv()
	req.service = svr
	req.mtype = mtype
	argv := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argv = req.argv.Addr().Interface()
	}
	if err := codec.ReadBody(argv); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}
func (server *Server) SendResponse(codec codec.Codec, header *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := codec.Write(header, body); err != nil {
		log.Printf("send response error: %s \n", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Println(req.header, req.argv.Elem())
	svr := req.service
	err := svr.Call(req.mtype, req.argv, req.replyv)
	if err != nil {
		req.header.Err = err.Error()
		server.SendResponse(cc, req.header, invalidRequest, sending)
		return
	}
	server.SendResponse(cc, req.header, req.replyv.Interface(), sending)
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

func (server *Server) Register(rcvr interface{}) error {
	s := NewService(rcvr)
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }
