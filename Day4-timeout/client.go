package Day3_Service

import (
	"Day1-codec/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Call struct {
	Seq           uint64
	ServiceMethod string
	Args          interface{}
	Reply         interface{}
	Err           error
	DDone         chan *Call
}

type Client struct {
	Cc       codec.Codec
	Header   codec.Header
	Seq      uint64
	Pending  map[uint64]*Call
	Closing  bool
	Shutdown bool
	Opt      *Option
	sending  sync.Mutex
	mu       sync.Mutex
}

var _ io.Closer = (*Client)(nil)
var ShutDown = errors.New("connection is shut down")

func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.Closing {
		return ShutDown
	}
	client.Closing = true
	return client.Cc.Close()
}
func (call *Call) Done() {
	call.DDone <- call
}

func (client *Client) IsAvaliable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.Closing && !client.Shutdown
}

func (client *Client) RegisterCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.Closing || client.Shutdown {
		return 0, ShutDown
	}
	call.Seq = client.Seq
	client.Pending[call.Seq] = call
	client.Seq++
	return call.Seq, nil
}
func (client *Client) RemoveCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.Pending[seq]
	delete(client.Pending, seq)
	return call
}
func (client *Client) TerminateCall(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.Shutdown = true
	for _, call := range client.Pending {
		call.Err = err
		call.Done()
	}
}
func (client *Client) Send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()
	seq, err := client.RegisterCall(call)
	if err != nil {
		call.Err = err
		call.Done()
		return
	}
	client.Header.Seq = seq
	client.Header.ServiceMethod = call.ServiceMethod
	client.Header.Err = ""
	if err := client.Cc.Write(&client.Header, call.Args); err != nil {
		call := client.RemoveCall(call.Seq)
		if call != nil {
			call.Err = err
			call.Done()
		}
	}
}

func (client *Client) Receive() {
	//一个无限for循环
	var err error
	for err == nil {
		//读消息
		var header codec.Header
		if err := client.Cc.ReadHeader(&header); err != nil {
			break
		}
		call := client.RemoveCall(header.Seq)
		switch {
		case call == nil:
			err = client.Cc.ReadBody(nil)
		case header.Err != "":
			call.Err = fmt.Errorf(header.Err)
			_ = client.Cc.ReadBody(nil)
			call.Done()
		default:
			err = client.Cc.ReadBody(call.Reply)
			if err != nil {
				call.Err = errors.New("reading body " + err.Error())
			}
			call.Done()
		}
	}
	//连接坏掉了  终止
	client.TerminateCall(err)
}

func (client *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		DDone:         done,
	}
	client.Send(call)
	return call
}
func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1)).DDone
	return call.Err
}

func NewClient(opt *Option, conn net.Conn) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.Codetype]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.Codetype)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: codec error:", err)
	_:
		conn.Close()
		return nil, err
	}
	return NewClientCodec(f(conn), opt), nil

}

type NewClientFunc func(opt *Option, conn net.Conn) (*Client, error)

func NewClientCodec(Cc codec.Codec, Opt *Option) *Client {
	client := &Client{
		Cc:      Cc,
		Seq:     1,
		Pending: make(map[uint64]*Call),
		Opt:     Opt,
	}
	go client.Receive()
	return client
}
func ParseOptions(opts ...*Option) (*Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.Codetype == "" {
		opt.Codetype = DefaultOption.Codetype
	}
	return opt, nil
}
func Dial(network, addr string, opt ...*Option) (*Client, error) {
	return DialTimeout(NewClient, network, addr, opt...)
}

type clientResult struct {
	Client *Client
	Err    error
}

func DialTimeout(f NewClientFunc, network, addr string, opt ...*Option) (client *Client, err error) {
	opts, err := ParseOptions(opt...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, addr, opts.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(opts, conn)
		ch <- clientResult{Client: client, Err: err}
	}()
	if opts.ConnectTimeout == 0 {
		result := <-ch
		return result.Client, result.Err
	}
	select {
	case result := <-ch:
		return result.Client, result.Err
	case <-time.After(opts.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opts.ConnectTimeout)
	}
}
