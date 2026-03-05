package xclient

import (
	"Day6-Loadbalance"
	"context"
	"io"
	"reflect"
	"sync"
)

type Xclient struct {
	d       *MultiServerDiscovery
	mu      sync.Mutex
	opt     *Day5_http.Option
	clients map[string]*Day5_http.Client
	mode    SelectMode
}

var _ io.Closer = (*Xclient)(nil)

func NewXclient(d *MultiServerDiscovery, opt *Day5_http.Option, mode SelectMode) *Xclient {
	return &Xclient{
		d:       d,
		opt:     opt,
		clients: make(map[string]*Day5_http.Client),
		mode:    mode,
	}
}

func (xclient *Xclient) Close() error {
	xclient.mu.Lock()
	defer xclient.mu.Unlock()
	for key, client := range xclient.clients {
		_ = client.Close()
		delete(xclient.clients, key)
	}
	return nil
}
func (xclient *Xclient) FindClient(rpcAddr string) (*Day5_http.Client, error) {
	xclient.mu.Lock()
	defer xclient.mu.Unlock()
	client, ok := xclient.clients[rpcAddr]
	if ok && !client.IsAvaliable() {
		_ = client.Close()
		delete(xclient.clients, rpcAddr)
		client = nil
	}
	if client == nil {
		var err error
		client, err = Day5_http.XDial(rpcAddr, xclient.opt)
		if err != nil {
			return nil, err
		}
		xclient.clients[rpcAddr] = client
	}
	return client, nil
}

func (xclient *Xclient) call(rpcAddr string, ctx context.Context, ServieMethod string, args, reply interface{}) error {
	c, err := xclient.FindClient(rpcAddr)
	if err != nil {
		return err
	}
	return c.Call(ctx, ServieMethod, args, reply)
}

func (xclient *Xclient) Call(ctx context.Context, ServiceMethod string, args interface{}, reply interface{}) error {
	rpcAddr, err := xclient.d.Get(xclient.mode)
	if err != nil {
		return err
	}
	return xclient.call(rpcAddr, ctx, ServiceMethod, args, reply)
}
func (xclient *Xclient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xclient.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcaddr string) {
			defer wg.Done()
			var Clonereply interface{}
			if reply != nil {
				Clonereply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xclient.call(rpcaddr, ctx, serviceMethod, args, Clonereply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(Clonereply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
