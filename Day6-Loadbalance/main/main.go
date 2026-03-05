package main

import (
	Day4_timeout "Day6-Loadbalance"
	"Day6-Loadbalance/xclient"
	"context"
	"log"
	"net"
	"sync"
	"time"
)

type Foo int

type Args struct {
	Num1, Num2 int
}

func (foo *Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func (f *Foo) Sleep(args Args, reply *int) error {
	time.Sleep(time.Duration(args.Num1) * time.Second)
	*reply = args.Num1 + args.Num2
	return nil
}

func foo(xc *xclient.Xclient, ctx context.Context, typ, servicemethod string, args *Args) {
	var reply int
	var err error
	switch typ {
	case "broadcast":
		err = xc.Broadcast(ctx, servicemethod, args, &reply)
	case "call":
		err = xc.Call(ctx, servicemethod, args, &reply)
	}
	if err != nil {
		log.Printf("%s %s error: %v", typ, servicemethod, err)
	} else {
		log.Printf("%s %s success: %d + %d = %d", typ, servicemethod, args.Num1, args.Num2, reply)
	}

}

func call(addr1, addr2 string) {
	d := xclient.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := xclient.NewXclient(d, nil, xclient.RandomSelect)
	defer func() {
		_ = xc.Close()
	}()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "call", "Foo.Sum", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}
func BroadCast(addr1, addr2 string) {
	d := xclient.NewMultiServerDiscovery([]string{"tcp@" + addr1, "tcp@" + addr2})
	xc := xclient.NewXclient(d, nil, xclient.RandomSelect)
	defer func() {
		_ = xc.Close()
	}()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			foo(xc, context.Background(), "broadcast", "Foo.Sum", &Args{Num1: i, Num2: i * i})
			ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
			foo(xc, ctx, "broadcast", "Foo.Sleep", &Args{Num1: i, Num2: i * i})
		}(i)
	}
	wg.Wait()
}

func startServer(addrCh chan string) {
	var foo Foo
	l, _ := net.Listen("tcp", ":0")
	server := Day4_timeout.NewServer()
	_ = server.Register(&foo)
	addrCh <- l.Addr().String()
	server.Accept(l)
}

func main() {
	log.SetFlags(0)
	ch1 := make(chan string)
	ch2 := make(chan string)
	go startServer(ch1)
	go startServer(ch2)
	addr1 := <-ch1
	addr2 := <-ch2
	call(addr1, addr2)

}
