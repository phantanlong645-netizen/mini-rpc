package main

import (
	Day4_timeout "Day1-codec"
	"context"
	"log"
	"net"
	"net/http"
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

func startServer(addrCh chan string) {
	var foo Foo
	l, err := net.Listen("tcp", ":0")
	log.Printf("listening on %s", l.Addr())
	if err != nil {
		log.Fatal("listen error:", err)
	}
	if err = Day4_timeout.Register(&foo); err != nil {
		log.Fatal("register error:", err)
	}
	Day4_timeout.HandleHTTP()
	addrCh <- l.Addr().String()
	if err = http.Serve(l, nil); err != nil {
		log.Fatal("http serve error:", err)
	}
}
func call(addrCh chan string) {
	client, err := Day4_timeout.DialHTTP("tcp", <-addrCh)
	if err != nil {
		log.Fatal("dial http error:", err)
	}
	defer func() { _ = client.Close() }()
	time.Sleep(time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call(context.Background(), "Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}

func main() {
	log.SetFlags(0)
	ch := make(chan string)
	go call(ch)
	startServer(ch)
}
