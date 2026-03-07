package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type GeeRegister struct {
	Timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	addr  string
	start time.Time
}

const (
	defaultPath    = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

func NewGeeRegister(timeout time.Duration) *GeeRegister {
	return &GeeRegister{
		Timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultGeeRegister = NewGeeRegister(defaultTimeout)

// 新增注册一个server
func (re *GeeRegister) PutServer(addr string) {
	re.mu.Lock()
	defer re.mu.Unlock()
	server := re.servers[addr]
	if server == nil {
		server = &ServerItem{addr: addr, start: time.Now()}
		re.servers[addr] = server
	} else {
		server.start = time.Now()
	}
}

// 找到还存活的servers，然后返回他们的地址
func (re *GeeRegister) aliveServers() []string {
	re.mu.Lock()
	defer re.mu.Unlock()
	var alives []string
	for addr, s := range re.servers {
		if re.Timeout == 0 || s.start.Add(re.Timeout).After(time.Now()) {
			alives = append(alives, addr)
		} else {
			delete(re.servers, addr)
		}
	}
	sort.Strings(alives)
	return alives
}
func (re *GeeRegister) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		w.Header().Set("X-Geerpc-Servers", strings.Join(re.aliveServers(), ","))
	case "POST":
		addr := r.Header.Get("X-Geerpc-Servers")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		re.PutServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
func (re *GeeRegister) HandleHTTP(registerPath string) {
	http.Handle(registerPath, re)
	log.Println("rpc registry path:", registerPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - 1*time.Minute
	}
	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpclient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Servers", addr)
	if _, err := httpclient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
