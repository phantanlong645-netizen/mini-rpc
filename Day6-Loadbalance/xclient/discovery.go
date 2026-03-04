package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect SelectMode = iota
	RoundRobinSelect
)

type Discovery interface {
	Refresh() error
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

var _ Discovery = (*MultiServerDiscovery)(nil)

type MultiServerDiscovery struct {
	index   int
	servers []string
	mu      sync.RWMutex
	r       *rand.Rand
}

func NewMultiServerDiscovery(servers []string) *MultiServerDiscovery {
	d := &MultiServerDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

func (d *MultiServerDiscovery) Refresh() error {
	return nil
}
func (d *MultiServerDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}
func (d *MultiServerDiscovery) Get(mode SelectMode) (string, error) {
	var length int
	d.mu.Lock()
	defer d.mu.Unlock()
	length = len(d.servers)
	if length == 0 {
		return "", errors.New("no servers")
	}
	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(length)], nil
	case RoundRobinSelect:
		s := d.servers[d.index%length]
		d.index = (d.index + 1) % length
		return s, nil
	default:
		return "", errors.New("invalid select mode")
	}

}
func (d *MultiServerDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
