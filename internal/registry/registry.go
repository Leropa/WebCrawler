package registry

import "sync"

type VisitedRegistry struct {
	mu      sync.Mutex
	history map[string]bool
}

func New() *VisitedRegistry {
	return &VisitedRegistry{
		history: make(map[string]bool),
	}
}

func (v *VisitedRegistry) Visit(url string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.history[url] {
		return false
	}
	v.history[url] = true
	return true
}

func (v *VisitedRegistry) Count() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.history)
}
