package main

import (
	"sync"

	"github.com/google/uuid"
)

type Hub struct {
	mu      sync.RWMutex
	clients map[uuid.UUID]*Client
}

func NewHub() *Hub {
	return &Hub{clients: make(map[uuid.UUID]*Client)}
}

func (h *Hub) Register(c *Client) (peers []ClientInfo, existing []*Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	peers = make([]ClientInfo, 0, len(h.clients))
	existing = make([]*Client, 0, len(h.clients))
	for _, other := range h.clients {
		peers = append(peers, other.PublicInfo())
		existing = append(existing, other)
	}

	h.clients[c.ID] = c
	return peers, existing
}

func (h *Hub) FindClient(clientID uuid.UUID) (*Client, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	client, ok := h.clients[clientID]
	return client, ok
}

func (h *Hub) Others(except uuid.UUID) []*Client {
	h.mu.RLock()
	defer h.mu.RUnlock()

	others := make([]*Client, 0, len(h.clients))
	for id, c := range h.clients {
		if id == except {
			continue
		}
		others = append(others, c)
	}
	return others
}

func (h *Hub) Unregister(c *Client) []*Client {
	h.mu.Lock()
	if _, ok := h.clients[c.ID]; !ok {
		h.mu.Unlock()
		c.close()
		return nil
	}

	delete(h.clients, c.ID)
	remaining := make([]*Client, 0, len(h.clients))
	for _, other := range h.clients {
		remaining = append(remaining, other)
	}
	h.mu.Unlock()

	c.close()
	return remaining
}
