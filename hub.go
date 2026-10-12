package gonotify

import (
	"sync"
)

type topicSubscription struct {
	client *Client
	topic  string
}

type broadcastItem struct {
	topic   string // empty means broadcast to all
	message []byte
}

// Hub maintains active client connections and topic-based routing
type Hub struct {
	clients       map[*Client]bool
	topicClients  map[string]map[*Client]bool
	register      chan *Client
	unregister    chan *Client
	subChannel    chan topicSubscription
	unsubChannel  chan topicSubscription
	broadcastChan chan broadcastItem
	done          chan struct{}
	mu            sync.RWMutex
	config        Config
}

func newHub(config Config) *Hub {
	return &Hub{
		clients:       make(map[*Client]bool),
		topicClients:  make(map[string]map[*Client]bool),
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		subChannel:    make(chan topicSubscription, 128),
		unsubChannel:  make(chan topicSubscription, 128),
		broadcastChan: make(chan broadcastItem, 512),
		done:          make(chan struct{}),
		config:        config,
	}
}

// Run executes the core event loop
func (h *Hub) run() {
	for {
		select {
		case <-h.done:
			return

		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			if h.config.OnConnect != nil {
				go h.config.OnConnect(client)
			}

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)

				// Remove client from all its subscribed topics
				client.mu.Lock()
				for topic := range client.topics {
					if subscribers, exists := h.topicClients[topic]; exists {
						delete(subscribers, client)
						if len(subscribers) == 0 {
							delete(h.topicClients, topic)
						}
					}
				}
				client.topics = make(map[string]bool)
				client.mu.Unlock()

				close(client.send)
			}
			h.mu.Unlock()

			if h.config.OnDisconnect != nil {
				go h.config.OnDisconnect(client)
			}

		case sub := <-h.subChannel:
			h.mu.Lock()
			if _, ok := h.clients[sub.client]; ok {
				if _, exists := h.topicClients[sub.topic]; !exists {
					h.topicClients[sub.topic] = make(map[*Client]bool)
				}
				h.topicClients[sub.topic][sub.client] = true

				sub.client.mu.Lock()
				sub.client.topics[sub.topic] = true
				sub.client.mu.Unlock()
			}
			h.mu.Unlock()

		case unsub := <-h.unsubChannel:
			h.mu.Lock()
			if subscribers, exists := h.topicClients[unsub.topic]; exists {
				delete(subscribers, unsub.client)
				if len(subscribers) == 0 {
					delete(h.topicClients, unsub.topic)
				}
			}
			unsub.client.mu.Lock()
			delete(unsub.client.topics, unsub.topic)
			unsub.client.mu.Unlock()
			h.mu.Unlock()

		case item := <-h.broadcastChan:
			h.mu.RLock()
			if item.topic == "" {
				// Broadcast to all connected clients
				for client := range h.clients {
					select {
					case client.send <- item.message:
					default:
						// Buffer full: drop or skip
					}
				}
			} else {
				// Deliver only to subscribers of item.topic
				if subscribers, exists := h.topicClients[item.topic]; exists {
					for client := range subscribers {
						select {
						case client.send <- item.message:
						default:
							// Buffer full: drop or skip
						}
					}
				}
			}
			h.mu.RUnlock()
		}
	}
}

// ClientCount returns the total number of currently active connections
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// TopicCount returns subscriber count for a particular topic
func (h *Hub) TopicSubscriberCount(topic string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.topicClients[topic])
}

func (h *Hub) close() {
	close(h.done)
	h.mu.Lock()
	defer h.mu.Unlock()
	for client := range h.clients {
		close(client.send)
		client.conn.Close()
		delete(h.clients, client)
	}
}
