package gonotify

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxMessageReadSize = 2048

// Client represents a single connected WebSocket subscriber
type Client struct {
	ID     string
	hub    *Hub
	conn   *websocket.Conn
	send   chan []byte
	topics map[string]bool
	mu     sync.RWMutex
	Meta   map[string]interface{}
}

// NewClient initializes a client instance
func newClient(id string, hub *Hub, conn *websocket.Conn, bufferSize int) *Client {
	if bufferSize <= 0 {
		bufferSize = 256
	}
	return &Client{
		ID:     id,
		hub:    hub,
		conn:   conn,
		send:   make(chan []byte, bufferSize),
		topics: make(map[string]bool),
		Meta:   make(map[string]interface{}),
	}
}

// Subscribe subscribes the client to a specific topic
func (c *Client) Subscribe(topic string) {
	if topic == "" {
		return
	}
	c.hub.subChannel <- topicSubscription{client: c, topic: topic}
}

// Unsubscribe unsubscribes the client from a specific topic
func (c *Client) Unsubscribe(topic string) {
	if topic == "" {
		return
	}
	c.hub.unsubChannel <- topicSubscription{client: c, topic: topic}
}

// HasTopic checks if the client is subscribed to the topic
func (c *Client) HasTopic(topic string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.topics[topic]
}

// Close explicitly disconnects the client
func (c *Client) Close() error {
	return c.conn.Close()
}

// readPump handles incoming messages & subscription commands from the socket
func (c *Client) readPump(config Config) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageReadSize)
	c.conn.SetReadDeadline(time.Now().Add(config.PongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(config.PongWait))
		return nil
	})

	for {
		_, messageBytes, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[gonotify] unexpected socket close for client %s: %v", c.ID, err)
			}
			break
		}

		// Try parsing client control command (JSON)
		var cmd ClientCommand
		if err := json.Unmarshal(messageBytes, &cmd); err == nil && cmd.Action != "" {
			switch cmd.Action {
			case "subscribe":
				c.Subscribe(cmd.Topic)
			case "unsubscribe":
				c.Unsubscribe(cmd.Topic)
			case "ping":
				c.send <- []byte(`{"event":"pong"}`)
			}
		}
	}
}

// writePump pumps messages from the hub to the websocket connection
func (c *Client) writePump(config Config) {
	ticker := time.NewTicker(config.PingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(config.WriteWait))
			if !ok {
				// Hub closed the channel
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Add queued messages to the current websocket frame
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(config.WriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
