package gonotify

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// Engine is the primary entry point for Gonotify
type Engine struct {
	config   Config
	hub      *Hub
	upgrader websocket.Upgrader
}

// New creates and initializes a new Gonotify notification engine
func New(configs ...Config) *Engine {
	cfg := defaultConfig()
	if len(configs) > 0 {
		userCfg := configs[0]
		if userCfg.PingPeriod > 0 {
			cfg.PingPeriod = userCfg.PingPeriod
		}
		if userCfg.PongWait > 0 {
			cfg.PongWait = userCfg.PongWait
		}
		if userCfg.WriteWait > 0 {
			cfg.WriteWait = userCfg.WriteWait
		}
		if userCfg.MessageBufferSize > 0 {
			cfg.MessageBufferSize = userCfg.MessageBufferSize
		}
		if userCfg.CheckOrigin != nil {
			cfg.CheckOrigin = userCfg.CheckOrigin
		}
		if userCfg.Authenticate != nil {
			cfg.Authenticate = userCfg.Authenticate
		}
		if userCfg.OnConnect != nil {
			cfg.OnConnect = userCfg.OnConnect
		}
		if userCfg.OnDisconnect != nil {
			cfg.OnDisconnect = userCfg.OnDisconnect
		}
	}

	hub := newHub(cfg)
	go hub.run()

	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     cfg.CheckOrigin,
	}

	return &Engine{
		config:   cfg,
		hub:      hub,
		upgrader: upgrader,
	}
}

// Handler returns standard http.HandlerFunc that upgrades requests to WebSocket
func (e *Engine) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Optional authentication hook
		if e.config.Authenticate != nil {
			if !e.config.Authenticate(r) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
		}

		conn, err := e.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		// Generate random client ID or retrieve from query param
		clientID := r.URL.Query().Get("client_id")
		if clientID == "" {
			clientID = generateRandomID()
		}

		client := newClient(clientID, e.hub, conn, e.config.MessageBufferSize)

		// Auto-subscribe to topics passed via query params (e.g. ?topic=orders&topic=billing)
		queryTopics := r.URL.Query()["topic"]
		for _, t := range queryTopics {
			client.topics[t] = true
		}

		// Register client with hub
		e.hub.register <- client

		// Ensure topics from query param are added to topic index
		for _, t := range queryTopics {
			e.hub.subChannel <- topicSubscription{client: client, topic: t}
		}

		go client.writePump(e.config)
		go client.readPump(e.config)
	}
}

// PublishHandler provides an HTTP REST endpoint (POST /publish or POST /api/notify)
// so that any external backend (Node.js, Python, PHP, etc.) can send notifications via HTTP POST
func (e *Engine) PublishHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var msg Message
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		if msg.Timestamp == 0 {
			msg.Timestamp = time.Now().Unix()
		}

		var err error
		if msg.Topic != "" {
			err = e.Publish(msg.Topic, msg)
		} else {
			err = e.Broadcast(msg)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	}
}

// Publish sends a notification message to all subscribers of a specific topic
func (e *Engine) Publish(topic string, msg Message) error {
	if topic == "" {
		return fmt.Errorf("gonotify: topic cannot be empty for Publish")
	}
	msg.Topic = topic
	bytes, err := msg.ToBytes()
	if err != nil {
		return err
	}
	e.hub.broadcastChan <- broadcastItem{topic: topic, message: bytes}
	return nil
}

// Broadcast sends a notification message to ALL connected clients regardless of topic
func (e *Engine) Broadcast(msg Message) error {
	bytes, err := msg.ToBytes()
	if err != nil {
		return err
	}
	e.hub.broadcastChan <- broadcastItem{topic: "", message: bytes}
	return nil
}

// ClientCount returns the total number of connected clients
func (e *Engine) ClientCount() int {
	return e.hub.ClientCount()
}

// TopicSubscriberCount returns the number of clients subscribed to a given topic
func (e *Engine) TopicSubscriberCount(topic string) int {
	return e.hub.TopicSubscriberCount(topic)
}

// Close gracefully closes all connections and terminates background routines
func (e *Engine) Close() {
	e.hub.close()
}

func generateRandomID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		return "client_unknown"
	}
	return hex.EncodeToString(bytes)
}
