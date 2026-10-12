package gonotify

import (
	"encoding/json"
	"time"
)

// Message represents a notification payload transmitted over WebSocket.
type Message struct {
	ID        string      `json:"id,omitempty"`
	Topic     string      `json:"topic,omitempty"`     // Channel or topic name (e.g. "orders", "user:42")
	Event     string      `json:"event"`               // Event identifier (e.g. "NEW_ORDER", "ALERT")
	Title     string      `json:"title"`               // User-facing notification title
	Body      string      `json:"body"`                // User-facing notification body
	Data      interface{} `json:"data,omitempty"`      // Custom metadata or entity payload
	Timestamp int64       `json:"timestamp"`           // Unix epoch timestamp (seconds)
}

// ClientCommand represents an incoming control message from client (e.g. subscribe/unsubscribe)
type ClientCommand struct {
	Action string `json:"action"` // "subscribe", "unsubscribe", "ping"
	Topic  string `json:"topic"`  // Target topic
}

// NewMessage constructs a standard Message with the current Unix timestamp
func NewMessage(topic, event, title, body string, data interface{}) Message {
	return Message{
		Topic:     topic,
		Event:     event,
		Title:     title,
		Body:      body,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}
}

// ToBytes converts the message into JSON byte slice
func (m Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}
