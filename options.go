package gonotify

import (
	"net/http"
	"time"
)

// Config defines configuration parameters for Gonotify engine
type Config struct {
	// PingPeriod specifies how frequently ping heartbeats are sent to clients. Default: 30s.
	PingPeriod time.Duration

	// PongWait specifies the maximum duration to wait for a pong response from the client. Default: 60s.
	PongWait time.Duration

	// WriteWait specifies the maximum duration to wait when writing a message to network socket. Default: 10s.
	WriteWait time.Duration

	// MessageBufferSize defines the channel buffer size for outgoing messages per client. Default: 256.
	MessageBufferSize int

	// CheckOrigin determines whether the incoming cross-origin request is permitted. Default: allows all origins (*).
	CheckOrigin func(r *http.Request) bool

	// Authenticate is an optional callback to validate incoming HTTP handshake requests (e.g. JWT, API key).
	// Returns true if authenticated, false to reject the connection with HTTP 401 Unauthorized.
	Authenticate func(r *http.Request) bool

	// OnConnect is triggered when a client successfully connects and completes the WebSocket handshake.
	OnConnect func(client *Client)

	// OnDisconnect is triggered when a client disconnects or times out.
	OnDisconnect func(client *Client)
}

// defaultConfig provides production-safe default configurations
func defaultConfig() Config {
	return Config{
		PingPeriod:        30 * time.Second,
		PongWait:          60 * time.Second,
		WriteWait:         10 * time.Second,
		MessageBufferSize: 256,
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow all by default for easy development & mobile access
		},
	}
}
