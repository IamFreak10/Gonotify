# ⚡ Gonotify

A lightweight, high-performance, and framework-agnostic **Real-time Notification & Live Event Engine** in Golang. Built with raw WebSockets for maximum speed, zero bloat, and seamless mobile (Flutter / React Native) & web integration.

---

## ✨ Features

- 🚀 **Lightweight & Fast**: Powered by pure Go and `gorilla/websocket` with zero unnecessary bloat.
- 🎯 **Topic-based Subscriptions**: Send notifications to specific channels (`orders`, `user:101`, `alerts`) or broadcast globally to all.
- 💓 **Built-in Heartbeat (Ping/Pong)**: Keeps connections alive across Render, NAT, and mobile networks.
- 📱 **Mobile Ready**: Built-in query param & JSON commands for seamless Flutter/React Native integration.
- 🔌 **Framework Agnostic**: Works out of the box with standard `net/http`, Gin, Fiber, Echo, or Chi.
- 🛡️ **Custom Auth & Hooks**: Easily plug in JWT/API token verification and connect/disconnect event hooks.

---

## 📦 Installation

```bash
go get github.com/IamFreak10/gonotify
```

---

## 🚀 Quick Start (Go Backend)

```go
package main

import (
    "net/http"
    "time"
    "github.com/IamFreak10/gonotify"
)

func main() {
    // 1. Initialize Gonotify engine
    notifier := gonotify.New(gonotify.Config{
        PingPeriod: 25 * time.Second,
    })
    defer notifier.Close()

    // 2. Mount WebSocket endpoint
    // Mobile / Web clients connect to: ws://localhost:8080/ws?topic=orders
    http.HandleFunc("/ws", notifier.Handler())

    // 3. Trigger notification on customer order
    http.HandleFunc("/api/order", func(w http.ResponseWriter, r *http.Request) {
        // Send instant notification to everyone subscribed to "orders"
        notifier.Publish("orders", gonotify.Message{
            Event: "NEW_ORDER",
            Title: "🎉 New Order Placed!",
            Body:  "Order #5021 received (৳ 1,500)",
            Data: map[string]interface{}{
                "order_id": 5021,
                "amount":   1500,
            },
        })

        w.Write([]byte("Order placed successfully!"))
    })

    http.ListenAndServe(":8080", nil)
}
```

---

## 📱 Flutter Client Integration

Add `web_socket_channel` to your `pubspec.yaml`:
```yaml
dependencies:
  web_socket_channel: ^3.0.1
```

Use the helper from [`examples/flutter/notification_service.dart`](./examples/flutter/notification_service.dart):

```dart
final service = GonotifyService(
  serverUrl: 'wss://your-app.onrender.com/ws',
  defaultTopics: ['orders'],
  onNotification: (message) {
    print('Notification received: ${message.title} - ${message.body}');
    // Show local notification with flutter_local_notifications
  },
);

service.connect();
```

---

## 🎯 Topic Subscription Guide

Clients can subscribe to topics in two convenient ways:

### 1. Via URL Query Parameter (On Connection)
```text
ws://your-server.com/ws?topic=orders&topic=user:42
```

### 2. Via WebSocket JSON Message (Dynamically)
```json
{
  "action": "subscribe",
  "topic": "orders"
}
```
To unsubscribe:
```json
{
  "action": "unsubscribe",
  "topic": "orders"
}
```

---

## ⚙️ Advanced Configuration

```go
notifier := gonotify.New(gonotify.Config{
    PingPeriod:        30 * time.Second,
    PongWait:          60 * time.Second,
    MessageBufferSize: 512,
    Authenticate: func(r *http.Request) bool {
        token := r.URL.Query().Get("token")
        return token == "my-secret-token" // Validate JWT / Bearer Token
    },
    OnConnect: func(client *gonotify.Client) {
        log.Printf("Client %s joined", client.ID)
    },
    OnDisconnect: func(client *gonotify.Client) {
        log.Printf("Client %s disconnected", client.ID)
    },
})
```

---

## 📄 License

MIT License © 2026 [IamFreak10](https://github.com/IamFreak10)

---

## 👥 Contributors

- **[Mahfuj Abdulla (@IamFreak10)](https://github.com/IamFreak10)** — Lead Developer & Maintainer
- **Antigravity AI (Google DeepMind)** — Co-designer & Architecture Pair Programmer
