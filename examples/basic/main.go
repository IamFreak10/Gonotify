package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/IamFreak10/gonotify"
)

func main() {
	// 1. Initialize Gonotify engine
	notifier := gonotify.New(gonotify.Config{
		PingPeriod: 25 * time.Second,
		OnConnect: func(client *gonotify.Client) {
			log.Printf("[Event] New client connected: %s", client.ID)
		},
		OnDisconnect: func(client *gonotify.Client) {
			log.Printf("[Event] Client disconnected: %s", client.ID)
		},
	})
	defer notifier.Close()

	// 2. Mount WebSocket route for Flutter / Mobile app
	// Clients can connect to: ws://localhost:8080/ws?topic=orders
	http.HandleFunc("/ws", notifier.Handler())

	// 3. API route: Simulate order placement
	http.HandleFunc("/api/order", func(w http.ResponseWriter, r *http.Request) {
		orderID := fmt.Sprintf("ORD-%d", time.Now().Unix()%10000)

		// Broadcast only to clients subscribed to "orders" topic
		err := notifier.Publish("orders", gonotify.Message{
			Event: "NEW_ORDER",
			Title: "🎉 New Order Placed!",
			Body:  fmt.Sprintf("Order #%s received for ৳ 1,500", orderID),
			Data: map[string]interface{}{
				"order_id": orderID,
				"amount":   1500,
				"customer": "Rahim Ahmed",
			},
			Timestamp: time.Now().Unix(),
		})

		if err != nil {
			http.Error(w, "Failed to publish notification", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(fmt.Sprintf(`{"status":"success","order_id":"%s"}`, orderID)))
	})

	// 4. Quick browser test client
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`
<!DOCTYPE html>
<html>
<head><title>Gonotify Test</title></head>
<body style="font-family:sans-serif; padding:2rem;">
  <h2>Gonotify WebSocket Test</h2>
  <button onclick="triggerOrder()" style="padding:10px 20px; font-size:16px; cursor:pointer;">Simulate Order</button>
  <ul id="list" style="margin-top:20px;"></ul>
  <script>
    const ws = new WebSocket("ws://" + window.location.host + "/ws?topic=orders");
    ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      const li = document.createElement("li");
      li.textContent = "[" + new Date().toLocaleTimeString() + "] " + msg.title + " - " + msg.body;
      document.getElementById("list").prepend(li);
    };
    function triggerOrder() {
      fetch("/api/order");
    }
  </script>
</body>
</html>
		`))
	})

	port := ":8080"
	log.Printf("Gonotify server running on http://localhost%s", port)
	log.Printf("WebSocket endpoint: ws://localhost%s/ws?topic=orders", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
