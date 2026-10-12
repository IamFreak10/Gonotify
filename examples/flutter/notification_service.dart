import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Notification event model matching Gonotify backend format
class GonotifyMessage {
  final String? id;
  final String? topic;
  final String event;
  final String title;
  final String body;
  final Map<String, dynamic>? data;
  final int timestamp;

  GonotifyMessage({
    this.id,
    this.topic,
    required this.event,
    required this.title,
    required this.body,
    this.data,
    required this.timestamp,
  });

  factory GonotifyMessage.fromJson(Map<String, dynamic> json) {
    return GonotifyMessage(
      id: json['id'] as String?,
      topic: json['topic'] as String?,
      event: json['event'] ?? 'UNKNOWN',
      title: json['title'] ?? '',
      body: json['body'] ?? '',
      data: json['data'] != null ? Map<String, dynamic>.from(json['data']) : null,
      timestamp: json['timestamp'] ?? 0,
    );
  }
}

/// Production-ready Gonotify Flutter WebSocket Client with Auto-Reconnect
class GonotifyService {
  final String serverUrl; // e.g. "wss://your-app.onrender.com/ws" or "ws://10.0.2.2:8080/ws"
  final List<String> defaultTopics;
  final void Function(GonotifyMessage message) onNotification;

  WebSocketChannel? _channel;
  StreamSubscription? _subscription;
  bool _isConnected = false;
  bool _shouldReconnect = true;
  Timer? _reconnectTimer;

  GonotifyService({
    required this.serverUrl,
    this.defaultTopics = const ['orders'],
    required this.onNotification,
  });

  /// Connect to the Gonotify WebSocket Server
  void connect() {
    _shouldReconnect = true;
    _reconnectTimer?.cancel();

    try {
      // Build URI with topic parameters
      final uri = Uri.parse(serverUrl).replace(queryParameters: {
        'topic': defaultTopics,
      });

      debugPrint('[Gonotify] Connecting to $uri...');
      _channel = WebSocketChannel.connect(uri);

      _subscription = _channel!.stream.listen(
        (data) {
          _isConnected = true;
          try {
            final jsonMap = jsonDecode(data as String) as Map<String, dynamic>;
            // Ignore internal ping/pong frames
            if (jsonMap['event'] == 'pong') return;

            final message = GonotifyMessage.fromJson(jsonMap);
            onNotification(message);
          } catch (e) {
            debugPrint('[Gonotify] Parse error: $e');
          }
        },
        onError: (error) {
          debugPrint('[Gonotify] Connection error: $error');
          _scheduleReconnect();
        },
        onDone: () {
          debugPrint('[Gonotify] Connection closed.');
          _scheduleReconnect();
        },
      );
    } catch (e) {
      debugPrint('[Gonotify] Setup error: $e');
      _scheduleReconnect();
    }
  }

  /// Subscribe to an additional topic dynamically
  void subscribe(String topic) {
    if (_channel != null && _isConnected) {
      final cmd = jsonEncode({'action': 'subscribe', 'topic': topic});
      _channel!.sink.add(cmd);
    }
  }

  /// Unsubscribe from a topic
  void unsubscribe(String topic) {
    if (_channel != null && _isConnected) {
      final cmd = jsonEncode({'action': 'unsubscribe', 'topic': topic});
      _channel!.sink.add(cmd);
    }
  }

  void _scheduleReconnect() {
    _isConnected = false;
    _subscription?.cancel();
    if (!_shouldReconnect) return;

    _reconnectTimer?.cancel();
    // Reconnect after 3 seconds
    _reconnectTimer = Timer(const Duration(seconds: 3), () {
      debugPrint('[Gonotify] Reconnecting...');
      connect();
    });
  }

  /// Disconnect cleanly
  void disconnect() {
    _shouldReconnect = false;
    _reconnectTimer?.cancel();
    _subscription?.cancel();
    _channel?.sink.close();
    _isConnected = false;
  }
}
