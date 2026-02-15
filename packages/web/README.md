# @floruntime/web

WebSocket client for Flo in browser environments.

## Features

- **Real-time Streams**: Subscribe to and publish events with pub/sub patterns
- **KV Store (Read-Only)**: Access configuration, feature flags, user preferences
- **Authentication**: JWT-based auth with scopes for fine-grained access control
- **Auto-reconnect**: Handles disconnections with token refresh callbacks

## Installation

```bash
npm install @floruntime/web
# or
pnpm add @floruntime/web
```

## Quick Start

```typescript
import { FloClient } from "@floruntime/web";

const client = new FloClient("wss://flo.example.com/ws", {
  namespace: "myapp",
  authToken: "your-jwt-token",
});

await client.connect();

// Subscribe to real-time events
await client.streams.subscribe("chat:room-123", (record) => {
  console.log("New message:", new TextDecoder().decode(record.payload));
});

// Publish an event
await client.streams.append("chat:room-123", encoder.encode("Hello!"));

// Read configuration (read-only KV)
const config = await client.kv.get("config:feature-flags");
```

## Authentication

### JWT Token Format

Flo uses JWT (JSON Web Tokens) for authentication. Tokens are passed during the WebSocket upgrade handshake as a query parameter.

**Supported JWT Claims:**

| Claim | Description |
|-------|-------------|
| `sub` | User ID |
| `flo_namespace` | Locked namespace (connection can only access this namespace) |
| `flo_scopes` | Permission scopes array |
| `exp` | Expiration timestamp (Unix seconds) |

### Scopes

Scopes control what operations a connection can perform. Format: `{action}:{resource_type}:{key_pattern}`

**Examples:**
- `read:stream:*` - Read any stream
- `write:stream:chat:*` - Write to streams starting with "chat:"
- `read:kv:config:*` - Read KV keys starting with "config:"
- `*:*:*` - Full access (admin)

```typescript
// Example JWT payload
{
  "sub": "user-123",
  "flo_namespace": "myapp",
  "flo_scopes": [
    "read:stream:chat:*",
    "write:stream:chat:*",
    "read:kv:config:*"
  ],
  "exp": 1734000000
}
```

### Token Refresh

Handle token expiration with the `onAuthRequired` callback:

```typescript
const client = new FloClient("wss://flo.example.com/ws", {
  authToken: getCurrentToken(),
  onAuthRequired: async () => {
    // Called when auth fails - return a fresh token
    const newToken = await refreshTokenFromAuthServer();
    return newToken;
  },
});
```

### Error Handling

```typescript
import { isUnauthorized, UnauthorizedError } from "@floruntime/web";

try {
  await client.streams.append("restricted:stream", data);
} catch (err) {
  if (isUnauthorized(err)) {
    // Scope doesn't allow this operation
    console.log("Permission denied:", err.message);
  }
}
```

## API Reference

### FloClient

```typescript
new FloClient(url: string, options?: WebClientOptions)
```

**Options:**
- `namespace`: Default namespace for operations
- `authToken`: JWT token for authentication
- `onAuthRequired`: Callback for token refresh
- `timeoutMs`: Request timeout (default: 5000ms)
- `logger`: `true` for console logging, or custom `Logger` object

### Stream Operations

```typescript
// Subscribe to a stream (real-time push)
const sub = await client.streams.subscribe(streamName, callback, options);
// Options: { offset: 0n } from beginning, { from: timestamp } from time, omit for tail

// Unsubscribe
await sub.unsubscribe();

// Publish to a stream
const result = await client.streams.append(streamName, payload, options);

// Read historical records
const records = await client.streams.read(streamName, options);
// Options: { offset: 0n, limit: 100 } or { from: timestamp, to: timestamp }
```

### KV Operations (Read-Only)

```typescript
// Get a value
const entry = await client.kv.get(key);

// Scan keys by prefix
const results = await client.kv.scan(prefix, options);
```

> **Note:** KV mutations (put, delete) are not available in the web client. Use `@floruntime/node` on your backend for write operations.

## Security Considerations

1. **Never expose JWT secrets in browser code** - Generate tokens on your backend
2. **Use short-lived tokens** - Set appropriate `exp` claims
3. **Minimum scopes** - Only grant scopes the client needs
4. **Namespace locking** - Use `flo_namespace` to restrict multi-tenant access
5. **HTTPS/WSS** - Always use secure WebSocket connections in production

## Browser Compatibility

Works in all modern browsers supporting WebSocket:
- Chrome 16+
- Firefox 11+
- Safari 7+
- Edge 12+

## License

MIT
