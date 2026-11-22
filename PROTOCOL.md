# PROTOCOL.md — Pub/Sub Wire Protocol v1

**Owner:** Kristopher Edz Jainar (M1 Broker Core) · **Status:** FROZEN for v1
Any change requires Jainar's approval and a notice to the whole team (Integration Checklist).
Reference implementation of framing and validation: `common/protocol.py`.

---

## 1. Transport and framing

| Item | Rule |
|---|---|
| Transport | TCP. Default address is `127.0.0.1:5555` |
| Encoding | UTF-8 |
| Framing | **Newline-delimited JSON.** Each frame is exactly one JSON object followed by `\n`. A frame may not contain a literal newline; JSON escapes newlines inside strings as `\n`, so this is automatic with `json.dumps`. |
| Max frame size | 65,536 bytes including the newline. A larger frame gets `ERROR 413 FRAME_TOO_LARGE` and the broker **closes the connection**. |
| Blank lines | Ignored, so they can be used as keep-alives. |
| Envelope | Every frame must be a JSON object with a string field `"type"`. |
| `req_id` (optional) | Any request may include `"req_id"` (any JSON value). The broker copies it into its reply or ERROR, so a client can match responses to requests. |
| Unknown fields | Ignored by the receiver (forward compatibility). |

TCP gives a reliable, ordered **byte stream** with no message boundaries, so the
application has to mark where each message ends. We use the newline.
Python clients can use `common.protocol.FrameStream` (`send(dict)` / `recv() -> dict | None`).

## 2. Session lifecycle

```
Publisher                      Broker                         Subscriber
    |-- CONNECT(role=publisher) -->|<-- CONNECT(role=subscriber) --|
    |<----------- CONNACK ---------|------------- CONNACK -------->|
    |                              |<-------- SUBSCRIBE -----------|
    |                              |-------------- SUBACK -------->|
    |-- PUBLISH(msg_id, topic) --->|  1. match subscriptions       |
    |                              |  2. persist msg + deliveries  |
    |<-------- PUBACK(msg_id) -----|  3. PUBACK only after commit  |
    |                              |------- DELIVER(msg_id) ------>|
    |                              |<-------- ACK(msg_id) ---------|  4. mark acked in storage
```

1. The client opens a TCP connection and **must** send `CONNECT` first. Anything else except `PING` gets `ERROR 405 NOT_CONNECTED`.
2. `client_id` is the **persistent identity** of a client. A subscriber that reconnects with the same `client_id` resumes its durable session: subscriptions are kept, and every un-ACKed message is delivered in publish order.
3. If a `client_id` connects while an older connection with that id is still open (for example a half-open socket after a crash), **the new connection wins** and the broker closes the old one.
4. A client ends the session with `DISCONNECT`, or by closing the socket. For a subscriber, both have the same effect: its subscriptions stay **durable**, and messages keep queuing until it reconnects. Use `UNSUBSCRIBE` to stop receiving a topic.

## 3. Topics and patterns

* **Topic** (used in `PUBLISH`): one or more dot-separated segments. Each segment matches `[A-Za-z0-9_-]+`. The maximum length is 200 characters, and wildcards are not allowed.
  Scenario topics: `stocks.tech`, `stocks.banking`, `crypto`.
* **Pattern** (used in `SUBSCRIBE` and `UNSUBSCRIBE`): a topic in which a whole segment may be a wildcard.
  * `*` matches exactly one segment. `stocks.*` matches `stocks.tech` and `stocks.banking`.
  * `#` matches zero or more trailing segments and may only be the last segment. `stocks.#` matches `stocks`, `stocks.tech` and `stocks.tech.us`.
* A message goes to each subscriber with **at least one** matching pattern, and **once** per subscriber even if several of its patterns match.

## 4. Message types

`C→B` = client to broker, `B→C` = broker to client. **Bold** fields are required.

### CONNECT (C→B)
```json
{"type":"CONNECT","client_id":"bank-analyst","role":"subscriber","version":1}
```
| Field | Type | Notes |
|---|---|---|
| **client_id** | string | 1–64 characters `[A-Za-z0-9_.-]`. Must be stable across restarts (pass it with `--id`). |
| **role** | `"publisher"` \| `"subscriber"` | Determines which frames are allowed (see §6). |
| version | int | Defaults to 1. Any other value gets `426` and the connection is closed. |

### CONNACK (B→C)
```json
{"type":"CONNACK","client_id":"bank-analyst","role":"subscriber","version":1,
 "session_present":true,"subscriptions":["crypto","stocks.banking"]}
```
`session_present` is true if the broker already holds durable subscriptions for this id. In that case the subscriber does **not** need to subscribe again.
CONNACK is always sent **before** any backlog `DELIVER`.

### SUBSCRIBE (C→B, subscriber only) → SUBACK
```json
{"type":"SUBSCRIBE","topics":["stocks.banking","crypto"]}
{"type":"SUBACK","topics":["stocks.banking","crypto"],"subscriptions":["crypto","stocks.banking"]}
```
`topics` is a non-empty list of patterns; a single string is also accepted. The call is idempotent. Subscriptions are persisted before SUBACK is sent. `subscriptions` lists the client's full current set.
A new subscription only receives messages published **after** it was registered.

### UNSUBSCRIBE (C→B, subscriber only) → UNSUBACK
```json
{"type":"UNSUBSCRIBE","topics":["crypto"]}
{"type":"UNSUBACK","topics":["crypto"],"subscriptions":["stocks.banking"]}
```
Removing a pattern that isn't subscribed is not an error. Messages already queued for the client are still delivered.

### PUBLISH (C→B, publisher only) → PUBACK
```json
{"type":"PUBLISH","msg_id":"5b0e...-uuid4","topic":"stocks.tech",
 "payload":{"symbol":"AAPL","price":191.20,"change_pct":0.42},"timestamp":1795822800000}
{"type":"PUBACK","msg_id":"5b0e...-uuid4","duplicate":false,"recipients":1}
```
| Field | Type | Notes |
|---|---|---|
| **msg_id** | string ≤ 128 | **Globally unique.** Use `uuid4` (`common.protocol.new_msg_id()`). Reuse the **same** msg_id when retrying. |
| **topic** | string | A concrete topic, with no wildcards. |
| **payload** | any JSON value | Opaque to the broker. Recommended shape for the scenario: `{"symbol","price","change_pct"}` |
| timestamp | int (ms since epoch) | Optional. If it is missing or not an integer, the broker fills in its own time. |

* The broker sends PUBACK **only after the message and its per-subscriber delivery records are committed to storage**. A PUBACKed message cannot be lost by the broker.
* **Publisher retry rule:** if no PUBACK arrives within the timeout (5 s is suggested), resend the **identical** PUBLISH, reconnecting first if needed. The broker de-duplicates by `msg_id`, so a retry replies `PUBACK` with `"duplicate":true` and is **not** delivered a second time.
* `recipients` is the number of subscribers the message was queued for. It can be 0.

### DELIVER (B→C, to subscriber)
```json
{"type":"DELIVER","msg_id":"5b0e...","topic":"stocks.tech","payload":{...},
 "publisher_id":"stock-exchange","timestamp":1795822800000,"attempt":1,"backlog":false}
```
| Field | Meaning |
|---|---|
| attempt | 1 for the first send in this connection. Values above 1 mean a retransmission after an ACK timeout. |
| backlog | `true` if the message was queued while the subscriber was offline or un-ACKed in a previous connection, and is now being delivered by catch-up. |

### ACK (C→B, subscriber only)
```json
{"type":"ACK","msg_id":"5b0e..."}
```
Send it **after** processing a DELIVER (after it has been displayed or logged). There is no reply. ACKing an unknown or already-ACKed id is harmless.
**If you receive a msg_id you have already processed, ACK it again and do not process it again.** This is the duplicate-detection rule (Tortuya, T5).

### PING → PONG (either role, allowed before CONNECT)
```json
{"type":"PING"}   {"type":"PONG"}
```

### DISCONNECT (C→B)
```json
{"type":"DISCONNECT"}
```
The broker closes the connection. There is no reply.

### ERROR (B→C)
```json
{"type":"ERROR","code":402,"error":"INVALID_TOPIC","message":"invalid topic 'stocks.*': ...","ref":"msg-id-if-any","req_id":7}
```

## 5. Error codes

| code | error | When | Connection |
|---|---|---|---|
| 400 | MALFORMED_FRAME | The line is not valid JSON, not an object, or has no string `type` | stays open |
| 401 | MISSING_FIELD | A required field is missing or has the wrong type (`role`, `topics`, `msg_id`, `payload`) | stays open |
| 402 | INVALID_TOPIC | Bad topic or pattern syntax, or a wildcard in PUBLISH | stays open |
| 403 | INVALID_CLIENT_ID | `client_id` breaks the format rules | stays open |
| 404 | UNKNOWN_TYPE | Unrecognised `type` | stays open |
| 405 | NOT_CONNECTED | A frame other than CONNECT or PING was sent before CONNECT | stays open |
| 406 | ALREADY_CONNECTED | A second CONNECT on the same socket | stays open |
| 407 | ROLE_VIOLATION | A publisher sent SUBSCRIBE/ACK, or a subscriber sent PUBLISH | stays open |
| 413 | FRAME_TOO_LARGE | The frame is over 64 KiB | **closed** |
| 426 | UNSUPPORTED_VERSION | `version` ≠ 1 | **closed** |
| 500 | INTERNAL_ERROR | Broker or storage failure. For PUBLISH, **no PUBACK was sent, so retry.** | stays open |

## 6. Allowed frames per role

| Frame | Publisher | Subscriber |
|---|:-:|:-:|
| CONNECT, PING, DISCONNECT | ✔ | ✔ |
| PUBLISH | ✔ | ✘ 407 |
| SUBSCRIBE, UNSUBSCRIBE, ACK | ✘ 407 | ✔ |

## 7. Delivery guarantees (what each side may rely on)

1. **At-least-once.** Every message that was PUBACKed is delivered to every subscriber matched at publish time, at least once, as long as that subscriber eventually connects and ACKs.
2. **Redelivery.** If a DELIVER is not ACKed within `ack_timeout` (default 5 s, set with `--ack-timeout`), the broker resends it with `attempt+1`. The timeout doubles on each attempt, up to 8 times the base value. Un-ACKed messages are also resent after a reconnect, with `backlog:true`.
3. **Ordering.** Live messages from the **same publisher** reach each subscriber in publish order on their first send. The catch-up backlog is sent in storage order (`seq`), which is the order the broker received the messages. There is no global order across different publishers, and a retransmission can arrive after newer messages. Subscribers that need a strict order should use `timestamp`.
4. **Duplicates are possible** (a lost ACK, or a crash between processing and ACK). Subscribers must be idempotent and de-duplicate by `msg_id`.
5. **Flow control.** At most `max_inflight` (default 32) un-ACKed DELIVERs per subscriber. Later messages wait in the broker's queue.
6. **Durability.** Subscriptions and pending deliveries are stored through the `QueueStore` interface (`storage/interface.py`). With the SQLite store, they survive a broker restart.

## 8. Example session (stock alert scenario)

```
C→B {"type":"CONNECT","client_id":"bank-analyst","role":"subscriber"}
B→C {"type":"CONNACK","client_id":"bank-analyst","role":"subscriber","version":1,"session_present":false,"subscriptions":[]}
C→B {"type":"SUBSCRIBE","topics":["stocks.banking","crypto"]}
B→C {"type":"SUBACK","topics":["stocks.banking","crypto"],"subscriptions":["crypto","stocks.banking"]}
            ... Publisher "crypto-feed" PUBLISHes {"symbol":"BTC","price":97100} on "crypto" ...
B→C {"type":"DELIVER","msg_id":"bfd3...","topic":"crypto","payload":{"symbol":"BTC","price":97100},"publisher_id":"crypto-feed","timestamp":1795822800000,"attempt":1,"backlog":false}
C→B {"type":"ACK","msg_id":"bfd3..."}
            ... subscriber is killed; 2 more messages are published and queued ...
C→B {"type":"CONNECT","client_id":"bank-analyst","role":"subscriber"}
B→C {"type":"CONNACK",...,"session_present":true,"subscriptions":["crypto","stocks.banking"]}
B→C {"type":"DELIVER","msg_id":"56d1...","topic":"stocks.banking",...,"backlog":true}
B→C {"type":"DELIVER","msg_id":"7434...","topic":"crypto",...,"backlog":true}
```

## 9. Broker command line

```
python -m broker [--host 127.0.0.1] [--port 5555] [--store sqlite|memory] [--db broker.db]
                 [--ack-timeout 5] [--max-inflight 32] [--log-level INFO|DEBUG]
```
