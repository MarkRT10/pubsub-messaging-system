# Design Document: Sections by K. E. Jainar (J9)

*For Gaitano to assemble into the System Design Document. Citations follow the format "(Coulouris et al., 2012, Sec x.y, pp. a–b)". The full reference is at the end.*

---

## 1. Architecture Overview

### 1.1 Purpose and style of communication

Our system is a **broker-based publish-subscribe middleware**. Publishers never address subscribers, and subscribers never know which publisher produced a message. Each side communicates only with an intermediary, the broker. Coulouris et al. call this **indirect communication**: communication through an intermediary with no direct coupling between sender and receiver (Coulouris et al., 2012, Sec 6.1, pp. 230–232). The design gives us both forms of decoupling they identify:

* **Space uncoupling.** The Stock Exchange Feed publishes on `stocks.banking` without knowing that the Bank Analyst exists, how many subscribers there are, or where they run. Subscribers can be added or removed without changing a publisher.
* **Time uncoupling.** Publisher and subscriber do not have to be running at the same time. If the Bank Analyst is offline when a price is published, the broker keeps the message in a **durable queue** and delivers it when the analyst reconnects. This is the property our live demo shows.

Of the architectures Coulouris et al. describe for publish-subscribe systems, we chose the **centralised broker** (Coulouris et al., 2012, Sec 6.3, pp. 242–254). A single broker process holds all subscriptions and does all routing. The book notes that this is the simplest architecture, at the cost of a single point of failure and a scalability ceiling. A network of brokers or a peer-to-peer design removes those limits, but it needs distributed routing (filtering-based routing, advertisements, rendezvous). For two publishers, three subscribers and a 15-minute demo, a centralised broker is the right trade-off. We reduce the single-point-of-failure risk with persistence: subscriptions and pending messages are written to SQLite, so a broker restart loses nothing that was acknowledged.

### 1.2 Components

| Component | Code | Responsibility |
|---|---|---|
| **Publisher client** (×2) | `publisher/client.py`, `demo/feeds.py` | Generates simulated prices. Sends `PUBLISH` with a unique `msg_id` and retries until it receives `PUBACK`. |
| **Broker: network layer** | `broker/server.py` | TCP server with one thread per connection. Handles framing, validation, CONNECT/session management and error replies. |
| **Broker: routing engine** | `broker/router.py` | Thread-safe subscription registry. Matches each message's topic against subscription patterns. |
| **Broker: delivery engine** | `broker/delivery.py` | Per-subscriber outbound queue, in-flight (un-ACKed) table, ACK handling, retransmission on timeout, reconnect catch-up. |
| **Durable storage** | `storage/interface.py`, `storage/queue_store.py` | SQLite tables for messages, subscriptions and per-subscriber deliveries. The source of truth for "what is still owed to whom". |
| **Subscriber client** (×3) | `subscriber/client.py` | CONNECTs with a persistent id, SUBSCRIBEs, displays alerts, ACKs each message, discards duplicates. |

The broker depends on storage **only through the abstract `QueueStore` interface** (`save_message`, `register_subscription`, `remove_subscription`, `load_subscriptions`, `get_pending`, `mark_acked`). As a result, the broker was built and tested against an in-memory implementation while the SQLite store was developed in parallel, and the two could be swapped with a command-line flag (`--store memory|sqlite`).

### 1.3 Architecture diagram (sketch for G5)

```
     PUBLISHERS                         BROKER (python -m broker)                         SUBSCRIBERS
 ┌──────────────────┐            ┌─────────────────────────────────────────┐        ┌──────────────────────┐
 │ A: Stock Exchange│ PUBLISH    │  server.py  (accept thread +            │DELIVER │ 1: Tech Investor     │
 │ stocks.tech      │──────────▶ │             1 thread per connection)    │──────▶ │    stocks.tech       │
 │ stocks.banking   │◀────────── │      │                                  │◀────── │                      │
 └──────────────────┘ PUBACK     │      ▼                                  │  ACK   └──────────────────────┘
                                 │  router.py  SubscriptionRegistry        │        ┌──────────────────────┐
 ┌──────────────────┐            │   topic ─match─▶ {subscriber ids}       │──────▶ │ 2: Bank Analyst      │
 │ B: Crypto Feed   │──────────▶ │      │                                  │◀────── │    stocks.banking,   │
 │ crypto           │◀────────── │      ▼  ①save_message (commit)          │        │    crypto            │
 └──────────────────┘            │  ┌──────────────────────┐  ②PUBACK      │        └──────────────────────┘
                                 │  │ storage (SQLite)     │               │        ┌──────────────────────┐
                                 │  │ messages             │◀─④mark_acked─ │──────▶ │ 3: Crypto Trader     │
                                 │  │ subscriptions        │               │◀────── │    crypto            │
                                 │  │ deliveries(acked?)   │─get_pending─▶ │        └──────────────────────┘
                                 │  └──────────────────────┘  (catch-up)   │
                                 │      ③ delivery.py: per-subscriber      │
                                 │        outbound queue → in-flight →     │
                                 │        retransmit on ACK timeout        │
                                 └─────────────────────────────────────────┘
```

Suggested numbered flow for the final diagram: **① persist → ② PUBACK → ③ DELIVER → ④ ACK → mark_acked**, with a dashed arrow "reconnect → get_pending → backlog" into Subscriber 2.

### 1.4 Life of a message

1. **Publish.** Publisher A sends `PUBLISH {msg_id, topic:"stocks.banking", payload}`.
2. **Route.** The connection's handler thread asks the router which subscribers have a matching pattern. The answer is `{bank-analyst}`.
3. **Persist.** In one transaction, `save_message` stores the message and one *pending* delivery row per matched subscriber.
4. **Acknowledge the publisher.** Only after the commit does the broker send `PUBACK`. A message the publisher has seen acknowledged is therefore on disk and cannot be lost by the broker.
5. **Deliver.** If the Bank Analyst is online, the message goes into its session's outbound queue, and a dispatcher thread sends `DELIVER`. If it is offline, nothing more happens now. The pending row waits in storage.
6. **ACK.** The subscriber displays the alert and sends `ACK {msg_id}`. The broker calls `mark_acked`, and the message is never delivered to that subscriber again.
7. **Failure paths.** If no ACK arrives within the timeout, the delivery engine retransmits (with `attempt` incremented). If the subscriber disconnects first, the row is still pending, and **catch-up** delivers it after the reconnect.

### 1.5 Concurrency model

The broker uses the **thread-per-connection** architecture described by Coulouris et al. for multi-threaded servers (Coulouris et al., 2012, Sec 7.4, pp. 286–303): an accept thread hands each new TCP connection to its own handler thread, which blocks on `recv()`. Blocking I/O on one slow client therefore never stalls another, and the code for each client stays sequential and easy to follow. The book also discusses the costs of this design: thread creation overhead and a growing number of threads. These don't matter at our scale (five clients). A worker-pool or `asyncio` design would be the next step for thousands of clients.

The broker also runs two other kinds of thread:

* **One dispatcher thread per online subscriber.** It moves messages from the outbound queue to the socket while fewer than `max_inflight` (default 32) messages are un-ACKed. This is a simple flow-control window, so a slow subscriber builds a queue *in the broker* and does not block publishers.
* **One retransmit thread.** It scans all in-flight tables every 200 ms and resends any DELIVER whose ACK is overdue.

Shared state is protected as follows. The subscription registry uses a re-entrant lock. Each subscriber session has a `Condition` variable that guards its queues and wakes its dispatcher. The engine-wide session table has its own lock, which is always taken *before* a session lock, never after, so deadlock is impossible. Socket writes are serialised by a per-connection write lock, so a DELIVER from the dispatcher and a retransmission from the retransmit thread can never interleave their bytes. **No socket write is ever done while holding a routing or session lock**, so one stalled TCP connection cannot freeze the broker.

---

## 2. Protocol Design

The full specification is `PROTOCOL.md` in the repository. This section explains the design decisions behind it.

### 2.1 Transport: TCP streams

We use **TCP** rather than UDP because the system promises that no message is lost. TCP already provides reliable, ordered, flow-controlled delivery between two processes. Coulouris et al. describe the stream abstraction and its properties, including the absence of message boundaries and the fact that a failed connection is reported to the application (Coulouris et al., 2012, Sec 4.2, pp. 147–158). UDP would force us to rebuild ordering and retransmission for every hop.

TCP's reliability ends when a connection breaks, though. Bytes in flight, or a message the subscriber received but crashed before processing, are lost. That is why we add **application-level acknowledgements** (PUBACK and ACK) on top of TCP. TCP gives reliability *per connection*. Our protocol gives reliability *end to end*, across crashes and reconnects.

### 2.2 Framing and data representation

Because a TCP stream has no message boundaries, the application must mark where each message ends. We chose **newline-delimited JSON**: every frame is one JSON object followed by `\n`. JSON's string escaping guarantees that a raw newline never appears inside a frame, so splitting on `\n` is always correct. A 64 KiB frame limit protects the broker from a client that never sends a newline.

JSON is our **external data representation**. Both sides agree on a language- and platform-neutral textual form for the data and marshal into it before sending (Coulouris et al., 2012, Sec 4.3, pp. 158–169). Coulouris et al. compare binary forms such as CORBA CDR and Java serialization with textual ones such as XML. Textual forms are larger and slower to parse but self-describing and human-readable. We accepted that trade-off on purpose. Message rates in the demo are a few per second, and readable frames made debugging, logging and the fault-test evidence much easier. Python's standard `json` module does the marshalling, so no third-party dependency is needed.

### 2.3 Message types and session model

| Direction | Frames | Purpose |
|---|---|---|
| Client → Broker | `CONNECT`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `ACK`, `PING`, `DISCONNECT` | Requests and acknowledgements |
| Broker → Client | `CONNACK`, `SUBACK`, `UNSUBACK`, `PUBACK`, `DELIVER`, `PONG`, `ERROR` | Replies and deliveries |

* **CONNECT carries a persistent `client_id` and a `role`.** The `client_id` is the subscriber's identity across connections. Durable subscriptions and queues are keyed on it rather than on the TCP connection, which is what makes reconnect catch-up possible. The role lets the broker reject frames that make no sense, such as a publisher sending ACK (`ERROR 407`).
* **CONNACK reports `session_present` and the restored `subscriptions`,** so a reconnecting subscriber knows it does not need to subscribe again. CONNACK is always sent before any backlog DELIVER.
* **Session takeover.** If a `client_id` connects while an older connection with the same id still looks open (common after a crash, when the old socket is half-open), the newest connection wins and the old one is closed. A killed subscriber can then be restarted at once in the demo, without waiting for TCP to time out.
* **`req_id` echo.** Any request may carry a `req_id`, which the broker copies into the reply. This lets a client match replies to requests without the protocol needing strict lock-step.
* **Structured errors.** `ERROR` frames carry a numeric code, a symbolic name and a readable message (for example `402 INVALID_TOPIC`). Only two errors are fatal and close the connection: oversized frame and unsupported version. All others leave the connection usable.

### 2.4 Delivery semantics: at-least-once

Coulouris et al. analyse the invocation semantics that can be built from three mechanisms: retrying a request, filtering duplicates, and retransmitting results. Retrying without duplicate filtering gives **at-least-once** semantics, and adding duplicate filtering moves toward **at-most-once** (Coulouris et al., 2012, Sec 5.3, pp. 195–204; request-reply retry behaviour in Sec 5.2, pp. 187–195). We apply the same reasoning at both hops.

| Hop | Retry | Duplicate filtering | Result |
|---|---|---|---|
| Publisher → Broker | Publisher resends the **same** `msg_id` until PUBACK | Broker: `save_message` is idempotent on `msg_id` and replies `PUBACK duplicate:true` | Each message is stored **exactly once** |
| Broker → Subscriber | Broker retransmits DELIVER on ACK timeout (backoff ×2, capped at ×8), and again after a reconnect | Subscriber: local seen-`msg_id` file (Tortuya, T5) | **At-least-once** delivery, **effectively-once** processing |

Two ordering rules make the guarantee hold:

1. **Persist before PUBACK.** If the broker crashes before committing, the publisher gets no PUBACK and retries. If it crashes after committing, the message is on disk.
2. **Subscriber processes before ACK.** If the subscriber crashes between receiving and processing, the message is still pending and is redelivered. The cost is that a crash *after* processing but *before* ACK produces a duplicate. The subscriber's duplicate filter absorbs it.

We chose at-least-once over exactly-once deliberately. True exactly-once delivery would need a distributed commit between broker and subscriber on every message. For price alerts, a duplicate that is detected and ignored is harmless, while a lost alert is not.

---

## 3. Topic Filtering Mechanism

### 3.1 Choice of subscription model

Coulouris et al. classify publish-subscribe systems by **subscription (filter) model**: channel-based, topic-based (also called subject-based), content-based and type-based (Coulouris et al., 2012, Sec 6.3, pp. 242–254). We implement **topic-based** filtering. Every published message carries exactly one topic string, and a subscriber registers interest in topics. The broker matches on the topic only and never inspects the payload. Gaitano's section compares this with content-based filtering. The short version is that topic-based matching is cheap and predictable and fits a scenario whose categories (tech stocks, banking stocks, crypto) are known in advance.

### 3.2 Topic names and hierarchical patterns

Topics are **hierarchical, dot-separated names** (`stocks.tech`, `stocks.banking`, `crypto`). Each segment is `[A-Za-z0-9_-]+`. Subscriptions are **patterns** that may use two wildcards, a common extension of plain topic-based filtering:

| Pattern | Matches | Does not match |
|---|---|---|
| `stocks.tech` | `stocks.tech` | `stocks.banking`, `stocks` |
| `stocks.*` (`*` = exactly one segment) | `stocks.tech`, `stocks.banking` | `stocks`, `stocks.tech.us` |
| `stocks.#` (`#` = zero or more trailing segments) | `stocks`, `stocks.tech`, `stocks.tech.us` | `crypto` |

The hierarchy adds a small amount of expressiveness without the cost of content-based filtering. A "market overview" subscriber can follow every stock category with one pattern, `stocks.*`, and new categories (`stocks.energy`) are picked up automatically without changing the subscriber. Wildcards are rejected in PUBLISH, so a message always has one concrete topic. The scenario itself uses only exact topics.

### 3.3 Matching algorithm

`topic_matches(pattern, topic)` splits both strings on `.` and walks the segments together:

```
for each segment i of the pattern:
    if seg == "#":                return True      # absorbs the rest, including nothing
    if topic has no segment i:    return False
    if seg != "*" and seg != topic[i]: return False
return len(pattern) == len(topic)                  # no unmatched trailing topic segments
```

The router evaluates this for every (subscriber, pattern) pair and collects the set of subscriber ids with **at least one** match. Using a set means a subscriber whose patterns overlap (for example `stocks.*` and `stocks.tech`) still receives each message **once**. The cost is O(number of subscriptions × topic depth) per publish, which takes microseconds at our scale. A larger system would index patterns, for example in a trie keyed by segment, and could spread matching across a network of brokers. Coulouris et al. discuss this under the implementation of publish-subscribe systems (Coulouris et al., 2012, Sec 6.3, pp. 242–254).

### 3.4 Subscription registration and durability

1. The subscriber sends `SUBSCRIBE {topics:[...]}`. Every pattern is validated first, and a bad one is rejected with `ERROR 402`.
2. For each pattern, the router **first** calls `store.register_subscription(client_id, pattern)` (durable) and **then** adds it to the in-memory registry used for fast matching.
3. The broker replies `SUBACK` with the subscriber's complete subscription list.
4. On startup, the broker calls `store.load_subscriptions()` and rebuilds the registry. Subscriptions therefore survive both subscriber restarts and broker restarts.

Subscriptions are keyed by **`client_id`, not by connection**. A subscriber that disconnects is still subscribed. Messages for its topics keep being routed to it and written as pending deliveries, which is what makes the queues durable (Coulouris et al., 2012, Sec 6.4, pp. 254–262, on persistent message queues that hold messages until the consumer retrieves them). Only an explicit `UNSUBSCRIBE` stops the routing.

A small lock around "match subscribers + save message" ensures a PUBLISH is never routed against a half-applied SUBSCRIBE or UNSUBSCRIBE. Each message is matched against one consistent snapshot of the subscriptions.

### 3.5 Guaranteeing no cross-topic leakage

Filtering happens **only** in the broker. A subscriber is never sent a message unless the router matched it, because delivery rows are created only for the matched subscriber ids. `tests/test_broker.py::RoutingTests::test_topic_filtering_no_cross_topic_leakage` checks this with the full scenario: the Tech Investor receives only `stocks.tech`, the Crypto Trader only `crypto`, the Bank Analyst only `stocks.banking` and `crypto`, and the total number of deliveries is exactly the expected number.

---

## References (citations used in these sections)

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed Systems: Concepts and Design* (5th ed.). Addison-Wesley. ISBN 978-0-13-214301-1.

| # | Section | Pages* | Used for |
|---|---|---|---|
| 1 | Sec 4.2: The API for the Internet protocols | 147–158 | TCP stream communication, no message boundaries |
| 2 | Sec 4.3: External data representation and marshalling | 158–169 | JSON as the external data representation; textual vs binary |
| 3 | Sec 5.2: Request-reply protocols | 187–195 | Timeouts, retries, duplicate requests |
| 4 | Sec 5.3: Remote procedure call (invocation semantics) | 195–204 | At-least-once vs at-most-once semantics |
| 5 | Sec 6.1: Introduction (indirect communication) | 230–232 | Space and time uncoupling |
| 6 | Sec 6.3: Publish-subscribe systems | 242–254 | Subscription models, centralised broker architecture, implementation |
| 7 | Sec 6.4: Message queues | 254–262 | Persistent queues, reliable delivery |
| 8 | Sec 7.4: Processes and threads | 286–303 | Thread-per-connection server architecture |

\*Page numbers are from the US 5th edition (Addison-Wesley, 2012). The Pearson international/global printing (ISBN 978-1-4479-3017-4) is paginated differently. For example, Chapter 6 starts on p. 245 instead of p. 229 (+16). **Check against the copy the class uses before the final PDF.**
