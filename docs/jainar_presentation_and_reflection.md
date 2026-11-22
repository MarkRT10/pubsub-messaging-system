# J10: Presentation Segment: Architecture & Broker Design (K. E. Jainar)

**Slot:** about 3 minutes (15 min ÷ 5 speakers). Jainar speaks **first**, sets up the system, then hands over to Labrador (persistence).
**For Jurquina (U9):** four slides to drop into the deck. Each slide lists its title, content, visual, and what to say.

---

### Slide 1: "What we built: a broker-based pub/sub middleware"
**On slide**
- Stock Price Alert System: 2 publishers → 1 broker → 3 subscribers
- Topic-based filtering · durable queues · at-least-once delivery
- Python 3 standard library only: TCP sockets, threads, sqlite3, JSON
**Visual:** the scenario table (Publisher A/B, Subscribers 1–3 with their topics).

**Script (≈40 s)**
> "Good morning. Our project is a publish-subscribe middleware, built in plain Python with no external libraries. The scenario is a stock price alert system. Two publishers, a stock exchange feed and a crypto feed, publish prices. Three subscribers, a tech investor, a bank analyst and a crypto trader, each receive only the topics they care about. The key idea, from Chapter 6, is indirect communication. Publishers and subscribers never talk to each other. They only talk to the broker. That gives us space uncoupling, because nobody knows who's on the other side, and time uncoupling, because they don't even have to be online at the same time."

---

### Slide 2: "Architecture"
**On slide:** the architecture diagram (G5). Highlight the three broker parts: **server · router · delivery engine**, plus **storage**.

**Script (≈50 s)**
> "This is the architecture. Everything goes through one central broker. The book describes this centralised design as the simplest. Its weakness is being a single point of failure, so we put everything important into SQLite. Inside the broker there are three parts. The **server** accepts TCP connections and gives each client its own thread. That's the thread-per-connection model from Chapter 7, so one slow client never blocks another. The **router** holds the subscription table and decides who gets each message. The **delivery engine** sends messages to subscribers, waits for their acknowledgements, and resends anything that isn't acknowledged. The broker only talks to storage through a small interface, which let William build the database layer in parallel with me."

---

### Slide 3: "The protocol and the life of a message"
**On slide**
```
PUBLISH ─▶ match topic ─▶ ① save to disk ─▶ ② PUBACK
                                          ─▶ ③ DELIVER ─▶ ④ ACK ─▶ mark acked
```
- Newline-delimited JSON over TCP · 8 core message types (+ CONNACK/SUBACK/PING)
- Persistent `client_id`, so durable subscriptions survive disconnects

**Script (≈50 s)**
> "The protocol is newline-delimited JSON over TCP. TCP is a byte stream with no message boundaries, so each JSON message ends with a newline. Here's the life of one message. A publisher sends PUBLISH with a unique ID. The router matches the topic. The broker **saves it to disk first**, and only then sends PUBACK. So once a publisher gets a PUBACK, the message can't be lost. Then we DELIVER to each matching subscriber, and the subscriber sends back an ACK. Until that ACK arrives, the message stays pending. If the ACK doesn't come, we resend. If the subscriber disconnects, the message waits, and when it reconnects with the same client ID, the broker sends everything it missed, in order."

---

### Slide 4: "Design choices and trade-offs"
**On slide**
| Choice | Why | Cost |
|---|---|---|
| Topic-based (with `*`, `#`) | Cheap, predictable matching | Less expressive than content-based |
| At-least-once | No alert is ever lost | Duplicates possible, so subscriber de-dups by msg_id |
| Thread per connection | Simple, isolates clients | Doesn't scale to thousands of clients |
| Central broker + SQLite | Simple, restart-safe | Single point of failure |

**Script (≈40 s), then the hand-off**
> "Every design choice was a trade-off. We chose at-least-once rather than exactly-once, because for price alerts a duplicate is harmless as long as it's detected, but a lost alert isn't. That's why subscribers filter duplicates by message ID. We chose topics over content filtering for speed and simplicity, which Seth will compare in more detail. Now William will show how the persistent queues make all of this survive crashes."

---

### Likely Q&A for Jainar (prepare these)
| Question | Short answer |
|---|---|
| Why not UDP? | We'd have to rebuild ordering and retransmission ourselves. TCP gives per-connection reliability, and our ACKs add end-to-end reliability across crashes. |
| What if the broker crashes after saving but before sending PUBACK? | The publisher times out and retries with the same msg_id. The broker sees a duplicate, so it doesn't store or deliver it twice, and it replies PUBACK duplicate:true. |
| What if the ACK is lost? | The retransmit thread resends after the timeout (5 s, doubling, capped at ×8). The subscriber sees a duplicate msg_id, ACKs it again, and doesn't display it twice. |
| How do you avoid race conditions? | A registry lock, a per-session Condition, a fixed lock order (engine before session), a per-socket write lock, and no socket writes while holding a lock. A route lock makes "match + save" atomic with respect to SUBSCRIBE. |
| What if the same client_id connects twice? | The newest connection wins (takeover) and the old socket is closed. This handles half-open sockets after a crash. |
| Why is the order guaranteed after reconnect? | `get_pending` returns messages sorted by the storage sequence number, and live messages are queued behind the backlog. |
| How would you scale it? | A worker pool or asyncio instead of a thread per connection, a trie index for patterns, and multiple brokers with filtering-based routing (Sec 6.3). |
| Is it exactly-once? | No. Delivery is at-least-once. The subscriber's duplicate filter makes *processing* effectively once. |

---
---

# J11: Individual Reflection (DRAFT, to personalise)

> **Note to Kristopher:** this is a starting draft written from the project record. A reflection is graded as *your* experience, so rewrite it in your own voice. Replace or remove anything that didn't happen the way it's described here, especially the bracketed parts, and add specific moments only you know. Target length: 1 page (about 450–550 words).

**Kristopher Edz Jainar: Broker Core & System Integration (Team Lead)**

My role was to build the broker, the part every other module depends on, and to integrate the team's work into one system. Because everyone else's code talked to mine, I decided the first thing I had to finish was not code but the **protocol specification**. Writing PROTOCOL.md before the broker forced me to answer questions I would otherwise have found out about during integration: what a subscriber needs to know when it reconnects, what happens when a publisher retries, and which errors should close a connection. Freezing that document, together with the storage interface, let William, Mark, Joaquin and Seth work in parallel against a fixed contract instead of waiting for me.

The most useful thing I learned was how much reliability is a matter of **ordering decisions** rather than extra code. The rule "persist before PUBACK, process before ACK" is the whole at-least-once guarantee. If I had swapped either pair, the system would have looked the same in the demo but would have lost messages under failure. Before this project I thought of TCP as "reliable." Now I understand Chapter 4's point that TCP is reliable only *per connection*, and that an application that must survive crashes has to add its own acknowledgements, just like the request-reply protocols in Chapter 5.

The hardest part was **concurrency**. The broker has a thread per connection, a dispatcher thread per subscriber and a retransmit thread, all touching shared state. I had to make sure that one slow subscriber could never freeze every publisher, so I set two rules: locks are always taken in a fixed order, and no socket write ever happens while a lock is held. The subtlest case was reconnect. A message published at the exact moment a subscriber's backlog is being loaded could either be missed or be queued twice. Loading the backlog under the engine lock and de-duplicating by msg_id closes that window. A real bug I did hit was that the broker took seconds to shut down, because `close()` on a listening socket does not wake a thread blocked in `accept()` on Linux. Calling `shutdown()` first cut the test suite's run time from 50 seconds to about 4. [Add any real bugs you hit during integration with the other modules.]

As team lead, I learned that integration risk is best reduced early. Writing contract tests for the storage interface gave William a clear definition of "done" for his module, and running the broker against an in-memory store meant routing could be tested without waiting for the database. [Add how coordination actually went: what worked in the group, what you would change, for example earlier integration milestones, smaller merges, or clearer review turnaround.]

If I did this again, I would build an end-to-end demo script on day one, even with stub clients, so the whole team could see the system working from the start. I would also consider an asyncio design, which avoids most of the locking I had to reason about and scales further than a thread per connection.

Overall, this project turned the Chapter 6 ideas of space and time uncoupling from definitions into something I could see working: a subscriber is killed, the publishers don't notice, and when it comes back every message is waiting for it, in order.
