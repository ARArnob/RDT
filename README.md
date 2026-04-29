# Reliable Data Transfer Protocol
### Go-Back-N Implementation over UDP


## Project Overview

This project implements a **Reliable Data Transfer (RDT) Protocol** using the **Go-Back-N (GBN)** algorithm on top of UDP sockets in Java. Since UDP does not provide reliability by default, all reliability mechanisms are built from scratch:

- ✅ Sequence numbering
- ✅ Acknowledgment (ACK) based confirmation
- ✅ Timeout-based retransmission
- ✅ Sliding window (Go-Back-N)
- ✅ Duplicate packet detection and discard
- ✅ Packet loss simulation (random + manual)
- ✅ Data corruption simulation with CRC32 checksum

---

## File Structure

```
RDT_Project/
├── src/
│   ├── Config.java           # All tunable parameters (window, timeout, loss %)
│   ├── Packet.java           # Packet structure: seq num, data, CRC32 checksum
│   ├── NetworkSimulator.java # Random drop + manual drop + corruption injection
│   ├── Sender.java           # GBN Sender: sliding window, timer, retransmit
│   └── Receiver.java         # GBN Receiver: in-order accept, ACK/NAK sending
├── compile.sh                # Compile all Java files
├── run_receiver.sh           # Start the receiver
├── run_sender.sh             # Start the sender (with optional custom message)
└── README.md                 # This file
```

---

## How to Run

### Step 1 – Compile
```bash
cd src
javac *.java
```

### Step 2 – Start the Receiver (Terminal 1)
```bash
java Receiver
```
Receiver starts listening on **port 9001**.

### Step 3 – Start the Sender (Terminal 2)
```bash
# Default message:
java Sender

# Custom message:
java Sender Hello this is my custom message for transfer
```
Sender starts sending on **port 9000** (ACK listener) → **9001** (data destination).

---

## Configuration (Config.java)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `WINDOW_SIZE` | 4 | GBN sliding window size (N) |
| `MAX_SEQ_NUM` | 8 | Max sequence number (wraps around) |
| `TIMEOUT_MS` | 2000 | Retransmit after 2 seconds of no ACK |
| `LOSS_PROBABILITY` | 0.25 | 25% random packet/ACK loss |
| `CORRUPT_PROBABILITY` | 0.10 | 10% random data corruption |
| `SEND_DELAY_MS` | 300 | Delay between packet sends |

---

## Go-Back-N Protocol — How It Works

```
Sender Window (N=4):
 ┌───────────────────────────────────────────┐
 │  base        nextSeq                      │
 │   ↓            ↓                          │
 │ [0][1][2][3] [4][5][6][7][8]...           │
 │  ←── window ──→  ← not yet sent →         │
 └───────────────────────────────────────────┘

1. Sender sends packets 0,1,2,3 (window full)
2. ACK(0) arrives → window slides: send packet 4
3. Packet 1 lost → TIMEOUT after 2s
4. Sender retransmits 1,2,3,4 (entire window)
5. Receiver discards out-of-order packets
6. Process continues until all packets ACKed
```

### Key Events Visible in Console:
| Symbol | Meaning |
|--------|---------|
| `→ Sent PKT(seq=N)` | Packet sent successfully |
| `↩ RETRANSMIT` | Packet being retransmitted |
| `✔ ACK(seq=N)` | ACK received, window advances |
| `⏰ TIMEOUT!` | Timer expired, retransmit all |
| `✘ CORRUPTED` | CRC mismatch, NAK sent |
| `⚠ Out-of-order` | GBN receiver discards, re-ACKs |
| `🚫 MANUAL DROP` | User manually dropped a packet |
| `✗ RANDOM LOSS` | Simulator randomly dropped |

---

## Manual Drop (Live Demo)

While either terminal is running:
- **Press ENTER** to manually drop the next outgoing packet
- You'll see `[SIM] ⚡ Manual drop armed!` followed by `[SIM] 🚫 MANUAL DROP`
- This lets you demonstrate retransmission **on demand** during viva/demo

---

## Simulation Design

The `NetworkSimulator` class provides two layers:
1. **Random Loss** – configurable probability (default 25%)
2. **Manual Loss** – press ENTER to arm a drop
3. **Corruption** – randomly flips CRC32 checksum (default 10%)

Both the **sender's data packets** and the **receiver's ACK packets** pass through the simulator — simulating bidirectional network unreliability.

---

## Requirements Fulfilled

| Requirement | Implemented |
|------------|-------------|
| UDP socket communication | ✅ |
| Sequence numbering | ✅ |
| ACK-based delivery confirmation | ✅ |
| Timeout and retransmission | ✅ |
| Packet loss simulation (random) | ✅ |
| Packet loss simulation (manual) | ✅ |
| Duplicate packet discard | ✅ |
| In-order delivery at receiver | ✅ |
| Data corruption detection (CRC32) | ✅ |
| Sliding window (Go-Back-N) | ✅ |
| Transfer summary stats | ✅ |
