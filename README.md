# Reliable Data Transfer Protocol
### Go-Back-N Implementation over UDP with SSE Web Dashboard

## Project Overview

This project implements a **Reliable Data Transfer (RDT) Protocol** using the **Go-Back-N (GBN)** algorithm on top of UDP sockets in Java. It features a modern **Web Dashboard** powered by Server-Sent Events (SSE) for real-time telemetry, allowing you to observe and control the simulation visually.

Since UDP does not provide reliability by default, all reliability mechanisms are built from scratch:
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
│   ├── Sender.java           # GBN Sender logic
│   ├── Receiver.java         # GBN Receiver logic
│   ├── ProtocolLogger.java   # Central logger for broadcasting events via SSE
│   ├── RdtWebServer.java     # Zero-dependency HTTP & SSE Server (Entry Point)
│   └── index.html            # Web Dashboard UI
├── compile.sh                # Script to compile all Java files
└── README.md                 # This file
```

---

## How to Run

The project is orchestrated via a zero-dependency Web Server.

### Method 1: Using the Script (Linux/Mac/Git Bash)
The provided script compiles the project and starts the server automatically.
```bash
./compile.sh
```

### Method 2: Manual (Windows/All)
1. **Compile the Project**:
   ```bash
   cd src
   javac *.java
   ```
2. **Start the Web Server**:
   ```bash
   java RdtWebServer
   ```

### Access the Dashboard
Open your browser and navigate to **`http://localhost:8080`**.

---

## Web Interface Features

From the web dashboard, you can interact with the protocol simulation in real-time:

- **Start Transfer**: Type a custom message and click "Start Transfer". The server will instantiate the Sender and begin the GBN protocol.
- **Force Drop**: Click the "⚡ Force Drop" button to arm a manual packet drop. The next packet (either Data or ACK) will be dropped by the simulator, allowing you to observe retransmission behavior on demand.
- **Real-time Logs**: Watch the protocol state machine live. Logs are color-coded by source:
  - **`SND`**: Sender events (Sent, Retransmit, ACK received)
  - **`RCV`**: Receiver events (Incoming, Accepted, Sent ACK/NAK)
  - **`SIM`**: Network Simulator events (Random Loss, Corruption, Manual Drop)
  - **`SYS`**: System events

---

## Configuration (`Config.java`)

You can tune the protocol behavior by editing `src/Config.java`.

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
