import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Sender {

    // ── State ──────────────────────────────────────────────────────────────
    private final String[] packets;
    private int base = 0;
    private int nextSeq = 0;
    private final Object lock = new Object();

    private volatile boolean allAcked = false;
    private volatile boolean timerRunning = false;

    private ScheduledExecutorService timer;
    private ScheduledFuture<?> timerTask;

    private DatagramSocket sendSocket;
    private DatagramSocket ackSocket;
    private InetAddress receiverAddress;

    private NetworkSimulator sim;

    // Stats
    private final AtomicInteger totalSent = new AtomicInteger(0);
    private final AtomicInteger totalRetx = new AtomicInteger(0);
    private final AtomicInteger totalDropped = new AtomicInteger(0);

    // ── Constructor ────────────────────────────────────────────────────────
    public Sender(String message, NetworkSimulator simulator) throws Exception {
        // Split message into word-level packets
        String[] words = message.trim().split("\\s+");
        packets = words;

        sendSocket = new DatagramSocket();
        ackSocket = new DatagramSocket(Config.SENDER_PORT);
        receiverAddress = InetAddress.getByName(Config.HOST);
        sim = simulator;
        timer = Executors.newSingleThreadScheduledExecutor();

        ProtocolLogger.log("SND", "Message split into " + packets.length + " packets.", "info");
        for (int i = 0; i < packets.length; i++) {
            ProtocolLogger.log("SND", "      [" + i + "] \"" + packets[i] + "\"", "info");
        }
        ProtocolLogger.log("SND", "Window Size = " + Config.WINDOW_SIZE + " | Timeout = " + Config.TIMEOUT_MS + "ms | Loss = " + (int)(Config.LOSS_PROBABILITY*100) + "%", "info");
    }

    // ── Main send loop ─────────────────────────────────────────────────────
    public void start() throws Exception {

        // Start ACK listener thread
        Thread ackThread = new Thread(this::listenForAcks);
        ackThread.setDaemon(true);
        ackThread.start();

        // Main send loop
        while (!allAcked) {
            synchronized (lock) {
                // Send all packets that fit in the window
                while (nextSeq < packets.length && nextSeq < base + Config.WINDOW_SIZE) {
                    sendPacket(nextSeq, packets[nextSeq % packets.length], false);

                    if (!timerRunning) {
                        startTimer();
                    }
                    nextSeq++;
                }
            }
            Thread.sleep(Config.SEND_DELAY_MS);
        }

        // Send termination signal
        sendTermination();
        printStats();

        timer.shutdownNow();
        sendSocket.close();
        ackSocket.close();
    }

    // ── Send a single packet
    private void sendPacket(int seqNum, String data, boolean isRetransmit) throws Exception {
        Packet pkt = new Packet(seqNum % Config.MAX_SEQ_NUM, data);
        pkt = sim.maybeCorrupt(pkt);
        String label = "PKT(seq=" + (seqNum % Config.MAX_SEQ_NUM) + ", \"" + data + "\")";

        if (sim.shouldDrop(label)) {
            totalDropped.incrementAndGet();
            return;
        }

        byte[] bytes = serialize(pkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);

        totalSent.incrementAndGet();
        if (isRetransmit) totalRetx.incrementAndGet();

        String tag = isRetransmit ? "↩ RETRANSMIT " : "→ Sent       ";

        ProtocolLogger.log("SND", tag + label + " | window=[" + (base % Config.MAX_SEQ_NUM) + ".." + ((base + Config.WINDOW_SIZE - 1) % Config.MAX_SEQ_NUM) + "]", "success");
    }

    // ── ACK listener
    private void listenForAcks() {
        while (!allAcked) {
            try {
                byte[] buf = new byte[4096];
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                ackSocket.receive(dp);

                Packet ack = deserialize(dp.getData(), dp.getLength());
                if (ack == null) continue;

                if (ack.isNak()) {
                    ProtocolLogger.log("SND", "← NAK(seq=" + ack.getSequenceNumber() + ") received → retransmitting window", "warning");
                    synchronized (lock) { retransmitWindow(); }
                    continue;
                }

                if (ack.isAck()) {
                    int ackNum = ack.getSequenceNumber();
                    ProtocolLogger.log("SND", "← ACK(seq=" + ackNum + ") received", "success");

                    synchronized (lock) {
                        int windowLimit = Math.min(nextSeq, base + Config.WINDOW_SIZE);
                        for (int i = base; i < windowLimit; i++) {
                            if (i % Config.MAX_SEQ_NUM == ackNum) {
                                base = i + 1;
                                break;
                            }
                        }

                        ProtocolLogger.log("SND", "  Window advanced → base=" + (base % Config.MAX_SEQ_NUM), "info");

                        if (base >= packets.length) {
                            allAcked = true;
                            stopTimer();
                        } else if (base == nextSeq) {
                            stopTimer();
                        } else {
                            restartTimer();
                        }
                    }
                }

            } catch (Exception e) {
                if (!allAcked)
                    ProtocolLogger.log("SND", "ACK socket error: " + e.getMessage(), "error");
            }
        }
    }

    // ── Timer management
    private void startTimer() {
        timerRunning = true;
        timerTask = timer.schedule(() -> {
            ProtocolLogger.log("SND", "⏰ TIMEOUT! Retransmitting entire window...", "error");
            synchronized (lock) { retransmitWindow(); }
        }, Config.TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    private void stopTimer() {
        if (timerTask != null) timerTask.cancel(false);
        timerRunning = false;
    }

    private void restartTimer() {
        stopTimer();
        startTimer();
    }

    // ── Retransmit all unACKed packets in window
    private void retransmitWindow() {
        ProtocolLogger.log("SND", "Retransmitting window [" + (base % Config.MAX_SEQ_NUM) + ".." + ((nextSeq - 1) % Config.MAX_SEQ_NUM) + "]", "warning");
        try {
            for (int i = base; i < nextSeq; i++) {
                sendPacket(i, packets[i % packets.length], true);
                Thread.sleep(100);
            }
        } catch (Exception e) {
            ProtocolLogger.log("SND", "Retransmit error: " + e.getMessage(), "error");
        }
        restartTimer();
    }

    // ── Send __END__ termination packet
    private void sendTermination() throws Exception {
        ProtocolLogger.log("SND", "Sending END signal...", "info");
        Packet endPkt = new Packet(nextSeq % Config.MAX_SEQ_NUM, "__END__");
        byte[] bytes = serialize(endPkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);
        Thread.sleep(500);
    }

    // ── Stats
    private void printStats() {
        ProtocolLogger.log("SND", "Total packets sent: " + totalSent.get(), "info");
        ProtocolLogger.log("SND", "Retransmissions: " + totalRetx.get(), "info");
        ProtocolLogger.log("SND", "Packets dropped (sim): " + totalDropped.get(), "info");
        ProtocolLogger.log("SND", "Unique data packets: " + packets.length, "info");
    }

    // ── Serialization helpers
    private byte[] serialize(Packet pkt) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(pkt);
        oos.flush();
        return bos.toByteArray();
    }

    private Packet deserialize(byte[] data, int length) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data, 0, length);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return (Packet) ois.readObject();
        } catch (Exception e) {
            ProtocolLogger.log("SND", "Deserialize error: " + e.getMessage(), "error");
            return null;
        }
    }
}
