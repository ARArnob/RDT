import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Sender {

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

    private final AtomicInteger totalSent = new AtomicInteger(0);
    private final AtomicInteger totalRetx = new AtomicInteger(0);
    private final AtomicInteger totalDropped = new AtomicInteger(0);

    public Sender(String message, NetworkSimulator simulator) throws Exception {
        String[] words = message.trim().split("\\s+");
        packets = words;

        sendSocket = new DatagramSocket();
        ackSocket = new DatagramSocket(Config.SENDER_PORT);
        receiverAddress = InetAddress.getByName(Config.HOST);
        sim = simulator;
        timer = Executors.newSingleThreadScheduledExecutor();

        ProtocolLogger.logEvent("SND", "Message split into " + packets.length + " packets.", "info");
        for (int i = 0; i < packets.length; i++) {
            ProtocolLogger.logEvent("SND", "      [" + i + "] " + packets[i], "info");
        }
        ProtocolLogger.logEvent("SND", "Window Size = " + Config.WINDOW_SIZE + " | Timeout = " + Config.TIMEOUT_MS + "ms | Loss = " + (int)(Config.LOSS_PROBABILITY*100) + "%", "info");
    }

    public void start() throws Exception {
        Thread ackThread = new Thread(this::listenForAcks);
        ackThread.setDaemon(true);
        ackThread.start();

        while (!allAcked) {
            synchronized (lock) {
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

        sendTermination();
        printStats();

        timer.shutdownNow();
        sendSocket.close();
        ackSocket.close();
    }

    private void sendPacket(int seqNum, String data, boolean isRetransmit) throws Exception {
        Packet pkt = new Packet(seqNum % Config.MAX_SEQ_NUM, data);
        pkt = sim.maybeCorrupt(pkt);
        String label = "PKT(seq=" + (seqNum % Config.MAX_SEQ_NUM) + ", " + data + ")";

        if (sim.shouldDrop(label)) {
            totalDropped.incrementAndGet();
            return;
        }

        byte[] bytes = serialize(pkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);

        totalSent.incrementAndGet();
        if (isRetransmit) totalRetx.incrementAndGet();

        String tag = isRetransmit ? "RETRANSMIT " : "Sent ";
        ProtocolLogger.logEvent("SND", tag + label + " | window=[" + (base % Config.MAX_SEQ_NUM) + ".." + ((base + Config.WINDOW_SIZE - 1) % Config.MAX_SEQ_NUM) + "]", "success");
    }

    private void listenForAcks() {
        while (!allAcked) {
            try {
                byte[] buf = new byte[4096];
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                ackSocket.receive(dp);

                Packet ack = deserialize(dp.getData(), dp.getLength());
                if (ack == null) continue;

                if (ack.isNak()) {
                    ProtocolLogger.logEvent("SND", "NAK(seq=" + ack.getSequenceNumber() + ") received -> retransmitting window", "warning");
                    synchronized (lock) { retransmitWindow(); }
                    continue;
                }

                if (ack.isAck()) {
                    int ackNum = ack.getSequenceNumber();
                    ProtocolLogger.logEvent("SND", "ACK(seq=" + ackNum + ") received", "success");

                    synchronized (lock) {
                        int windowLimit = Math.min(nextSeq, base + Config.WINDOW_SIZE);
                        for (int i = base; i < windowLimit; i++) {
                            if (i % Config.MAX_SEQ_NUM == ackNum) {
                                base = i + 1;
                                break;
                            }
                        }

                        ProtocolLogger.logEvent("SND", "  Window advanced -> base=" + (base % Config.MAX_SEQ_NUM), "info");

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
                if (!allAcked) ProtocolLogger.logEvent("SND", "ACK socket error: " + e.getMessage(), "error");
            }
        }
    }

    private void startTimer() {
        timerRunning = true;
        timerTask = timer.schedule(() -> {
            ProtocolLogger.logEvent("SND", "TIMEOUT! Retransmitting entire window...", "error");
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

    private void retransmitWindow() {
        ProtocolLogger.logEvent("SND", "Retransmitting window [" + (base % Config.MAX_SEQ_NUM) + ".." + ((nextSeq - 1) % Config.MAX_SEQ_NUM) + "]", "warning");
        try {
            for (int i = base; i < nextSeq; i++) {
                sendPacket(i, packets[i % packets.length], true);
                Thread.sleep(100);
            }
        } catch (Exception e) {
            ProtocolLogger.logEvent("SND", "Retransmit error: " + e.getMessage(), "error");
        }
        restartTimer();
    }

    private void sendTermination() throws Exception {
        ProtocolLogger.logEvent("SND", "Sending END signal...", "info");
        Packet endPkt = new Packet(nextSeq % Config.MAX_SEQ_NUM, "__END__");
        byte[] bytes = serialize(endPkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);
        Thread.sleep(500);
    }

    private void printStats() {
        ProtocolLogger.logEvent("SND", "Total packets sent: " + totalSent.get(), "info");
        ProtocolLogger.logEvent("SND", "Retransmissions: " + totalRetx.get(), "info");
        ProtocolLogger.logEvent("SND", "Packets dropped (sim): " + totalDropped.get(), "info");
        ProtocolLogger.logEvent("SND", "Unique data packets: " + packets.length, "info");
    }

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
            ProtocolLogger.logEvent("SND", "Deserialize error: " + e.getMessage(), "error");
            return null;
        }
    }
}
