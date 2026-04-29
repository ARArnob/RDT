import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Sender {

    // ── State ──────────────────────────────────────────────────────────────
    private final String[]  packets;          
    private int             base      = 0;   
    private int             nextSeq   = 0;   
    private final Object    lock      = new Object();

    private volatile boolean allAcked  = false;
    private volatile boolean timerRunning = false;

    private ScheduledExecutorService  timer;
    private ScheduledFuture<?>        timerTask;

    private DatagramSocket  sendSocket;       
    private DatagramSocket  ackSocket;        
    private InetAddress     receiverAddress;

    private NetworkSimulator sim;

    // Stats
    private final AtomicInteger totalSent   = new AtomicInteger(0);
    private final AtomicInteger totalRetx   = new AtomicInteger(0);
    private final AtomicInteger totalDropped = new AtomicInteger(0);

    // ── Constructor ────────────────────────────────────────────────────────
    public Sender(String message) throws Exception {
        // Split message into word-level packets
        String[] words = message.trim().split("\\s+");
        packets = words;

        sendSocket      = new DatagramSocket();
        ackSocket       = new DatagramSocket(Config.SENDER_PORT);
        receiverAddress = InetAddress.getByName(Config.HOST);
        sim             = new NetworkSimulator();
        timer           = Executors.newSingleThreadScheduledExecutor();

        printBanner();
        System.out.println(Config.GREEN + Config.BOLD
            + "[SND] Message split into " + packets.length + " packets:"
            + Config.RESET);
        for (int i = 0; i < packets.length; i++) {
            System.out.println(Config.GREEN
                + "      [" + i + "] \"" + packets[i] + "\"" + Config.RESET);
        }
        System.out.println(Config.GREEN
            + "[SND] Window Size = " + Config.WINDOW_SIZE
            + " | Timeout = " + Config.TIMEOUT_MS + "ms"
            + " | Loss = " + (int)(Config.LOSS_PROBABILITY*100) + "%"
            + Config.RESET);
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
                while (nextSeq < packets.length
                       && nextSeq < base + Config.WINDOW_SIZE) {

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
        Packet pkt    = new Packet(seqNum % Config.MAX_SEQ_NUM, data);
        pkt            = sim.maybeCorrupt(pkt);            // maybe corrupt
        String label   = "PKT(seq=" + (seqNum % Config.MAX_SEQ_NUM) + ", \"" + data + "\")";

        if (sim.shouldDrop(label)) {
            totalDropped.incrementAndGet();
            return;
        }

        byte[] bytes = serialize(pkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);

        totalSent.incrementAndGet();
        if (isRetransmit) totalRetx.incrementAndGet();

        String tag = isRetransmit
            ? Config.MAGENTA + "[SND] ↩ RETRANSMIT "
            : Config.GREEN   + "[SND] → Sent       ";

        System.out.println(tag + label
            + " | window=[" + (base % Config.MAX_SEQ_NUM)
            + ".." + ((base + Config.WINDOW_SIZE - 1) % Config.MAX_SEQ_NUM) + "]"
            + Config.RESET);
    }

    // ── ACK listener 
    private void listenForAcks() {
        while (!allAcked) {
            try {
                byte[]         buf = new byte[4096];
                DatagramPacket dp  = new DatagramPacket(buf, buf.length);
                ackSocket.receive(dp);

                Packet ack = deserialize(dp.getData(), dp.getLength());
                if (ack == null) continue;

                if (ack.isNak()) {
                    System.out.println(Config.RED
                        + "[SND] ← NAK(seq=" + ack.getSequenceNumber()
                        + ") received → retransmitting window"
                        + Config.RESET);
                    synchronized (lock) { retransmitWindow(); }
                    continue;
                }

                if (ack.isAck()) {
                    int ackNum = ack.getSequenceNumber();
                    System.out.println(Config.CYAN
                        + "[SND] ← ACK(seq=" + ackNum + ") received"
                        + Config.RESET);

                    synchronized (lock) {
                        // Find how many packets this ACK covers (cumulative)
                        int baseSeqMod = base % Config.MAX_SEQ_NUM;

                        // advance base until we pass the acked sequence number
                        int windowLimit = Math.min(nextSeq, base + Config.WINDOW_SIZE);
                        for (int i = base; i < windowLimit; i++) {
                            if (i % Config.MAX_SEQ_NUM == ackNum) {
                                base = i + 1;
                                break;
                            }
                        }

                        System.out.println(Config.CYAN
                            + "[SND]   Window advanced → base=" + (base % Config.MAX_SEQ_NUM)
                            + Config.RESET);

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
                    System.out.println(Config.RED + "[SND] ACK socket error: " + e.getMessage() + Config.RESET);
            }
        }
    }

    // ── Timer management 
    private void startTimer() {
        timerRunning = true;
        timerTask = timer.schedule(() -> {
            System.out.println(Config.RED + Config.BOLD
                + "\n[SND] ⏰ TIMEOUT! Retransmitting entire window..."
                + Config.RESET);
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
        System.out.println(Config.MAGENTA
            + "[SND] Retransmitting window [" + (base % Config.MAX_SEQ_NUM)
            + ".." + ((nextSeq - 1) % Config.MAX_SEQ_NUM) + "]"
            + Config.RESET);
        try {
            for (int i = base; i < nextSeq; i++) {
                sendPacket(i, packets[i % packets.length], true);
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.out.println(Config.RED + "[SND] Retransmit error: " + e.getMessage() + Config.RESET);
        }
        restartTimer();
    }

    // ── Send __END__ termination packet 
    private void sendTermination() throws Exception {
        System.out.println(Config.GREEN + Config.BOLD
            + "\n[SND] Sending END signal..." + Config.RESET);
        Packet endPkt = new Packet(nextSeq % Config.MAX_SEQ_NUM, "__END__");
        byte[] bytes  = serialize(endPkt);
        DatagramPacket dp = new DatagramPacket(bytes, bytes.length, receiverAddress, Config.RECEIVER_PORT);
        sendSocket.send(dp);
        Thread.sleep(500); 
    }

    // ── Stats ─────────────────────────────────────────────────────────────
    private void printStats() {
        System.out.println(Config.BOLD + Config.WHITE);
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║              TRANSFER SUMMARY            ║");
        System.out.printf ("║  Total packets sent   : %-16d  ║%n", totalSent.get());
        System.out.printf ("║  Retransmissions      : %-16d  ║%n", totalRetx.get());
        System.out.printf ("║  Packets dropped(sim) : %-16d  ║%n", totalDropped.get());
        System.out.printf ("║  Unique data packets  : %-16d  ║%n", packets.length);
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println(Config.RESET);
    }

    // ── Serialization helpers 
    private byte[] serialize(Packet pkt) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream    oos = new ObjectOutputStream(bos);
        oos.writeObject(pkt);
        oos.flush();
        return bos.toByteArray();
    }

    private Packet deserialize(byte[] data, int length) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(data, 0, length);
            ObjectInputStream    ois = new ObjectInputStream(bis);
            return (Packet) ois.readObject();
        } catch (Exception e) {
            System.out.println(Config.RED + "[SND] Deserialize error: " + e.getMessage() + Config.RESET);
            return null;
        }
    }

    // ── Banner 
    private void printBanner() {
        System.out.println(Config.GREEN + Config.BOLD);
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║   Go-Back-N RDT — SENDER                 ║");
        System.out.println("║   Green University of Bangladesh | CSE318 ║");
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println(Config.RESET);
    }

    // ── Entry point 
    public static void main(String[] args) {
        try {
            String message = args.length > 0
                ? String.join(" ", args)
                : "Hello this is a reliable data transfer protocol using Go-Back-N over UDP";

            new Sender(message).start();
        } catch (Exception e) {
            System.err.println(Config.RED + "[SND] Fatal: " + e.getMessage() + Config.RESET);
            e.printStackTrace();
        }
    }
}
