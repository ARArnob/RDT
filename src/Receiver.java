import java.io.*;
import java.net.*;

public class Receiver {

    private DatagramSocket socket;
    private InetAddress    senderAddress;
    private NetworkSimulator sim;

    private int rcvBase = 0;                    
    private StringBuilder fullMessage = new StringBuilder();

    public Receiver() throws Exception {
        socket        = new DatagramSocket(Config.RECEIVER_PORT);
        senderAddress = InetAddress.getByName(Config.HOST);
        sim           = new NetworkSimulator();

        printBanner();
        System.out.println(Config.CYAN + Config.BOLD
            + "[RCV] Receiver ready on port " + Config.RECEIVER_PORT
            + " | Expecting seq=" + rcvBase
            + Config.RESET);
    }

    // ── Main receive loop ────────────────────────────────────────────────
    public void start() throws Exception {

        while (true) {
            // ── Receive raw datagram ─────────────────────────────────────
            byte[] buf = new byte[4096];
            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            socket.receive(dp);

            Packet pkt = deserialize(dp.getData(), dp.getLength());
            if (pkt == null) continue;

            System.out.println("\n" + Config.BLUE
                + "[RCV] ← Incoming: " + pkt + Config.RESET);

            // ── Check for termination signal ─────────────────────────────
            if (!pkt.isAck() && !pkt.isNak() && pkt.getData().equals("__END__")) {
                System.out.println(Config.GREEN + Config.BOLD
                    + "\n[RCV] ✔ Transfer complete!"
                    + "\n[RCV] Full message received:\n"
                    + "       \"" + fullMessage.toString() + "\""
                    + Config.RESET);
                sendAck(pkt.getSequenceNumber()); // ACK the END signal
                break;
            }

            // ── Corruption check ─────────────────────────────────────────
            if (pkt.isCorrupted()) {
                System.out.println(Config.RED
                    + "[RCV] ✘ Packet CORRUPTED! Sending NAK for seq=" + pkt.getSequenceNumber()
                    + Config.RESET);
                sendNak(pkt.getSequenceNumber());
                continue;
            }

            // ── In-order packet? ─────────────────────────────────────────
            if (pkt.getSequenceNumber() == rcvBase) {
                fullMessage.append(pkt.getData()).append(" ");
                System.out.println(Config.GREEN
                    + "[RCV] ✔ Accepted seq=" + rcvBase
                    + " | data=\"" + pkt.getData() + "\""
                    + Config.RESET);
                rcvBase = (rcvBase + 1) % Config.MAX_SEQ_NUM;
                sendAck(pkt.getSequenceNumber());

            } else {
                // Out-of-order or duplicate → re-send last good ACK
                int lastGoodAck = (rcvBase - 1 + Config.MAX_SEQ_NUM) % Config.MAX_SEQ_NUM;
                System.out.println(Config.YELLOW
                    + "[RCV] ⚠ Out-of-order/duplicate seq=" + pkt.getSequenceNumber()
                    + " | expected=" + rcvBase
                    + " | Re-ACK seq=" + lastGoodAck
                    + Config.RESET);
                sendAck(lastGoodAck);
            }
        }

        socket.close();
    }

    // ── Send ACK ─────────────────────────────────────────────────────────
    private void sendAck(int seqNum) throws Exception {
        Packet ackPkt = new Packet(seqNum, true, false);
        String label  = "ACK(seq=" + seqNum + ")";

        // Simulate ACK loss
        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(ackPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        System.out.println(Config.CYAN + "[RCV] → Sent " + label + Config.RESET);
    }

    // ── Send NAK ─────────────────────────────────────────────────────────
    private void sendNak(int seqNum) throws Exception {
        Packet nakPkt = new Packet(seqNum, false, true);
        String label  = "NAK(seq=" + seqNum + ")";

        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(nakPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        System.out.println(Config.MAGENTA + "[RCV] → Sent " + label + Config.RESET);
    }

    // ── Serialization helpers ─────────────────────────────────────────────
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
            System.out.println(Config.RED + "[RCV] Failed to deserialize packet." + Config.RESET);
            return null;
        }
    }

    // ── Banner ────────────────────────────────────────────────────────────
    private void printBanner() {
        System.out.println(Config.CYAN + Config.BOLD);
        System.out.println("╔══════════════════════════════════════════╗");
        System.out.println("║   Go-Back-N RDT — RECEIVER               ║");
        System.out.println("║   Green University of Bangladesh | CSE318 ║");
        System.out.println("╚══════════════════════════════════════════╝");
        System.out.println(Config.RESET);
    }

    // ── Entry point ───────────────────────────────────────────────────────
    public static void main(String[] args) {
        try {
            new Receiver().start();
        } catch (Exception e) {
            System.err.println(Config.RED + "[RCV] Fatal error: " + e.getMessage() + Config.RESET);
            e.printStackTrace();
        }
    }
}
