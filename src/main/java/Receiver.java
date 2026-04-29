import java.io.*;
import java.net.*;

public class Receiver {

    private DatagramSocket socket;
    private InetAddress senderAddress;
    private NetworkSimulator sim;

    private int rcvBase = 0;
    private StringBuilder fullMessage = new StringBuilder();
    private volatile boolean running = true;

    public Receiver(NetworkSimulator simulator) throws Exception {
        socket = new DatagramSocket(Config.RECEIVER_PORT);
        senderAddress = InetAddress.getByName(Config.HOST);
        sim = simulator;

        ProtocolLogger.log("RCV", "Receiver ready on port " + Config.RECEIVER_PORT + " | Expecting seq=" + rcvBase, "info");
    }

    // ── Main receive loop ────────────────────────────────────────────────
    public void start() {
        try {
            while (running) {
                // ── Receive raw datagram ─────────────────────────────────────
                byte[] buf = new byte[4096];
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                socket.receive(dp);

                Packet pkt = deserialize(dp.getData(), dp.getLength());
                if (pkt == null) continue;

                ProtocolLogger.log("RCV", "← Incoming: " + pkt, "info");

                // ── Check for termination signal ─────────────────────────────
                if (!pkt.isAck() && !pkt.isNak() && "__END__".equals(pkt.getData())) {
                    ProtocolLogger.log("RCV", "✔ Transfer complete! Full message: \"" + fullMessage.toString().trim() + "\"", "success");
                    sendAck(pkt.getSequenceNumber()); // ACK the END signal
                    continue; // Keep running for future messages
                }

                // ── Corruption check ─────────────────────────────────────────
                if (pkt.isCorrupted()) {
                    ProtocolLogger.log("RCV", "✘ Packet CORRUPTED! Sending NAK for seq=" + pkt.getSequenceNumber(), "error");
                    sendNak(pkt.getSequenceNumber());
                    continue;
                }

                // ── In-order packet? ─────────────────────────────────────────
                if (pkt.getSequenceNumber() == rcvBase) {
                    fullMessage.append(pkt.getData()).append(" ");
                    ProtocolLogger.log("RCV", "✔ Accepted seq=" + rcvBase + " | data=\"" + pkt.getData() + "\"", "success");
                    rcvBase = (rcvBase + 1) % Config.MAX_SEQ_NUM;
                    sendAck(pkt.getSequenceNumber());

                } else {
                    // Out-of-order or duplicate → re-send last good ACK
                    int lastGoodAck = (rcvBase - 1 + Config.MAX_SEQ_NUM) % Config.MAX_SEQ_NUM;
                    ProtocolLogger.log("RCV", "⚠ Out-of-order/duplicate seq=" + pkt.getSequenceNumber() + " | expected=" + rcvBase + " | Re-ACK seq=" + lastGoodAck, "warning");
                    sendAck(lastGoodAck);
                }
            }
        } catch (Exception e) {
            if (running) {
                ProtocolLogger.log("RCV", "Error in receiver loop: " + e.getMessage(), "error");
            }
        } finally {
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }

    public void stop() {
        running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }

    // ── Send ACK ─────────────────────────────────────────────────────────
    private void sendAck(int seqNum) throws Exception {
        Packet ackPkt = new Packet(seqNum, true, false);
        String label = "ACK(seq=" + seqNum + ")";

        // Simulate ACK loss
        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(ackPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        ProtocolLogger.log("RCV", "→ Sent " + label, "info");
    }

    // ── Send NAK ─────────────────────────────────────────────────────────
    private void sendNak(int seqNum) throws Exception {
        Packet nakPkt = new Packet(seqNum, false, true);
        String label = "NAK(seq=" + seqNum + ")";

        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(nakPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        ProtocolLogger.log("RCV", "→ Sent " + label, "warning");
    }

    // ── Serialization helpers ─────────────────────────────────────────────
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
            ProtocolLogger.log("RCV", "Failed to deserialize packet.", "error");
            return null;
        }
    }
}
