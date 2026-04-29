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

        ProtocolLogger.logEvent("RCV", "Receiver ready on port " + Config.RECEIVER_PORT + " | Expecting seq=" + rcvBase, "info");
    }

    public void start() {
        try {
            while (running) {
                byte[] buf = new byte[4096];
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                socket.receive(dp);

                Packet pkt = deserialize(dp.getData(), dp.getLength());
                if (pkt == null) continue;

                ProtocolLogger.logEvent("RCV", "Incoming: " + pkt, "info");

                if (!pkt.isAck() && !pkt.isNak() && "__END__".equals(pkt.getData())) {
                    ProtocolLogger.logEvent("RCV", "Transfer complete! Full message: " + fullMessage.toString().trim(), "success");
                    sendAck(pkt.getSequenceNumber());
                    continue; 
                }

                if (pkt.isCorrupted()) {
                    ProtocolLogger.logEvent("RCV", "Packet CORRUPTED! Sending NAK for seq=" + pkt.getSequenceNumber(), "error");
                    sendNak(pkt.getSequenceNumber());
                    continue;
                }

                if (pkt.getSequenceNumber() == rcvBase) {
                    fullMessage.append(pkt.getData()).append(" ");
                    ProtocolLogger.logEvent("RCV", "Accepted seq=" + rcvBase + " | data=" + pkt.getData(), "success");
                    rcvBase = (rcvBase + 1) % Config.MAX_SEQ_NUM;
                    sendAck(pkt.getSequenceNumber());

                } else {
                    int lastGoodAck = (rcvBase - 1 + Config.MAX_SEQ_NUM) % Config.MAX_SEQ_NUM;
                    ProtocolLogger.logEvent("RCV", "Out-of-order/duplicate seq=" + pkt.getSequenceNumber() + " | expected=" + rcvBase + " | Re-ACK seq=" + lastGoodAck, "warning");
                    sendAck(lastGoodAck);
                }
            }
        } catch (Exception e) {
            if (running) {
                ProtocolLogger.logEvent("RCV", "Error in receiver loop: " + e.getMessage(), "error");
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

    private void sendAck(int seqNum) throws Exception {
        Packet ackPkt = new Packet(seqNum, true, false);
        String label = "ACK(seq=" + seqNum + ")";

        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(ackPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        ProtocolLogger.logEvent("RCV", "Sent " + label, "info");
    }

    private void sendNak(int seqNum) throws Exception {
        Packet nakPkt = new Packet(seqNum, false, true);
        String label = "NAK(seq=" + seqNum + ")";

        if (sim.shouldDrop(label)) return;

        byte[] data = serialize(nakPkt);
        DatagramPacket dp = new DatagramPacket(data, data.length, senderAddress, Config.SENDER_PORT);
        socket.send(dp);
        ProtocolLogger.logEvent("RCV", "Sent " + label, "warning");
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
            ProtocolLogger.logEvent("RCV", "Failed to deserialize packet.", "error");
            return null;
        }
    }
}
