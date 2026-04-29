import java.io.Serializable;
import java.util.zip.CRC32;

public class Packet implements Serializable {

    private static final long serialVersionUID = 1L;

    private int sequenceNumber;
    private String data;
    private long checksum;
    private boolean isAck;
    private boolean isNak;

    // ── Constructor for DATA packet ──────────────────────────────────────
    public Packet(int sequenceNumber, String data) {
        this.sequenceNumber = sequenceNumber;
        this.data           = data;
        this.isAck          = false;
        this.isNak          = false;
        this.checksum       = computeChecksum(sequenceNumber, data);
    }

    // ── Constructor for ACK / NAK packet ────────────────────────────────
    public Packet(int sequenceNumber, boolean isAck, boolean isNak) {
        this.sequenceNumber = sequenceNumber;
        this.data           = "";
        this.isAck          = isAck;
        this.isNak          = isNak;
        this.checksum       = computeChecksum(sequenceNumber, "");
    }

    // ── Checksum computation (CRC32) ─────────────────────────────────────
    private long computeChecksum(int seqNum, String data) {
        CRC32 crc = new CRC32();
        crc.update(String.valueOf(seqNum).getBytes());
        if (data != null) crc.update(data.getBytes());
        return crc.getValue();
    }

    // ── Validate integrity ───────────────────────────────────────────────
    public boolean isCorrupted() {
        long expected = computeChecksum(sequenceNumber, data);
        return expected != this.checksum;
    }

    // ── Corrupt packet (for simulation) ─────────────────────────────────
    public void corrupt() {
        this.checksum = this.checksum + 1; // flip checksum to simulate corruption
    }

    // ── Getters ──────────────────────────────────────────────────────────
    public int    getSequenceNumber() { return sequenceNumber; }
    public String getData()           { return data; }
    public long   getChecksum()       { return checksum; }
    public boolean isAck()            { return isAck; }
    public boolean isNak()            { return isNak; }

    @Override
    public String toString() {
        if (isAck) return "[ACK | seq=" + sequenceNumber + "]";
        if (isNak) return "[NAK | seq=" + sequenceNumber + "]";
        return "[PKT | seq=" + sequenceNumber + " | data=\"" + data + "\" | crc=" + checksum + "]";
    }
}
