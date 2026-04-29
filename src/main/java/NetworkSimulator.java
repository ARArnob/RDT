import java.util.Random;

public class NetworkSimulator {

    private final Random random;
    private volatile boolean manualDropNext = false;

    public NetworkSimulator() {
        this.random = new Random();
    }

    public void triggerManualDrop() {
        this.manualDropNext = true;
        ProtocolLogger.log("SIM", "⚡ Manual drop armed! Next packet will be lost.", "info");
    }

    public boolean shouldDrop(String label) {
        // 1. Manual drop (highest priority)
        if (manualDropNext) {
            manualDropNext = false;
            ProtocolLogger.log("SIM", "🚫 MANUAL DROP → " + label, "error");
            return true;
        }

        // 2. Random loss
        if (random.nextDouble() < Config.LOSS_PROBABILITY) {
            ProtocolLogger.log("SIM", "✗ RANDOM LOSS → " + label, "error");
            return true;
        }

        return false;
    }

    public Packet maybeCorrupt(Packet pkt) {
        if (random.nextDouble() < Config.CORRUPT_PROBABILITY) {
            pkt.corrupt();
            ProtocolLogger.log("SIM", "⚠ CORRUPTION injected into " + pkt, "warning");
        }
        return pkt;
    }
}
