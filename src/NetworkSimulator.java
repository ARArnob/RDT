import java.util.Random;

public class NetworkSimulator {

    private final Random random;
    private volatile boolean manualDropNext = false;

    public NetworkSimulator() {
        this.random = new Random();
    }

    public void triggerManualDrop() {
        this.manualDropNext = true;
        ProtocolLogger.logEvent("SIM", "[!] Manual drop armed! Next packet will be lost.", "info");
    }

    public boolean shouldDrop(String label) {
        if (manualDropNext) {
            manualDropNext = false;
            ProtocolLogger.logEvent("SIM", "[DROP] MANUAL DROP -> " + label, "error");
            return true;
        }

        if (random.nextDouble() < Config.LOSS_PROBABILITY) {
            ProtocolLogger.logEvent("SIM", "[DROP] RANDOM LOSS -> " + label, "error");
            return true;
        }

        return false;
    }

    public Packet maybeCorrupt(Packet pkt) {
        if (random.nextDouble() < Config.CORRUPT_PROBABILITY) {
            pkt.corrupt();
            ProtocolLogger.logEvent("SIM", "[WARN] CORRUPTION injected into " + pkt, "warning");
        }
        return pkt;
    }
}
