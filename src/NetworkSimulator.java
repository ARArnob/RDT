import java.util.Random;
import java.util.Scanner;

public class NetworkSimulator {

    private final Random  random;
    private volatile boolean manualDropNext = false;  
    private final Thread manualDropThread;

    public NetworkSimulator() {
        this.random = new Random();

        // Background thread: user presses ENTER → next packet is dropped
        manualDropThread = new Thread(() -> {
            Scanner sc = new Scanner(System.in);
            System.out.println(Config.YELLOW
                + "[SIM] Press ENTER at any time to manually drop the next packet."
                + Config.RESET);
            while (true) {
                sc.nextLine();
                manualDropNext = true;
                System.out.println(Config.MAGENTA
                    + "[SIM] ⚡ Manual drop armed! Next packet will be lost."
                    + Config.RESET);
            }
        });
        manualDropThread.setDaemon(true); 
        manualDropThread.start();
    }

    public boolean shouldDrop(String label) {

        // 1. Manual drop (highest priority)
        if (manualDropNext) {
            manualDropNext = false;
            System.out.println(Config.MAGENTA
                + "[SIM] 🚫 MANUAL DROP → " + label + Config.RESET);
            return true;
        }

        // 2. Random loss
        if (random.nextDouble() < Config.LOSS_PROBABILITY) {
            System.out.println(Config.RED
                + "[SIM] ✗ RANDOM LOSS → " + label + Config.RESET);
            return true;
        }

        return false;
    }

    public Packet maybeCorrupt(Packet pkt) {
        if (random.nextDouble() < Config.CORRUPT_PROBABILITY) {
            pkt.corrupt();
            System.out.println(Config.YELLOW
                + "[SIM] ⚠ CORRUPTION injected into " + pkt + Config.RESET);
        }
        return pkt;
    }
}
