
public class Config {

    // ── Network Settings ─────────────────────────────────────────────────
    public static final String HOST             = "127.0.0.1";
    public static final int    SENDER_PORT      = 9000;  
    public static final int    RECEIVER_PORT    = 9001;  
    // ── Go-Back-N Window ─────────────────────────────────────────────────
    public static final int    WINDOW_SIZE      = 4;     
    public static final int    MAX_SEQ_NUM      = 8;    

    // ── Timing ───────────────────────────────────────────────────────────
    public static final int    TIMEOUT_MS       = 2000;  
    public static final int    SEND_DELAY_MS    = 300;   

    // ── Network Simulation ───────────────────────────────────────────────
    public static final double LOSS_PROBABILITY = 0.25;  
    public static final double CORRUPT_PROBABILITY = 0.10; 

    // ── Display ──────────────────────────────────────────────────────────
    public static final boolean COLORED_OUTPUT  = true;  

    // ── ANSI Color Codes ─────────────────────────────────────────────────
    public static final String RESET   = "\u001B[0m";
    public static final String RED     = "\u001B[31m";
    public static final String GREEN   = "\u001B[32m";
    public static final String YELLOW  = "\u001B[33m";
    public static final String BLUE    = "\u001B[34m";
    public static final String CYAN    = "\u001B[36m";
    public static final String MAGENTA = "\u001B[35m";
    public static final String BOLD    = "\u001B[1m";
    public static final String WHITE   = "\u001B[37m";
}
