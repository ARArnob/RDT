import io.javalin.websocket.WsContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class ProtocolLogger {
    private static final Set<WsContext> clients = ConcurrentHashMap.newKeySet();
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void addClient(WsContext ctx) {
        clients.add(ctx);
    }

    public static void removeClient(WsContext ctx) {
        clients.remove(ctx);
    }

    public static void log(String source, String message, String type) {
        try {
            LogEvent event = new LogEvent(source, message, LocalTime.now().format(TIME_FORMATTER), type);
            String json = mapper.writeValueAsString(event);
            for (WsContext ctx : clients) {
                if (ctx.session.isOpen()) {
                    ctx.send(json);
                }
            }
        } catch (Exception e) {
            // Log to standard error only if serialization fails
            e.printStackTrace();
        }
    }

    public static class LogEvent {
        public String source;
        public String message;
        public String timestamp;
        public String type;

        public LogEvent(String source, String message, String timestamp, String type) {
            this.source = source;
            this.message = message;
            this.timestamp = timestamp;
            this.type = type;
        }
    }
}
