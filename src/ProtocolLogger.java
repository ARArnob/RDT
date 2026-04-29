import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProtocolLogger {
    private static final CopyOnWriteArrayList<HttpExchange> clients = new CopyOnWriteArrayList<>();
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void addClient(HttpExchange exchange) {
        clients.add(exchange);
    }

    public static void removeClient(HttpExchange exchange) {
        clients.remove(exchange);
    }

    public static void logEvent(String source, String message, String color) {
        String timestamp = LocalTime.now().format(TIME_FORMATTER);
        String escapedMessage = message.replace("\"", "\\\"").replace("\n", "\\n");
        String json = String.format("{\"source\":\"%s\", \"message\":\"%s\", \"color\":\"%s\", \"timestamp\":\"%s\"}", 
                                    source, escapedMessage, color, timestamp);
        
        String sseData = "data: " + json + "\n\n";
        byte[] bytes = sseData.getBytes();

        for (HttpExchange client : clients) {
            try {
                OutputStream os = client.getResponseBody();
                os.write(bytes);
                os.flush();
            } catch (IOException e) {
                removeClient(client);
            }
        }
    }
}
