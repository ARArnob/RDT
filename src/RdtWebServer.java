import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;

public class RdtWebServer {

    private static NetworkSimulator simulator;
    private static Receiver receiver;
    private static volatile boolean senderActive = false;

    public static void main(String[] args) throws IOException {
        simulator = new NetworkSimulator();
        
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        
        server.createContext("/", new StaticHandler());
        server.createContext("/events", new SSEHandler());
        server.createContext("/api/drop", new DropHandler());
        server.createContext("/api/send", new SendHandler());

        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.start();

        try {
            receiver = new Receiver(simulator);
            Thread receiverThread = new Thread(() -> {
                try {
                    receiver.start();
                } catch (Exception e) {
                    ProtocolLogger.logEvent("SYS", "Receiver error: " + e.getMessage(), "error");
                }
            });
            receiverThread.setDaemon(true);
            receiverThread.start();
            ProtocolLogger.logEvent("SYS", "Receiver thread started on port " + Config.RECEIVER_PORT, "info");
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("RDT Web Server started on http://localhost:8080");
    }

    static class StaticHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/")) {
                path = "/index.html";
            }
            
            File file = new File("src" + path);
            if (!file.exists()) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }
            
            byte[] bytes = Files.readAllBytes(file.toPath());
            exchange.getResponseHeaders().set("Content-Type", "text/html");
            exchange.sendResponseHeaders(200, bytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(bytes);
            os.close();
        }
    }

    static class SSEHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
            exchange.sendResponseHeaders(200, 0);
            
            ProtocolLogger.addClient(exchange);
            ProtocolLogger.logEvent("SYS", "Client connected to SSE Event Bus", "info");
            
            while (true) {
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }

    static class DropHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                simulator.triggerManualDrop();
                String response = "Manual drop armed";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    static class SendHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (exchange.getRequestMethod().equalsIgnoreCase("POST")) {
                if (senderActive) {
                    String response = "Transfer already in progress";
                    exchange.sendResponseHeaders(409, response.length());
                    exchange.getResponseBody().write(response.getBytes());
                    exchange.close();
                    return;
                }

                String query = exchange.getRequestURI().getQuery();
                String message = "Test RDT transfer over web";
                if (query != null && query.startsWith("message=")) {
                    message = java.net.URLDecoder.decode(query.substring(8), "UTF-8");
                }

                final String finalMsg = message;
                senderActive = true;
                new Thread(() -> {
                    try {
                        Sender sender = new Sender(finalMsg, simulator);
                        sender.start();
                    } catch (Exception e) {
                        ProtocolLogger.logEvent("SYS", "Failed to start sender: " + e.getMessage(), "error");
                    } finally {
                        senderActive = false;
                    }
                }).start();

                String response = "Transfer started";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }
}
