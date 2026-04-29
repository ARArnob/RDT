import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;

public class RdtWebServer {
    private static NetworkSimulator simulator;
    private static Receiver receiver;
    private static volatile boolean senderActive = false;

    public static void main(String[] args) {
        simulator = new NetworkSimulator();

        Javalin app = Javalin.create(config -> {
            config.staticFiles.add("/public", Location.CLASSPATH);
        }).start(8080);

        app.ws("/api/ws", ws -> {
            ws.onConnect(ctx -> {
                ProtocolLogger.addClient(ctx);
                ProtocolLogger.log("SYS", "Client connected to Event Bus", "info");
            });
            ws.onClose(ctx -> {
                ProtocolLogger.removeClient(ctx);
            });
        });

        app.post("/api/simulator/drop", ctx -> {
            simulator.triggerManualDrop();
            ctx.status(200).result("Manual drop armed.");
        });

        app.post("/api/simulator/send", ctx -> {
            if (senderActive) {
                ctx.status(409).result("A transfer is already in progress.");
                return;
            }

            String message = ctx.queryParam("message");
            if (message == null || message.trim().isEmpty()) {
                message = "Hello this is a reliable data transfer protocol using Go-Back-N over UDP";
            }
            
            final String finalMsg = message;
            senderActive = true;
            new Thread(() -> {
                try {
                    Sender sender = new Sender(finalMsg, simulator);
                    sender.start();
                } catch (Exception e) {
                    ProtocolLogger.log("SYS", "Failed to start sender: " + e.getMessage(), "error");
                } finally {
                    senderActive = false;
                }
            }).start();
            
            ctx.status(200).result("Transfer started.");
        });

        try {
            receiver = new Receiver(simulator);
            Thread receiverThread = new Thread(receiver::start);
            receiverThread.setDaemon(true);
            receiverThread.start();
            ProtocolLogger.log("SYS", "Receiver thread started on port " + Config.RECEIVER_PORT, "info");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
