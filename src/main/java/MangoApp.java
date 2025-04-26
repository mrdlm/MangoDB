import server.CommandProcessor;
import server.MangoServer;

public class MangoApp {
    public static void main(String[] args) {
        final MangoServer mangoServer = new MangoServer();
        mangoServer.start();
    }
}
