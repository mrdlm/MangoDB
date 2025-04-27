import server.CommandProcessor;
import server.MangoServer;

import java.io.IOException;

public class MangoApp {
    public static void main(String[] args) throws IOException {
        final MangoServer mangoServer = new MangoServer();
        mangoServer.start();
    }
}
