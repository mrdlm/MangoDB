import server.MangoServer;

import java.io.IOException;

public class MangoApp {
    public static void main(String[] args) throws IOException {

        final String banner = """
                ███╗   ███╗ █████╗ ███╗   ██╗ ██████╗  ██████╗ ██████╗ ██████╗\s
                ████╗ ████║██╔══██╗████╗  ██║██╔════╝ ██╔═══██╗██╔══██╗██╔══██╗
                ██╔████╔██║███████║██╔██╗ ██║██║  ███╗██║   ██║██║  ██║██████╔╝
                ██║╚██╔╝██║██╔══██║██║╚██╗██║██║   ██║██║   ██║██║  ██║██╔══██╗
                ██║ ╚═╝ ██║██║  ██║██║ ╚████║╚██████╔╝╚██████╔╝██████╔╝██████╔╝
                ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝  ╚═════╝ ╚═════╝ ╚═════╝\s 
                """ ;
        System.out.println(banner);

        if (args.length == 2) {
            int port = Integer.parseInt(args[0]);
            String role = args[1];

            final MangoServer mangoServer = new MangoServer(role, port);
            mangoServer.start();
        } else {
            final MangoServer mangoServer = new MangoServer();
            mangoServer.start();
        }
    }
}
