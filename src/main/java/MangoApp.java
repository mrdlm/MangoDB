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
        
        final MangoServer mangoServer = new MangoServer();
        mangoServer.start();
    }
}
