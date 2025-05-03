package cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.util.concurrent.Callable;

@Command(
        name = "mango",
        mixinStandardHelpOptions = true,
        version = "mango 1.0",
        description = "Command line interface for MangoDB",
        subcommands = {
                MangoCLI.StartCommand.class
        }
)
public class MangoCLI implements Callable<Integer> {

    @Command(
            name="start",
            description = "Start MangoDB resources"
    )
    static class StartCommand implements Callable<Integer> {
        // a subclass which handles specific commands
        @Command(
                name="replicaset",
                description = "Start a replicaset"
        )
        int startReplicaSet(
                @Parameters(index= "0", description = "The unique name for this replicaset deployment")
                String replicaSetName,
                @Option(names = {"-f", "--file"}, required = true, description = "The configuration file for this replicaset")
                File configFile
        ) {
            System.out.printf(">>> Starting replica set '%s' using config '%s'...\n",
                    replicaSetName, configFile.getAbsolutePath());

            if (!configFile.exists()) {
                System.err.printf("Config file not found");
            }

            return 0;
        }

        @Override
        public Integer call() throws Exception {
            System.out.println("Please provide which resource to start");
            return 0;
        }
    }

    public static void main(String[] args) {
        final String RESET = "\u001B[0m";
        final String PASTEL_YELLOW = "\u001B[38;2;255;223;0m";

        final String banner = """
                ███╗   ███╗ █████╗ ███╗   ██╗ ██████╗  ██████╗ ██████╗ ██████╗\s
                ████╗ ████║██╔══██╗████╗  ██║██╔════╝ ██╔═══██╗██╔══██╗██╔══██╗
                ██╔████╔██║███████║██╔██╗ ██║██║  ███╗██║   ██║██║  ██║██████╔╝
                ██║╚██╔╝██║██╔══██║██║╚██╗██║██║   ██║██║   ██║██║  ██║██╔══██╗
                ██║ ╚═╝ ██║██║  ██║██║ ╚████║╚██████╔╝╚██████╔╝██████╔╝██████╔╝
                ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝  ╚═════╝ ╚═════╝ ╚═════╝\s 
                """ ;

        System.out.println(PASTEL_YELLOW + banner + RESET);

        int exitCode = new CommandLine(new MangoCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        return 0;
    }
}
