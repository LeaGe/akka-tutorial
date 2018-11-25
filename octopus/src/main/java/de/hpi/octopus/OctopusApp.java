package de.hpi.octopus;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import de.hpi.octopus.OctopusMaster;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class OctopusApp {

	public static final String ACTOR_SYSTEM_NAME = "octopus";
	
	public static void main(String[] args) {

    	MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
        	.addCommand(OctopusMaster.MASTER_ROLE, masterCommand)
            .addCommand(OctopusSlave.SLAVE_ROLE, slaveCommand)
            .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            switch (jCommander.getParsedCommand()) {
                case OctopusMaster.MASTER_ROLE:
                    File csvData = new File(masterCommand.csv);
                    CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.DEFAULT.withDelimiter(';'));
                    HashMap<Integer, String> originalPasswordHashes = new HashMap<>();
                    HashMap<Integer, String> originalGeneSequences = new HashMap<>();
                    for (CSVRecord csvRecord : parser) {
                        if (csvRecord.get(0).equals("ID")){
                            continue;
                        }
                        originalPasswordHashes.put(Integer.parseInt(csvRecord.get(0)), csvRecord.get(2));
                        originalGeneSequences.put(Integer.parseInt(csvRecord.get(0)), csvRecord.get(3));
                        /*
                        for (String field : csvRecord) {
                            System.out.print("\"" + field + "\", ");
                        }
                        System.out.println();
                        */
                    }

                    OctopusMaster.start(ACTOR_SYSTEM_NAME, masterCommand.workers, masterCommand.host, masterCommand.port, originalPasswordHashes, originalGeneSequences);
                    break;
                case OctopusSlave.SLAVE_ROLE:
                    OctopusSlave.start(ACTOR_SYSTEM_NAME, slaveCommand.workers, slaveCommand.host, slaveCommand.port, slaveCommand.masterhost, slaveCommand.masterport);
                    break;
                default:
                    throw new AssertionError();
            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    abstract static class CommandBase {

    	public static final int DEFAULT_MASTER_PORT = 7877;
    	public static final int DEFAULT_SLAVE_PORT = 7879;
        public static final int DEFAULT_WORKERS = 4;
    	
    	@Parameter(names = {"-h", "--host"}, description = "this machine's host name or IP to bind against")
        String host = this.getDefaultHost();

        String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }
    	
        @Parameter(names = {"-p", "--port"}, description = "port to bind against", required = false)
        int port = this.getDefaultPort();

        abstract int getDefaultPort();

    	@Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally", required = false)
        int workers = DEFAULT_WORKERS;
    }

    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return DEFAULT_MASTER_PORT;
        }

        @Parameter(names = {"-i", "--input"}, description = "location of csv-file", required = true)
        String csv;
    }

    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return DEFAULT_SLAVE_PORT;
        }
        
        @Parameter(names = {"-mp", "--masterport"}, description = "port of the master", required = false)
        int masterport = DEFAULT_MASTER_PORT;

        @Parameter(names = {"-mh", "--masterhost"}, description = "host name or IP of the master", required = true)
        String masterhost;
    }
}
