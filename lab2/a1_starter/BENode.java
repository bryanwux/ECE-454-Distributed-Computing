import java.net.InetAddress;
import java.net.Socket;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;


import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;



public class BENode {
    static Logger log;

    public static void main(String [] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: java BENode FE_host FE_port BE_port");
			System.exit(-1);
		}

		// initialize log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(BENode.class.getName());

		String hostFE = args[0];
		int portFE = Integer.parseInt(args[1]);
		int portBE = Integer.parseInt(args[2]);
		log.info("Launching BE node on port " + portBE + " at host " + getHostName());

		// connect FE and BE
		BEFEConnector(getHostName(), portBE, hostFE, portFE);

		// launch Thrift server
		BcryptService.Processor processor = new BcryptService.Processor<BcryptService.Iface>(new BcryptServiceHandler());
		TNonblockingServerSocket socket = new TNonblockingServerSocket(portBE);
		THsHaServer.Args sargs = new THsHaServer.Args(socket);
		sargs.protocolFactory(new TBinaryProtocol.Factory());
		sargs.transportFactory(new TFramedTransport.Factory());
		sargs.processorFactory(new TProcessorFactory(processor));
		sargs.maxWorkerThreads(64);

		TServer server = new THsHaServer(sargs);
		server.serve();
    }

	private static void BEFEConnector(String BEHost, int BEPort, String FEHost, int FEPort) throws InterruptedException{
		// Establish connection between BE and FE

		TSocket sock = new TSocket(FEHost, FEPort);
		TTransport transport = new TFramedTransport(sock);
		TProtocol protocol = new TBinaryProtocol(transport);
		BcryptService.Client FENode = new BcryptService.Client(protocol);
		Boolean isConnected = false;
		while (!isConnected) {
			try {
				transport.open();
				isConnected = true;
				FENode.BENodeHandler(BEHost, BEPort);
				transport.close();
			} catch (Exception e) {
				float wait = 0.1f;
				Thread.sleep((long)wait * 1000);
				log.info("Couldn't connect to FE Node. Try again in " + wait + " secs");
			}
		}
		log.info("Successfully connected to FE node " + FEHost + "on port" + FEPort);
	}

    static String getHostName()
    {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			return "localhost";
		}
    }
}
