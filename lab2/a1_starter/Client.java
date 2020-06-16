//import java.util.List;
//import java.util.ArrayList;
//
//import org.apache.thrift.TException;
//import org.apache.thrift.protocol.TProtocol;
//import org.apache.thrift.protocol.TBinaryProtocol;
//import org.apache.thrift.transport.TTransport;
//import org.apache.thrift.transport.TSocket;
//import org.apache.thrift.transport.TFramedTransport;
//import org.apache.thrift.transport.TTransportFactory;
//
//public class Client {
//    public static void main(String [] args) {
//		if (args.length != 3) {
//			System.err.println("Usage: java Client FE_host FE_port password");
//			System.exit(-1);
//		}
//
//		try {
//			TSocket sock = new TSocket(args[0], Integer.parseInt(args[1])); //from args[0] to args[1] FE port
//			TTransport transport = new TFramedTransport(sock);
//			TProtocol protocol = new TBinaryProtocol(transport);
//			BcryptService.Client client = new BcryptService.Client(protocol);
//			transport.open();
//
//			List<String> password = new ArrayList<>();
//			//password.add(args[2]);
//			for(int i=0; i<20; i++){
//				password.add("cen424  "+i);
//			}
//			List<String> hash = client.hashPassword(password, (short)10);
////			System.out.println("Password: " + password.get(0));
////			System.out.println("Hash: " + hash.get(0));
////			System.out.println("Positive check: " + client.checkPassword(password, hash));
////			hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
////			System.out.println("Negative check: " + client.checkPassword(password, hash));
//			List<Boolean> check = client.checkPassword(password, hash);
//			System.out.println(check.toString());
//
//			transport.close();
//		} catch (TException x) {
//			x.printStackTrace();
//		}
//    }
//}
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.util.concurrent.*;

public class Client {
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: java Client FE_host FE_port password");
			System.exit(-1);
		}

		try {
			int threadNums = 16;
			ExecutorService executor = Executors.newFixedThreadPool(16);
			List<TTransport> transports = new ArrayList<>();
			List<Callable<List<String>>> workers = new LinkedList<>();
			long x = System.currentTimeMillis();
			for (int i = 0; i < threadNums; i++) {
				final int j = i;
				TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
				TTransport transport = new TFramedTransport(sock);
				transports.add(transport);
				TProtocol protocol = new TBinaryProtocol(transport);
				BcryptService.Client client = new BcryptService.Client(protocol);
				transport.open();


				// create a new thread for each split
				Callable<List<String>> workerCallable = new Callable<List<String>>() {
					@Override
					public List<String> call() throws Exception {
						List<String> password = new ArrayList<>();
						password.add(j + " password");
						password.add(j + " second password");
						password.add(j + " third password");
						password.add(j + " fourth password");
						List<String> hash = client.hashPassword(password, (short) 12);
						System.out.println(hash.get(0));
						return hash;
					}
				};
				System.out.println("Added callable: " + i);
				workers.add(workerCallable);
			}
			List<Future<List<String>>> futures = executor.invokeAll(workers);

			List<String> output = new LinkedList<>();
			for (Future<List<String>> task : futures) {
				System.out.println("Waiting on: " + task.toString());
				output.addAll(task.get());
			}

			System.out.println("DONE in: " + (System.currentTimeMillis() - x) / 1000.00);

//            System.out.println(hash.toString());
//            System.out.println("Password: " + password.get(0));
//            System.out.println("Hash: " + hash.get(0));
//            System.out.println("Positive check: " + client.checkPassword(password, hash));
//            hash.set(0, "$2a$14$reBHJvwbb0UWqJHLyPTVF.6Ld5sFRirZx/bXMeMmeurJledKYdZmG");
//            System.out.println("Negative check: " + client.checkPassword(password, hash));
//            try {
//                hash.set(0, "too short");
//                List<Boolean> rets = client.checkPassword(password, hash);
//                System.out.println("Exception check: no exception thrown");
//            } catch (Exception e) {
//                System.out.println("Exception check: exception thrown");
//            }

			for (TTransport transport : transports) {
				System.out.println("Closing transport");
				transport.close();
			}
			System.out.println("DONE THE CLIENT CODE");
		} catch (TException x) {
			x.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(0);
	}
}