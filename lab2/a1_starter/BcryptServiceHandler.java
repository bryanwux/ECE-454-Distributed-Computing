import java.util.ArrayList;
import java.util.List;

import org.apache.http.impl.cookie.BasicClientCookie;
import java.util.concurrent.*;

import org.apache.thrift.protocol.TProtocolFactory;
import org.mindrot.jbcrypt.BCrypt;

//import org.apache.thrift.async.TAsyncClientManager;
//import org.apache.thrift.protocol.TCompactProtocol;
//import org.apache.thrift.protocol.TProtocolFactory;
//import org.apache.thrift.transport.TNonblockingSocket;
//import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportException;

import java.util.Iterator;
import java.util.Map;

class TransportPair{
	private BcryptService.Client client;
	private TTransport transport;

	public TransportPair(BcryptService.Client client, TTransport transport){
		this.client = client;
		this.transport = transport;
	}

	public BcryptService.Client getClient(){
		return this.client;
	}

	public TTransport getTransport(){
		return this.transport;
	}
}

class BackendNode{
	private String BEHost;
	private int BEPort;
	private int RequestNum;
	private TransportPair ClientTransportPair;

	BackendNode(String BEHost, int BEPort, TransportPair ClientTransportPair){
		this.BEHost = BEHost;
		this.BEPort = BEPort;
		this.ClientTransportPair=ClientTransportPair;
	}

	public String getBEHost(){
		return this.BEHost;
	}

	public int getBEPort() {
		return BEPort;
	}

	public synchronized TransportPair getTransportPair(){
		return this.ClientTransportPair;
	}

}

public class BcryptServiceHandler implements BcryptService.Iface {
    //private ExecutorService executor;
	public static List<BackendNode> idleNodes;
    public BcryptServiceHandler(){
    	//executor = Executors.newFixedThreadPool(32);
    	idleNodes=new CopyOnWriteArrayList<BackendNode>();
	}

	public synchronized BackendNode getBE(){
    	if(!idleNodes.isEmpty()){
    		BackendNode BE = idleNodes.get(0);
    		idleNodes.remove(BE);
    		return BE;
		}
    	return null;
	}

	public synchronized void putBE(BackendNode BE){
		if(BE != null){
			idleNodes.add(BE);
		}
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		if (logRounds < 4 || logRounds > 16) {
			throw new IllegalArgument("Bad logRounds!");
		}
		if (password.isEmpty()) {
			throw new IllegalArgument("Empty passwords!");
		}

		if(idleNodes.isEmpty()){
			System.out.println("FE doing work");
			return hashPasswordComp(password, logRounds);
		}
		boolean offload=false;
		List<String> hash = new ArrayList<String>();
		while(!offload) {
			BackendNode BE = getBE();

			//if all resources are locked, and the thread gets none, wait
			if(BE==null){
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE "+BE.toString()+"doing work");
					hash = async.hashPasswordComp(password, logRounds);
					transport.close();
					putBE(BE);
					offload=true;
				} catch (TTransportException e) {
					System.out.println("Failed connect to target BE, drop it.");
					System.out.println("FE doing work");
					return hashPasswordComp(password, logRounds);
				}
			}
		}
		return hash;
	}

	public List<String> hashPasswordComp(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		try {
			List<String> ret = new ArrayList<>();
			for(String onePwd: password){
				String oneHash = BCrypt.hashpw(onePwd, BCrypt.gensalt(logRounds));
				ret.add(oneHash);
			}

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
	}

	public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
	{
		if (password.isEmpty()) {
			throw new IllegalArgument("Empty list of passwords");
		}
		if (hash.isEmpty()) {
			throw new IllegalArgument("Empty list of hashes");
		}

		if(idleNodes.isEmpty()){
			System.out.println("FE doing work");
			return checkPasswordComp(password, hash);
		}
		boolean offload=false;
		List<Boolean> check = new ArrayList<Boolean>();
		while(!offload) {
			BackendNode BE = getBE();
			//if all resources are locked, and the thread gets none, wait
			if(BE==null){
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE "+BE.toString()+"doing work");
					check = async.checkPasswordComp(password, hash);
					transport.close();
					putBE(BE);
					offload=true;
				} catch (TTransportException e) {
					System.out.println("Failed connect to target BE, drop it.");
					System.out.println("FE doing work");
					return checkPassword(password, hash);
				}
			}
		}
		return check;
	}

    public List<Boolean> checkPasswordComp(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException
    {
		try {
			List<Boolean> ret = new ArrayList<>();
			for(int i = 0; i < password.size(); i++){
				String onePwd = password.get(i);
				String oneHash = hash.get(i);
				ret.add(BCrypt.checkpw(onePwd, oneHash));
			}

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
    }

	public void BENodeHandler(String BEHost, int BEPort) throws IllegalArgument, org.apache.thrift.TException {
		try {
//			TProtocolFactory protocolFactory;
//			TAsyncClientManager clientManager;
//			TNonblockingTransport transport;
//
//			protocolFactory = new TCompactProtocol.Factory();
//			clientManager = new TAsyncClientManager();
//			transport = new TNonblockingSocket(BEHost, BEPort);

			TSocket sock = new TSocket(BEHost, BEPort);
			TTransport transport = new TFramedTransport(sock);
			TProtocol protocol = new TBinaryProtocol(transport);
			BcryptService.Client client = new BcryptService.Client(protocol);

			TransportPair pair = new TransportPair(client, transport);

			BackendNode BE = new BackendNode(BEHost, BEPort, pair);// set backend node to idle
			idleNodes.add(BE);
			System.out.println(idleNodes.size() + " BE nodes in list");
		} catch (Exception e) {
		}
	}
}
