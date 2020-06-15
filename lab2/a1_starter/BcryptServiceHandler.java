import java.util.ArrayList;
import java.util.List;

import org.apache.http.impl.cookie.BasicClientCookie;
import java.util.concurrent.*;

import org.apache.thrift.protocol.TProtocolFactory;
import org.mindrot.jbcrypt.BCrypt;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;

import java.util.Iterator;
import java.util.Map;

class TransportPair{
	private BcryptService.AsyncClient client;
	private TNonblockingTransport transport;

	public TransportPair(BcryptService.AsyncClient client, TNonblockingTransport transport){
		this.client = client;
		this.transport = transport;
	}

	public BcryptService.AsyncClient getClient(){
		return this.client;
	}

	public TNonblockingTransport getTransport(){
		return this.transport;
	}
}

class BackendNode{
	private String BEHost;
	private int BEPort;
	private int RequestNum;
	private TransportPair ClientTransport;
	private boolean isBusy;

	BackendNode(String BEHost, int BEPort, TransportPair ClientTransport, boolean isBusy){
		this.BEHost = BEHost;
		this.BEPort = BEPort;
		//this.RequestNum = 0;
		this.ClientTransport=ClientTransport;
		this.isBusy = isBusy;
	}

	public String getBEHost(){
		return this.BEHost;
	}

	public int getBEPort() {
		return BEPort;
	}

//	public synchronized int getRequestNum(){
//		return this.RequestNum;
//	}
//
//	public synchronized void incrementRequest(){
//		this.RequestNum++;
//	}

//	public synchronized void decrementRequest(TransportPair pair){
//		this.RequestNum--;
//		// no one is using this client
//		this.ClientTransportPair.put(pair, false);
//	}

	public synchronized TransportPair getTransportPair(){
		return this.ClientTransport;
	}

	public synchronized bool isBusy() {
		return this.isBusy;
	}

}

public class BcryptServiceHandler implements BcryptService.Iface {
    //private ExecutorService executor;
	public static List<BENode> backendNodes = new CopyOnWriteArrayList<>();

    public BcryptServiceHandler(){
    	//executor = Executors.newFixedThreadPool(32);
    	backendNodes=new ConcurrentLinkedQueue<BackendNode>();
	}

	public synchronized BackendNode getBE(){
    	for(BackendNode be : backendNodes) {
			if (!be.isBusy()){
				return be;
			}
		}
    	return null;
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		if(backendNodes.isEmpty()){
			return hashPasswordComp(password, hash);
		}else {
			BackendNode BE = getBE();
			ClientTransportPair cp = BE.getTransportPair();
			List<String> hash = new ArrayList<String>();
			if (cp != null) {
				BcryptService.AsyncClient async = cp.getClient();
				TNonblockingTransport transport = cp.getTransport();
				async.checkPasswordComp(password, hash);
				try {
					transport.open();
					System.out.println("BE doing work");
					hash = client.hashPasswordComp(password, logRounds);
					transport.close();
				} catch (TTransportException t) {
					System.out.println("Failed connect to target BE, drop it.");
					backendNodes.remove(BE);
				}
			}
			return hash;
		}
	}

	public List<String> hashPasswordComp(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		if (logRounds < 4 || logRounds > 16) {
			throw new IllegalArgument("Bad logRounds!");
		}
		if (password.isEmpty()) {
			throw new IllegalArgument("Empty passwords!");
		}

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
		if(backendNodes.isEmpty()){
			return checkPasswordComp(password, hash);
		}else {
			BackendNode BE = getBE();
			ClientTransportPair cp = BE.getTransportPair();
			List<String> hash = new ArrayList<String>();
			if (cp != null) {
				BcryptService.AsyncClient async = cp.getClient();
				TNonblockingTransport transport = cp.getTransport();
				async.checkPasswordComp(password, hash);
				try {
					transport.open();
					System.out.println("BE doing work");
					hash = client.checkPasswordComp(password, logRounds);
					transport.close();
				} catch (TTransportException t) {
					System.out.println("Failed connect to target BE, drop it.");
					backendNodes.remove(BE);
				}
			}
			return hash;
		}
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
			TProtocolFactory protocolFactory;
			TAsyncClientManager clientManager;
			TNonblockingTransport transport;

			protocolFactory = new TCompactProtocol.Factory();
			clientManager = new TAsyncClientManager();
			transport = new TNonblockingSocket(BEHost, BEPort);

			BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
			TransportPair pair = new TransportPair(client, transport);

			BackendNode BENode = new BackendNode(BEHost, BEPort, pair, false);// set backend node to busy
			backendNodes.add(BENode);
		} catch (Exception e) {
		}
	}
}
