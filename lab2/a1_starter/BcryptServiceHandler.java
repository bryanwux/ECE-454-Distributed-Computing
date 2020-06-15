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
import org.apache.thrift.transport.TTransportException;

import java.util.Iterator;
import java.util.Map;

class TransportPair{
	private BcryptService.Client client;
	private TNonblockingTransport transport;

	public TransportPair(BcryptService.AsyncClient client, TNonblockingTransport transport){
		this.client = client;
		this.transport = transport;
	}

	public BcryptService.Client getClient(){
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
	private TransportPair ClientTransportPair;
	private boolean isBusy;

	BackendNode(String BEHost, int BEPort, TransportPair ClientTransportPair, boolean isBusy){
		this.BEHost = BEHost;
		this.BEPort = BEPort;
		//this.RequestNum = 0;
		this.ClientTransportPair=ClientTransportPair;
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
		return this.ClientTransportPair;
	}

	public synchronized boolean isBusy() {
		return this.isBusy;
	}

}

public class BcryptServiceHandler implements BcryptService.Iface {
    //private ExecutorService executor;
	public static List<BackendNode> backendNodes;

    public BcryptServiceHandler(){
    	//executor = Executors.newFixedThreadPool(32);
    	backendNodes=new CopyOnWriteArrayList<BackendNode>();
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
			return hashPasswordComp(password, logRounds);
		}else {
			BackendNode BE = getBE();
			TransportPair cp = BE.getTransportPair();
			List<String> hash = new ArrayList<String>();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TNonblockingTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE doing work");
					hash = async.hashPasswordComp(password, logRounds);
					transport.close();
				} catch (TTransportException e) {
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
			TransportPair cp = BE.getTransportPair();
			List<Boolean> check = new ArrayList<Boolean>();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TNonblockingTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE doing work");
					check = async.checkPasswordComp(password, hash);
					transport.close();
				} catch (TTransportException e) {
					System.out.println("Failed connect to target BE, drop it.");
					backendNodes.remove(BE);
				}
			}
			return check;
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

			BcryptService.Client client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
			TransportPair pair = new TransportPair(client, transport);

			BackendNode BE = new BackendNode(BEHost, BEPort, pair, false);// set backend node to busy
			backendNodes.add(BE);
		} catch (Exception e) {
		}
	}
}
