import java.util.ArrayList;
import java.util.List;

import org.apache.http.impl.cookie.BasicClientCookie;
import java.util.concurrent.*;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.*;
import org.mindrot.jbcrypt.BCrypt;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

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
	static Logger log;
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

	public static void errorCheckingHashPassword(List<String> password, short logRounds)throws IllegalArgument, org.apache.thrift.TException
	{
		if (logRounds < 4 || logRounds > 30) {
			throw new IllegalArgument("Bad logRounds!");
		}
		if (password.isEmpty()) {
			throw new IllegalArgument("Empty passwords!");
		}

	}

	public static void errorCheckingCheckPassword(List<String> password, List<String> hashes)throws IllegalArgument, org.apache.thrift.TException
	{
		if (password.size() != hashes.size()) {
			throw new IllegalArgument("Password list and hash list must have the same size!");
		}
		if (password.isEmpty() || hashes.isEmpty()) {
			throw new IllegalArgument("Empty passwords!");
		}

	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		errorCheckingHashPassword(password, logRounds);

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
				if(idleNodes.isEmpty()){
					return hashPasswordComp(password, logRounds);
				}
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.AsyncClient async = cp.getClient();
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
					continue;
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
		errorCheckingCheckPassword(password, hash);

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
				if(idleNodes.isEmpty()){
					return checkPassword(password, hash);
				}
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.AsyncClient async = cp.getClient();
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
					continue;
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
				try{
					ret.add(BCrypt.checkpw(onePwd, oneHash));
				}catch(Exception e){
					ret.add(false);
				}
			}

			return ret;
		} catch (Exception e) {
			throw new IllegalArgument(e.getMessage());
		}
    }

	private class hashCallback implements AsyncMethodCallback<List<String>>{
		public CountDownLatch latch;
		public List<String> hash;

		public hashCallback(){
			latch = new CountDownLatch(1);
		}
		public void onComplete(List<String> response){
			hash = response;
			latch.countDown();
		}

		public void onError(Exception e){
			e.printStackTrace();
			latch.countDown();
		}
	}

	private class checkCallback implements AsyncMethodCallback<List<Boolean>>{
		public CountDownLatch latch;
		public List<Boolean> res;

		public checkCallback(){
			latch = new CountDownLatch(1);
		}
		public void onComplete(List<Boolean> response){
			res = response;
			latch.countDown();
		}

		public void onError(Exception e){
			e.printStackTrace();
			latch.countDown();
		}
	}

	public void BENodeHandler(String BEHost, int BEPort) throws IllegalArgument, org.apache.thrift.TException {

		TProtocolFactory protocolFactory;
		TAsyncClientManager clientManager;
		TNonblockingTransport transport;
    	try {
			protocolFactory = new TBinaryProtocol.Factory();
			clientManager = new TAsyncClientManager();
			transport = new TNonblockingSocket(BEHost, BEPort);

			BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);

			TransportPair pair = new TransportPair(client, transport);

			BackendNode BE = new BackendNode(BEHost, BEPort, pair);// set backend node to idle
			idleNodes.add(BE);
			System.out.println(idleNodes.size() + " BE nodes in list");
		} catch (Exception e) {
			log.error("Problem in connecting to FE node " + e);
			throw new IllegalArgument(e.getMessage());
		}
	}
}
