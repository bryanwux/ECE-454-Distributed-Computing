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
	//Global variable
	final int MAXBATCHSIZE=4;

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
	
	public static void hashPasswordFE(List<String> password, short logRounds, hashCallback callback){
		BcryptServiceHandler handler = new BcryptServiceHandler();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					List<String> resultHashes = handler.hashPasswordComp(password, logRounds);
					callback.onComplete(resultHashes);
				} catch (Exception e) {
					callback.onError(e);
				}
			}
		}).start();
	}

	public static void checkPasswordFE(List<String> password, List<String> hash, checkCallback callback){
		BcryptServiceHandler handler = new BcryptServiceHandler();
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					List<Boolean> resultHashes = handler.checkPasswordComp(password, hash);
					callback.onComplete(resultHashes);
				} catch (Exception e) {
					callback.onError(e);
				}
			}
		}).start();
	}


	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		errorCheckingHashPassword(password, logRounds);

		//if(password.size()<=MAXBATCHSIZE) {

			if (idleNodes.isEmpty()) {
				System.out.println("FE doing work");
				hashCallback callback = new hashCallback();
				hashPasswordFE(password, logRounds, callback);
				//callback.latch.await();
				return callback.hash;
			}


			boolean offload = false;

			while (!offload) {

				BackendNode BE = getBE();

				//if all resources are locked, and the thread gets none, wait
				if (BE == null) {
					if (idleNodes.isEmpty()) {
						hashCallback callback = new hashCallback();
						hashPasswordFE(password, logRounds,callback);
						return callback.hash;
					}
					continue;
				}

				TransportPair cp = BE.getTransportPair();
				if (cp != null) {
					try {
						BcryptService.AsyncClient async = cp.getClient();
						System.out.println("BE " + BE.toString() + " doing work");
						hashCallback callback = new hashCallback();
						async.hashPasswordComp(password, logRounds, callback);
						callback.latch.await();
						if (callback.hash != null) {
							putBE(BE);
						} else {
							System.out.println("Failed connect to target BE, drop it.");
							continue;
						}
						offload = true;
					} catch (Exception e) {
						System.out.println("Something wrong happened");
						continue;
					}
				}
			}
			return callback.hash;
//		}else{
//			List<hashCallback> callbacks = new ArrayList<>();
//			System.out.println("Batch too big, size: " + password.size() + ", splitting...");
//			int subBatchNum=password.size()/MAXBATCHSIZE;
//
//			//assign sub task to BE
//			for(int i=0; i<subBatchNum; i++){
//				List<String> subPassword = password.subList(i*MAXBATCHSIZE, i*MAXBATCHSIZE+MAXBATCHSIZE);
//				hashPasswordSub(subPassword,logRounds,callbacks);
//			}
//			List<String> last = password.subList((subBatchNum-1)*MAXBATCHSIZE, password.size());
//			hashPasswordSub(last,logRounds,callbacks);
//
//			//Process results
//			List<String> result = new ArrayList<>();
//			for(hashCallback c:callbacks){
//				try{
//					c.latch.await();
//				} catch(InterruptedException e){
//					System.out.println("Await fail");
//				}
//				if(c.hash != null){
//					result.addAll(c.hash);
//					putBE(c.BE);
//				}else{
//					putBE(c.BE);
//					return hashPasswordComp(password,logRounds);
//				}
//			}
//			return result;
//		}
	}

	public void hashPasswordSub(List<String> password, short logRounds, List<hashCallback> callbacks) throws IllegalArgument, org.apache.thrift.TException
	{
		hashCallback callback = new hashCallback();

		if (idleNodes.isEmpty()) {
			System.out.println("FE doing work");
			hashPasswordFE(password, logRounds, callback);
			callbacks.add(callback);
			return;
		}

		boolean offload = false;
		while (!offload) {

			BackendNode BE = getBE();
			callback.BE=BE;
			//if all resources are locked, and the thread gets none, wait
			if (BE == null) {
				if (idleNodes.isEmpty()) {
					hashPasswordFE(password, logRounds, callback);
					callbacks.add(callback);
					return;
				}
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				try {
					BcryptService.AsyncClient async = cp.getClient();
					System.out.println("BE " + BE.toString() + " doing sub work");
					async.hashPasswordComp(password, logRounds, callback);
					//add callback to worker list
					callbacks.add(callback);
					offload = true;
				} catch (Exception e) {
					System.out.println("Something wrong happened");
					e.printStackTrace();
					continue;
				}
			}
		}
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
		checkCallback callback = new checkCallback();
		//if(password.size()<=MAXBATCHSIZE) {
			if (idleNodes.isEmpty()) {
				System.out.println("FE doing work");
				checkPasswordFE(password,hash,callback);
				return callback.res;
			}
			boolean offload = false;

			while (!offload) {
				BackendNode BE = getBE();
				//if all resources are locked, and the thread gets none, wait
				if (BE == null) {
					if (idleNodes.isEmpty()) {
						checkPasswordFE(password,hash,callback);
						return callback.res;
					}
					continue;
				}

				TransportPair cp = BE.getTransportPair();
				if (cp != null) {
					try {
						BcryptService.AsyncClient async = cp.getClient();
						System.out.println("BE " + BE.toString() + " doing work");
						async.checkPasswordComp(password, hash, callback);
						callback.latch.await();
						if (callback.res != null) {
							putBE(BE);
						} else {
							System.out.println("Failed connect to target BE, drop it.");
							continue;
						}
						offload = true;
					} catch (Exception e) {
						System.out.println("Something wrong happened");
						continue;
					}
				}
			}
			return callback.res;
//		}else{
//			List<checkCallback> callbacks = new ArrayList<>();
//			System.out.println("Batch too big, size: " + password.size() + ", splitting...");
//			int subBatchNum=password.size()/MAXBATCHSIZE;
//
//			//assign sub task to BE
//			for(int i=0; i<subBatchNum; i++){
//				List<String> subPassword = password.subList(i*MAXBATCHSIZE, i*MAXBATCHSIZE+MAXBATCHSIZE);
//				checkPasswordSub(subPassword,hash,callbacks);
//			}
//			List<String> last = password.subList((subBatchNum-1)*MAXBATCHSIZE, password.size());
//			checkPasswordSub(last,hash,callbacks);
//
//			List<Boolean> result = new ArrayList<>();
//			for(checkCallback c:callbacks){
//				try{
//					c.latch.await();
//				} catch(InterruptedException e){
//					System.out.println("Await fail");
//				}
//				if(c.res != null){
//					result.addAll(c.res);
//					putBE(c.BE);
//				}else{
//					putBE(c.BE);
//					return checkPasswordComp(password,hash);
//				}
//			}
//			return result;
//		}
	}

	public void checkPasswordSub (List<String> password, List<String> hash, List<checkCallback> callbacks) throws IllegalArgument, org.apache.thrift.TException {

    	checkCallback callback = new checkCallback();

    	if (idleNodes.isEmpty()) {
			System.out.println("FE doing work");
			checkPasswordFE(password, hash, callback);
			callbacks.add(callback);
			return;
		}

		boolean offload = false;
    	while (!offload) {

			BackendNode BE = getBE();
			callback.BE = BE;
			//if all resources are locked, and the thread gets none, wait
			if (BE == null) {
				if (idleNodes.isEmpty()) {
					checkPasswordFE(password, hash, callback);
					callbacks.add(callback);
					return;
				}
				continue;
			}

			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				try {
					BcryptService.AsyncClient async = cp.getClient();
					System.out.println("BE " + BE.toString() + " doing sub work");
					async.checkPasswordComp(password, hash, callback);
					//add callback to worker list
					callbacks.add(callback);
					offload = true;
				} catch (Exception e) {
					System.out.println("Something wrong happened");
					e.printStackTrace();
					continue;
				}
			}
		}
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
		public BackendNode BE;

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
		public BackendNode BE;

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
