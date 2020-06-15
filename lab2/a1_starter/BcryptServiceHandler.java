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
    		return idleNodes.get(0);
		}
    	return null;
	}

	public synchronized void putBE(BackendNode BE){
		if(BE != null){
			idleNodes.add(BE);
			System.out.println("Put BE: "+BE.toString());
		}
	}

	public synchronized void delBE(BackendNode BE){
		if(BE != null && !idleNodes.isEmpty()){
			idleNodes.remove(BE);
			System.out.println("Del BE: "+BE.toString());
		}
	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
	{
		if(idleNodes.isEmpty()){
			return hashPasswordComp(password, logRounds);
		}
		boolean offload=false;
		List<String> hash = new ArrayList<String>();
		while(!offload) {
			BackendNode BE = getBE();

			System.out.println(idleNodes.toString());
			System.out.println("BE before delete is: "+BE.toString());
			delBE(BE);
			System.out.println("BE after delete is: "+BE.toString());
			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE doing work");
					hash = async.hashPasswordComp(password, logRounds);
					transport.close();
					putBE(BE);
					offload=true;
				} catch (TTransportException e) {
					System.out.println("Failed connect to target BE, drop it.");
				}
			}
		}
		return hash;
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
		if(idleNodes.isEmpty()){
			return checkPasswordComp(password, hash);
		}
		boolean offload=false;
		List<Boolean> check = new ArrayList<Boolean>();
		while(!offload) {
			BackendNode BE = getBE();
			System.out.println(idleNodes.toString());

			delBE(BE);
			TransportPair cp = BE.getTransportPair();
			if (cp != null) {
				BcryptService.Client async = cp.getClient();
				TTransport transport = cp.getTransport();
				try {
					transport.open();
					System.out.println("BE doing work");
					check = async.checkPasswordComp(password, hash);
					transport.close();
					putBE(BE);
					offload=true;
				} catch (TTransportException e) {
					System.out.println("Failed connect to target BE, drop it.");
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
		} catch (Exception e) {
		}
	}
}
