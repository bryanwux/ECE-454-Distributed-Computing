import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.util.Map;

import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.log4j.Logger;
import java.util.concurrent.*;

import org.apache.thrift.protocol.TProtocolFactory;
import org.mindrot.jbcrypt.BCrypt;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingTransport;


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

class BackendNode {
	private String BEHost;
	private int BEPort;
	private int RequestNum;//what is this??
	private ConcurrentHashMap<TransportPair, Boolean> ClientTransportPair;

	BackendNode(String BEHost, int BEPort, ConcurrentHashMap<TransportPair, Boolean> ClientTransportPair) {
		this.BEHost = BEHost;
		this.BEPort = BEPort;
		this.RequestNum = 0;
		this.ClientTransportPair = ClientTransportPair;
	}

	public String getBEHost() {
		return this.BEHost;
	}

	public int getBEPort() {
		return BEPort;
	}

	public synchronized int getRequestNum() {
		return this.RequestNum;
	}

	public synchronized void incrementRequest() {
		this.RequestNum++;
	}

	public synchronized void decrementRequest(TransportPair pair) {
		this.RequestNum--;
		// no one is using this client
		this.ClientTransportPair.put(pair, false);
	}

	public synchronized TransportPair getTransportPair() {
		Iterator ctIterator = ClientTransportPair.entrySet().iterator();
		while (ctIterator.hasNext()) {
			Map.Entry entry = (Map.Entry) ctIterator.next();
			if ((Boolean) entry.getValue() == false) {
				ClientTransportPair.put((TransportPair) entry.getKey(), true);
				return (TransportPair) entry.getKey();
			}
		}
		return null;
	}
}

public class BcryptServiceHandler implements BcryptService.Iface {
    private ExecutorService executor;
    private Logger log;
	public static ConcurrentLinkedQueue<BackendNode> backendNodes;


//    public BcryptServiceHandler(){
//    	log = Logger.getLogger(BcryptServiceHandler.class.getName());
//    	executor = Executors.newFixedThreadPool(32);
//	}

	public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException
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

	public void BENodeHandlerInit(String BEHost, int BEPort) throws IllegalArgument, org.apache.thrift.TException {
		try {
			TProtocolFactory protocolFactory;
			TAsyncClientManager clientManager;
			TNonblockingTransport transport;

			ConcurrentHashMap<TransportPair, Boolean> ClientTransportPair = new ConcurrentHashMap<TransportPair, Boolean>();
			for(int i=0; i<4; i++){
				protocolFactory = new TCompactProtocol.Factory();
				clientManager = new TAsyncClientManager();
				transport = new TNonblockingSocket(BEHost, BEPort);

				BcryptService.AsyncClient client = new BcryptService.AsyncClient(protocolFactory, clientManager, transport);
				TransportPair pair = new TransportPair(client, transport);
				ClientTransportPair.put(pair, false);  // set backend node to busy
			}

			BackendNode BENode = new BackendNode(BEHost, BEPort, ClientTransportPair);
			backendNodes.add(BENode);
		} catch (Exception e) {

		}
	}
}
