import java.util.ArrayList;
import java.util.List;

import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.log4j.Logger;
import java.util.concurrent.*;

import org.apache.thrift.protocol.TProtocolFactory;
import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {
    private ExecutorService executor;
    private Logger log;
	public static ConcurrentLinkedQueue<BackendNode> backendNodes;


    public BcryptServiceHandler(){
    	log = Logger.getLogger(BcryptServiceHandler.class.getName());
    	executor = Executors.newFixedThreadPool(32);




	}

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
}
