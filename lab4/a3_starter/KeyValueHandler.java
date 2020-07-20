import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;


public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private volatile InetSocketAddress primaryAddress;
    private volatile InetSocketAddress backupAddress;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
	this.host = host;
	this.port = port;
	this.curClient = curClient;
    this.zkNode = zkNode;
    primaryAddress = null;
    backupAddress = null;
    myMap = new ConcurrentHashMap<String, String>();
    
    
    curClient.sync();
    List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
    }

    // basically read operation, do not need locks
    public String get(String key) throws org.apache.thrift.TException
    {	
        if (!isPrimary()){
            throw new org.apache.thrift.TException("Backup znode can not get");
        }else{
            try{
                String ret = myMap.get(key);
                if (ret == null)
                    return "";
                else
                    return ret;
            }catch (Exception e){
                e.printStackTrace();
                return "";
            }
        } 
    }
    // basically write operation, need locks
    public void put(String key, String value) throws org.apache.thrift.TException
    {
        if (!isPrimary()){
            throw new org.apache.thrift.TException("Backup znode can not get");
        }else{

        }
    }

    public synchronized Boolean isPrimary(){
        if (null == primaryAddress) return false;
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }
}
