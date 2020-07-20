import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.util.concurrent.Striped;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.CuratorWatcher;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    public final int LOCK_NUM = 64;

    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private volatile InetSocketAddress primaryAddress;
    private volatile InetSocketAddress backupAddress;
    private ReentrantLock reLock = new ReentrantLock();
    private Striped<Lock> stripedLock = Striped.lock(LOCK_NUM);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupPool = null;

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
    if(children.size() > 1){
        //sort the return list of children in ascending lexicographic order
        Collections.sort(children);
        byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
        String strBackup = new String(data);
        String[] backup = strBackup.split(":");
        String backupHost = backup[0];
        int backupPort = Integer.parseInt(backup[1]);
    }

    
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
            // Returns the stripe that corresponds to the passed key
            Lock lock = stripedLock.get(key);
            lock.lock();

            if (!reLock.isLocked()){
                try{
                    myMap.put(key, value);
                    if(!this.backupPool.isEmpty()){
                        // retrieves the head of the backupPol
                        KeyValueService.Client backupClient = backupPool.poll();
                        backupClient.putBackup(key,value);
                        this.backupPool.offer(backupClient);
                    }
                }catch(Exception e){
                    e.printStackTrace();
                    this.backupPool = null;
                }finally{
                    lock.unlock();
                }
            }

        }
    }

    public synchronized Boolean isPrimary(){
        if (null == primaryAddress) return false;
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }
}
