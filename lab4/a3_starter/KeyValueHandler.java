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
import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher {
    public final int LOCK_NUM = 64;
    public final int CLIENT_NUM = 32;
    private static Logger log;

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
    private volatile Boolean isPrimary = false;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        primaryAddress = null;
        backupAddress = null;
        myMap = new ConcurrentHashMap<String, String>();
        log = Logger.getLogger(KeyValueHandler.class.getName());
        // Set up watcher
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            this.isPrimary = true;
        } else {
            // Find primary and backup 
            Collections.sort(children);
            byte[] Data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strData = new String(Data);
            String[] backup = strData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            // Check if this is primary
            if (backupHost.equals(host) && backupPort == port) {
                this.isPrimary = false;
            } else {
                this.isPrimary = true;
            }
        }
    }

    // basically read operation, do not need locks
    public String get(String key) throws org.apache.thrift.TException
    {	
        if (isPrimary == false){
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
        if (isPrimary == false){
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
                        backupClient.backupPut(key,value);
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

    public void backupPut(String key, String value) throws org.apache.thrift.TException {
        // Returns the stripe that corresponds to the passed key
        Lock lock = stripedLock.get(key);
        lock.lock();

        try {
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void sync(Map<String, String> primaryMap) throws org.apache.thrift.TException {
        myMap = new ConcurrentHashMap<String, String>(primaryMap);
    }

    public void setPrimary(boolean isPrimary) throws org.apache.thrift.TException {
        this.isPrimary = isPrimary;
    }

    public synchronized Boolean isPrimary(){
        if (null == primaryAddress) return false;
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }

    synchronized public void decideNodes() throws Exception {
        
        while(true) {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);
            Collections.sort(children);
            if (children.size() == 1) {
                // only one children, must be primary
                primaryAddress = new InetSocketAddress(host, port);
                backupAddress = null;
                backupPool = null;
                this.isPrimary = true;
                return;

            } else if (children.size() > 1) {
                // get backup data
                byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
                String strData = new String(strData);
                String[] backup = strData.split(":");
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);
                if (backupHost.equals(host) && backupPort == port) {
                    this.isPrimary = false;
                } else {
                    this.isPrimary = true;
                }
                if (isPrimary() && this.backupPool == null) {
                    try {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        KeyValueService.Client backupClient = new KeyValueService.Client(protocol);

                        backupClient.sync(this.myMap);
                        transport.close();
                    } catch(Exception e) {
                        System.out.println("Failed to copy to replica");
                        //System.out.println(e.getLocalizedMessage());
                    }
                    reLock.lock();
                    backupPool = new ConcurrentLinkedQueue<KeyValueService.Client>();
                    for(int i = 0; i < CLIENT_NUM; i++) {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                
                        backupPool.add(new KeyValueService.Client(protocol));
                    }
                    reLock.unlock();  
                }
                backupPool = null;
                backupAddress = new InetSocketAddress(backup[0], Integer.parseInt(backup[1]));
                // get primary data
                byte[] data1 = curClient.getData().forPath(zkNode + "/" + children.get(0));
                String strData1 = new String(data);
                String[] primary = strData.split(":");
                primaryAddress = new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
                System.out.println("Found primary " + strData);
            }
            break;
        }
    }
    synchronized public void process(WatchedEvent event) {
        System.out.println("ZooKeeper event: " + event);
        try {
            decideNodes();
        } catch (Exception e) {
            log.error("Unable to determine primary or children");
            this.backupPool = null;
        }
    }
}
