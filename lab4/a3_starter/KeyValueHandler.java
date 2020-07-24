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

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    public final int LOCK_NUM = 64;
    public final int CLIENT_NUM = 32;
    private volatile Boolean isPrimary = false;

    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    private static Logger log;
    
    private volatile InetSocketAddress primaryAddress;
    private volatile InetSocketAddress backupAddress;
    private ReentrantLock reLock = new ReentrantLock();
    private Striped<Lock> stripedLock = Striped.lock(LOCK_NUM);
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> backupPool = null;

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception{
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        myMap = new ConcurrentHashMap<String, String>();
        primaryAddress = null;
        backupAddress = null;

        log = Logger.getLogger(KeyValueHandler.class.getName());
        determineNodes(host, port, curClient, zkNode);
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
    
    // copy data to backup
    public void sync(Map<String, String> data) throws org.apache.thrift.TException {
        this.myMap = new ConcurrentHashMap<String, String>(data); 
    }

    public void determineNodes(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        curClient.sync();
        List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

        if (children.size() == 1) {
            this.isPrimary = true;
        } else {
            Collections.sort(children);
            byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
            String strData = new String(data);
            String[] backup = strData.split(":");
            String backupHost = backup[0];
            int backupPort = Integer.parseInt(backup[1]);

            if (backupHost.equals(host) && backupPort == port) {
                this.isPrimary = false;
            } else {
                this.isPrimary = true;
            }
        }

    }

    public void setPrimary(boolean isPrimary) throws org.apache.thrift.TException {
        this.isPrimary = isPrimary;
    }

    // basically read operation, do not need locks
    public String get(String key) throws org.apache.thrift.TException {
        if (isPrimary == false) {
            // System.out.println("Backup is not allowed to get.");
            throw new org.apache.thrift.TException("can not read in backup");
        }

        try {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    // basically write operation, need locks    
    public void put(String key, String value) throws org.apache.thrift.TException {
        if (isPrimary == false) {
            throw new org.apache.thrift.TException("can not write in backup");
        }else{
            // Returns the stripe that corresponds to the passed key
            Lock lock = stripedLock.get(key);
            lock.lock();

            // If the relock is set, which means copying data is in process, prevent write operation
            while (reLock.isLocked())
                doNothing();

            try {
                // save key-value pairs to primary
                myMap.put(key, value);

                if (this.backupPool != null) {
                    KeyValueService.Client backupClient = null;
                    // retrieves the head of the backupPool
                    backupClient = backupPool.poll();
                    backupClient.backupPut(key, value);
                    this.backupPool.offer(backupClient);
                }
            } catch (Exception e) {
                e.printStackTrace();
                this.backupPool = null;
            } finally {
                // release the lock
                lock.unlock();
            }
            }
    }

    public void doNothing(){
        System.out.println("A put operation is in process");
    }

    public synchronized Boolean isPrimary(){
        if (null == primaryAddress) return false;
        return (host.equals(primaryAddress.getHostName()) && port == primaryAddress.getPort());
    }

	synchronized public void decideNodes(WatchedEvent event) throws org.apache.thrift.TException {
        try {
            curClient.sync();
            List<String> children = curClient.getChildren().usingWatcher(this).forPath(zkNode);

            if (children.size() == 1) {
                // only one children, must be primary
                this.isPrimary = true;                
                primaryAddress = new InetSocketAddress(host, port);
                backupAddress = null;
                this.backupPool = null;
                this.isPrimary = true;
                return;
            }else{
                // Find primary data and backup data
                Collections.sort(children);
                byte[] data = curClient.getData().forPath(zkNode + "/" + children.get(children.size() - 1));
                String strData = new String(data);
                String[] backup = strData.split(":");
                String backupHost = backup[0];
                int backupPort = Integer.parseInt(backup[1]);

                
                if (backupHost.equals(host) && backupPort == port) {
                    this.isPrimary = false;
                } else {
                    this.isPrimary = true;
                }
                
                if (this.isPrimary && this.backupPool == null) {
                  
                    // try {
                    //     TSocket sock = new TSocket(backupHost, backupPort);
                    //     TTransport transport = new TFramedTransport(sock);
                    //     transport.open();
                    //     TProtocol protocol = new TBinaryProtocol(transport);
                    //     KeyValueService.Client backupClient = new KeyValueService.Client(protocol);

                    //     backupClient.sync(this.myMap);
                    // } catch(Exception e) {
                    //     System.out.println("Failed to copy to replica");
                    //     System.out.println(e.getLocalizedMessage());
                    // }
                    KeyValueService.Client firstBackupClient = null;

                    while(firstBackupClient == null) {
                        try {
                            TSocket sock = new TSocket(backupHost, backupPort);
                            TTransport transport = new TFramedTransport(sock);
                            transport.open();
                            TProtocol protocol = new TBinaryProtocol(transport);
                            firstBackupClient = new KeyValueService.Client(protocol);
                        } catch (Exception e) {
                            System.out.println("Failed to copy to replica");
                        }
                    }
                    
                    reLock.lock();
                    firstBackupClient.sync(this.myMap);
                
                    // Create 32 backup clients
                    this.backupPool = new ConcurrentLinkedQueue<KeyValueService.Client>();
        
                    for(int i = 0; i < CLIENT_NUM; i++) {
                        TSocket sock = new TSocket(backupHost, backupPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                
                        this.backupPool.add(new KeyValueService.Client(protocol));
                    }
                    reLock.unlock();
                } else {
                    this.backupPool = null;
                }
                }
                // backupPool = null;
                // //backupAddress = new InetSocketAddress(backup[0], Integer.parseInt(backup[1]));
                // // get primary data
                // byte[] data1 = curClient.getData().forPath(zkNode + "/" + children.get(0));
                // String strData1 = new String(data1);
                // String[] primary = strData1.split(":");
                // primaryAddress = new InetSocketAddress(primary[0], Integer.parseInt(primary[1]));
                // System.out.println("Found primary " + strData1);

        
        } catch (Exception e) {
            log.error("Unable to determine primary or children");
            this.backupPool = null;
        }
    }

    synchronized public void process(WatchedEvent event) throws org.apache.thrift.TException {
        System.out.println("ZooKeeper event: " + event);
        try {
            decideNodes(event);
        } catch (Exception e) {
            log.error("Unable to determine primary or children");
            this.backupPool = null;
        }
    }

}