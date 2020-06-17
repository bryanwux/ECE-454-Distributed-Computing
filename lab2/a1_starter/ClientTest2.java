import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportFactory;

public class ClientTest2 {

    private static final List<String> SHORT_PASSWORDS = Arrays.asList(
            new String[] { "", "password", "123456789", "hello world", "i hope we finish this assignment on time", });

    // Hardcoded for testing
    private static final String FE_HOST = "ecetesla2";
    private static final int FE_PORT = 10750;

    private static BcryptService.Client CLIENT;
    private static TTransport TRANSPORT;

    private static void assertEqual(int expected, int actual) {
        if (expected != actual) {
            System.out.println("Expected " + expected + " but got " + actual);
        }
        assert (expected == actual);
    }

    private static void setupNewThriftConnection() throws TException {
        TSocket sock = new TSocket(FE_HOST, FE_PORT);
        TRANSPORT = new TFramedTransport(sock);
        TProtocol protocol = new TBinaryProtocol(TRANSPORT);
        CLIENT = new BcryptService.Client(protocol);
    }

    /**
     * Tests multi threaded client operations and prints execution time.
     */
    private static void timeOperationsMultithreadedClient(int numThreads, short logRounds, int numPasswordsPerRequest)
            throws TException {
        System.out.println(String.format("\nRuntime from %d threaded client, %d password per request, logRounds=%d",
                numThreads, numPasswordsPerRequest, logRounds));
        List<Thread> threads = new ArrayList<>();
        List<String> passwords = new ArrayList<>();
        for (int __ = 0; __ < numPasswordsPerRequest; ++__) {
            byte bytes[] = new byte[1024];
            Random rand = new Random();
            rand.nextBytes(bytes);
            String password = new String(bytes);
            passwords.add(password);
        }

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numThreads; ++i) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        TSocket sock = new TSocket(FE_HOST, FE_PORT);
                        TTransport transport = new TFramedTransport(sock);
                        TProtocol protocol = new TBinaryProtocol(transport);
                        BcryptService.Client client = new BcryptService.Client(protocol);
                        transport.open();
                        List<String> hashes = client.hashPassword(passwords, logRounds);
                        transport.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            threads.add(t);
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        float throughput = numPasswordsPerRequest * numThreads * 1000f / (endTime - startTime);
        System.out.println("Throughput: " + throughput + " operations per second");
        System.out.println("Latency: " + (float) (endTime - startTime) / (numPasswordsPerRequest * numThreads) + " ms");
    }

    /**
     * Tests single threaded client operations and prints execution time. Copied the
     * code structure of Calibratr.java
     */
    private static void timeOperationsSingleThread() throws TException {
        System.out.println("\nRuntime from single threaded client with 1 password per request");
        int numRounds = 256;
        for (short s = 8; s <= 12; s++) {
            byte bytes[] = new byte[1024];
            Random rand = new Random();
            rand.nextBytes(bytes);
            String password = new String(bytes);
            List<String> passwords = new ArrayList<>();
            passwords.add(password);
            long startTime = System.currentTimeMillis();
            int n = 4096 / numRounds; // 16, 8, 4, 2, 1
            for (int i = 0; i < n; i++) {
                List<String> hashes = CLIENT.hashPassword(passwords, s);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Throughput for logRounds=" + s + ": " + n * 1000f / (endTime - startTime));
            System.out.println("Latency for logRounds=" + s + ": " + (endTime - startTime) / n);
            numRounds *= 2;
        }
    }

    /**
     * Tests for correctness on multiple threads
     *
     * @throws TException
     */
    private static void testPasswordsMultiThread(int numThreads, int numPasswordsPerRequest) throws TException {
        List<Thread> threads = new ArrayList<>();
        CopyOnWriteArrayList<Boolean> results = new CopyOnWriteArrayList<>();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < numThreads; ++i) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        TSocket sock = new TSocket(FE_HOST, FE_PORT);
                        TTransport transport = new TFramedTransport(sock);
                        TProtocol protocol = new TBinaryProtocol(transport);
                        BcryptService.Client client = new BcryptService.Client(protocol);
                        transport.open();
                        List<String> passwords = new ArrayList<>();
                        for (int __ = 0; __ < numPasswordsPerRequest; ++__) {
                            byte bytes[] = new byte[1024];
                            Random rand = new Random();
                            rand.nextBytes(bytes);
                            String password = new String(bytes);
                            passwords.add(password);
                        }
                        List<String> hashes = client.hashPassword(passwords, (short) ((new Random().nextInt(10)) + 4));
                        results.addAll(client.checkPassword(passwords, hashes));
                        transport.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            threads.add(t);
            t.start();
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (Boolean res : results) {
            assert res;
        }
        System.out.println("testPasswordsMultiThread passed");
    }

    /**
     * Tests with 8 batches of 16 strings of various lengths for correctness on a
     * single threaded client
     *
     * @throws TException
     */
    private static void testPasswordsSingleThread() throws TException {
        List<List<String>> input = new ArrayList<>();
        int batchSize = 16;
        int numBatches = 8;
        for (int i = 0; i < numBatches * batchSize && i < Passwords.PASSWORDS.length - batchSize; i += batchSize) {
            List<String> batch = new ArrayList<>();
            for (int j = i; j < i + batchSize; ++j) {
                batch.add(Passwords.PASSWORDS[j]);
            }
            input.add(batch);
        }
        for (List<String> batch : input) {
            List<String> hashes = CLIENT.hashPassword(batch, (short) ((new Random().nextInt(10)) + 4));
            List<Boolean> results = CLIENT.checkPassword(batch, hashes);
            assertEqual(batch.size(), results.size());
            for (Boolean res : results) {
                assert (res);
            }
        }
        System.out.println("testPasswordsSingleThread passed");
    }

    /**
     * Tests with 1 batch of 1024 strings of various lengths on a single thread for
     * correctness
     *
     * @throws TException
     */
    private static void testSingleLargeBatchSingleThread() throws TException {
        List<String> passwords = Arrays.asList(Passwords.PASSWORDS);
        List<String> hashes = CLIENT.hashPassword(passwords, (short) ((new Random().nextInt(10)) + 4));
        assertEqual(Passwords.PASSWORDS.length, hashes.size());
        List<Boolean> results = CLIENT.checkPassword(passwords, hashes);
        for (Boolean res : results) {
            assert res;
        }
        System.out.println("testSingleLargeBatchSingleThread passed");
    }

    /**
     * Test with small number of passwords for correctness. Also tests error cases.
     */
    private static void testShortPasswords() throws TException {
        List<String> hashes = CLIENT.hashPassword(SHORT_PASSWORDS, (short) 10);
        List<Boolean> results = CLIENT.checkPassword(SHORT_PASSWORDS, hashes);
        assertEqual(SHORT_PASSWORDS.size(), results.size());
        for (Boolean res : results) {
            assert (res);
        }

        // shift each hash by one position
        String first_hash = hashes.get(0);
        for (int i = 0; i < hashes.size() - 1; ++i) {
            hashes.set(i, hashes.get((i + 1) % hashes.size()));
        }
        hashes.set(hashes.size() - 1, first_hash);
        results = CLIENT.checkPassword(SHORT_PASSWORDS, hashes);
        assertEqual(SHORT_PASSWORDS.size(), results.size());
        for (Boolean res : results) {
            assert (!res);
        }

        // Invalid hash
        for (int i = 0; i < hashes.size(); ++i) {
            hashes.set(i, "too short" + i);
        }
        results = CLIENT.checkPassword(SHORT_PASSWORDS, hashes);
        for (Boolean res : results) {
            assert (!res);
        }

        // Empty hash
        for (int i = 0; i < hashes.size(); ++i) {
            hashes.set(i, "");
        }
        results = CLIENT.checkPassword(SHORT_PASSWORDS, hashes);
        for (Boolean res : results) {
            assert (!res);
        }

        // invalid arguments
        List<String> empty = new ArrayList<>();
        try {
            hashes = CLIENT.hashPassword(empty, (short) 10);
            assert false;
        } catch (TException e) {
        }

        try {
            results = CLIENT.checkPassword(empty, hashes);
            assert false;
        } catch (TException e) {
        }

        try {
            results = CLIENT.checkPassword(SHORT_PASSWORDS, empty);
            assert false;
        } catch (TException e) {
        }

        try {
            // logrounds is out of range
            hashes = CLIENT.hashPassword(SHORT_PASSWORDS, (short) 3);
            assert false;
        } catch (TException e) {
        }

        try {
            // logrounds is out of range
            hashes = CLIENT.hashPassword(SHORT_PASSWORDS, (short) 31);
            assert false;
        } catch (TException e) {
        }

        try {
            hashes.remove(0); // passwords and hashes are different lengths
            results = CLIENT.checkPassword(SHORT_PASSWORDS, hashes);
            assert false;
        } catch (TException e) {
        }

        System.out.println("TestShortPasswords passed");
    }

    public static void main(String[] args) {
        try {
            setupNewThriftConnection();
            TRANSPORT.open();

            // Correctness tests
            testShortPasswords();
            testPasswordsSingleThread();
            testSingleLargeBatchSingleThread();
            testPasswordsMultiThread(10, 5);
            TRANSPORT.close();

            // timeOperationsSingleThread();

            // Efficiency tests
            timeOperationsMultithreadedClient(1, (short) 12, 20);
            timeOperationsMultithreadedClient(1, (short) 12, 16);
            timeOperationsMultithreadedClient(4, (short) 12, 4);
            timeOperationsMultithreadedClient(16, (short) 12, 1);
            timeOperationsMultithreadedClient(20, (short) 12, 1);
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
