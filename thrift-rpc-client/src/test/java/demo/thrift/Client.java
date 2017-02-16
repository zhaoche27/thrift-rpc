package demo.thrift;

/**
 * Created by zhaoche on 2017/2/3.
 */

import com.ttyc.datagroup.commons.thrift.client.ThriftClientPool;
import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import com.ttyc.datagroup.commons.zookeeper.zkclient.ZKClientZookeeperClient;
import demo.thrift.tutorial.Calculator;
import demo.thrift.tutorial.InvalidOperationException;
import demo.thrift.tutorial.Operation;
import demo.thrift.tutorial.Work;
import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static com.ttyc.datagroup.commons.thrift.client.LoadBalance.LOAD_BALANCE_RANDOM;
import static com.ttyc.datagroup.commons.zookeeper.ZkEnv.BETA;

public class Client {

    private static Logger LOGGER = LoggerFactory.getLogger(Client.class);

    public static void simple() {
        TTransport tsocket;
        tsocket = new TSocket("172.15.70.94", 9988, 1000);
        TTransport transport = new TFramedTransport(tsocket);
        TProtocol protocol = new TBinaryProtocol(transport);
        try {
            transport.open();
            Calculator.Client client = new Calculator.Client(protocol);
            client.ping();
            System.out.println(client.add(1, 2));
        } catch (Exception e) {
            if (e instanceof TTransportException) {
                System.out.println("==================================");
            }
            e.printStackTrace();
        }
    }

    public static <T> T execute(Callable<T> callable) throws Exception {
        int i = 0;
        while (i < 2) {
            i++;
            try {
                return callable.call();
            } catch (Exception e) {
                throw e;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        //simple();
        ZookeeperClient zookeeperClient = new ZKClientZookeeperClient("10.10.104.4:2181,10.10.103.230:2181,10.10.112.18:2181");
        ThriftClientPool thriftClientPool;
        thriftClientPool = new ThriftClientPool(zookeeperClient, BETA, "thrift-client-test", Calculator.class
                , 30, 1000, 2000, 1, LOAD_BALANCE_RANDOM);
        thriftClientPool.init();
        final Calculator.Iface client = (Calculator.Iface) thriftClientPool.getThriftClient();
        //perform(client);
        //thriftClientPool.close();
        //System.in.read();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1
                , 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(30000));
        int count = 30000;
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        try {
            long ms = System.currentTimeMillis();
            int i = 0;
            while (i < count) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            client.add(1,2);
                        } catch (TException e) {
                            e.printStackTrace();
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
                i++;
            }
            countDownLatch.await();
            long uMs = System.currentTimeMillis() - ms;
            System.out.println(uMs);
            System.out.println(1.0 * count / uMs);
            System.out.println("====================================");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executor.shutdown();
            thriftClientPool.close();
        }


    }

    private static void perform(Calculator.Iface client) throws TException, InterruptedException {
        client.ping();
        System.out.println("ping()");
        int sum = client.add(1, 1);
        System.out.println("1+1=" + sum);
        Work work = new Work();
        work.op = Operation.DIVIDE;
        work.num1 = 1;
        work.num2 = 0;
        try {
            int quotient = client.calculate(0, work);
            System.out.println(quotient);
        } catch (InvalidOperationException io) {
            System.out.println("Invalid operation: " + io.why);
        }
        /*work.op = Operation.SUBTRACT;
        work.num1 = 15;
        work.num2 = 10;
        try {
            int diff = client.calculate(1, work);
            System.out.println("15-10=" + diff);
        } catch (InvalidOperationException io) {
            System.out.println("Invalid operation: " + io.why);
        }

        SharedStruct log = client.getStruct(1);
        System.out.println("Check log: " + log.value);*/
    }
}
