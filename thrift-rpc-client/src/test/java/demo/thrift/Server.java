package demo.thrift;

/**
 * Created by zhaoche on 2017/2/3.
 */

import com.ttyc.datagroup.commons.thrift.server.ThriftServerAddressRegister;
import com.ttyc.datagroup.commons.thrift.server.ThriftServerAddressRegisterZookeeper;
import com.ttyc.datagroup.commons.thrift.server.ThriftServiceServer;
import com.ttyc.datagroup.commons.zookeeper.ZkEnv;
import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import com.ttyc.datagroup.commons.zookeeper.zkclient.ZKClientZookeeperClient;
import demo.thrift.tutorial.Calculator;
import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

import java.lang.instrument.IllegalClassFormatException;
import java.lang.reflect.Constructor;

public class Server {

    public static CalculatorHandler service;

    public static TProcessor getTProcessor(Object service) throws IllegalClassFormatException {
        Class serviceClass = service.getClass();
        Class[] interfaces = serviceClass.getInterfaces();
        if (interfaces.length == 0) {
            throw new IllegalClassFormatException("service-class should implements Iface");
        }
        TProcessor processor = null;
        String serviceName;
        for (Class clazz : interfaces) {
            String clazzSimpleName = clazz.getSimpleName();
            if (!clazzSimpleName.equals("Iface")) {
                continue;
            }
            serviceName = clazz.getEnclosingClass().getName();
            String tProcessorClassName = serviceName + "$Processor";
            try {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                Class tProcessorClass = classLoader.loadClass(tProcessorClassName);
                if (!TProcessor.class.isAssignableFrom(tProcessorClass)) {
                    continue;
                }
                Constructor<?> constructor = tProcessorClass.getConstructor(clazz);
                processor = (TProcessor) constructor.newInstance(service);
                break;
            } catch (Exception e) {

            }
        }
        if (processor == null) {
            throw new IllegalClassFormatException("service-class should implements Iface");
        }
        return processor;
    }

    public static void main(String[] args) {
        //service = new CalculatorHandler();
        //nonBlock(getTProcessor(service));
        BasicConfigurator.configure();
        ZookeeperClient zookeeperClient = new ZKClientZookeeperClient("10.10.104.4:2181,10.10.103.230:2181,10.10.112.18:2181");
        ThriftServerAddressRegister thriftServerAddressRegister = new ThriftServerAddressRegisterZookeeper(zookeeperClient, ZkEnv.BETA);
        try {
            service = new CalculatorHandler();
            ThriftServiceServer thriftServiceServer = new ThriftServiceServer(service, 9988, 20, thriftServerAddressRegister);
            thriftServiceServer.startService();
            System.in.read();
            thriftServerAddressRegister.close();
            thriftServiceServer.close();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(Calculator.Processor processor) {
        try {
            System.out.println(processor.getClass().getCanonicalName());
            TServerTransport serverTransport = new TServerSocket(9988);
            TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport);
            args.maxWorkerThreads(10);
            args.minWorkerThreads(2);
            TServer server = new TThreadPoolServer(args.processor(processor));
            System.out.println("Starting the secure server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void nonBlock(TProcessor processor) throws TTransportException {
        TNonblockingServerSocket serverTransport = new TNonblockingServerSocket(9988);
        TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(serverTransport);
        TProcessorFactory processorFactory = new TProcessorFactory(processor);
        tArgs.processorFactory(processorFactory);
        tArgs.workerThreads(10);
        tArgs.transportFactory(new TFramedTransport.Factory());
        tArgs.protocolFactory(new TBinaryProtocol.Factory(true, true));
        TServer server = new TThreadedSelectorServer(tArgs);
        server.serve();
    }
}
