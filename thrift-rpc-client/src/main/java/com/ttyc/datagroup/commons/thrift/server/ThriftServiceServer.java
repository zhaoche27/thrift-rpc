package com.ttyc.datagroup.commons.thrift.server;

import com.ttyc.datagroup.commons.HostAndPort;
import com.ttyc.datagroup.commons.thrift.ThriftServiceServerWeight;
import com.ttyc.datagroup.commons.thrift.exceptions.ThriftClientException;
import com.ttyc.datagroup.commons.util.Constants;
import com.ttyc.datagroup.commons.util.Nets;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.reflect.Constructor;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class ThriftServiceServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftServiceServer.class);

    //服务注册本机端口
    private final int port;

    //权重,越大表示处理能力有越强
    private final int weight;

    //服务实现类
    private final Object service;

    //服务注册
    private final ThriftServerAddressRegister thriftServerAddressRegister;

    //selector 线程数,用于接受连接
    private int selectorThreadNum;

    //工作线程数,用于处理连接
    private int workThreadNum;

    private ServerThread serverThread;

    public ThriftServiceServer(Object service
            , ThriftServerAddressRegister thriftServerAddressRegister) {
        this(service, Constants.DEFAULT_SERVER_WEIGHT, thriftServerAddressRegister);
    }

    public ThriftServiceServer(Object service, int weight
            , ThriftServerAddressRegister thriftServerAddressRegister) {
        this(service, 0, weight, thriftServerAddressRegister);
    }

    public ThriftServiceServer(Object service, int port, int weight
            , ThriftServerAddressRegister thriftServerAddressRegister) {
        this(service, port, weight, thriftServerAddressRegister, 0, 0);
    }

    public ThriftServiceServer(Object service, int port, int weight
            , ThriftServerAddressRegister thriftServerAddressRegister, int selectorThreadNum, int workThreadNum) {
        this.port = Nets.getAvailablePort(port);
        if (weight < 0) {
            this.weight = 1;
        } else {
            this.weight = weight;
        }
        this.service = service;
        this.thriftServerAddressRegister = thriftServerAddressRegister;
        this.selectorThreadNum = selectorThreadNum;
        this.workThreadNum = workThreadNum;
    }

    /**
     * 启动服务
     */
    public void startService() {
        try {
            Class serviceClass = service.getClass();
            Class[] interfaces = serviceClass.getInterfaces();
            if (interfaces.length == 0) {
                throw new IllegalClassFormatException("service-class should implements Iface");
            }
            TProcessor processor = null;
            String serviceName = null;
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
                    Constructor constructor = tProcessorClass.getConstructor(clazz);
                    processor = (TProcessor) constructor.newInstance(service);
                    break;
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }
            if (processor == null) {
                throw new IllegalClassFormatException("service-class should implements Iface");
            }
            serverThread = new ServerThread(processor, port, selectorThreadNum, workThreadNum);
            serverThread.start();
            // 注册服务
            if (thriftServerAddressRegister != null) {
                ThriftServiceServerWeight thriftServiceServerWeight = new ThriftServiceServerWeight(new HostAndPort(Nets.getLocalIp(), port), weight);
                thriftServerAddressRegister.register(serviceName, thriftServiceServerWeight);
            }
        } catch (Exception e) {
            throw new ThriftClientException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (serverThread != null) {
            serverThread.close();
        }
    }

    /**
     * 服务线程
     */
    private static class ServerThread extends Thread implements Closeable {
        private static final Logger LOGGER = LoggerFactory.getLogger(ServerThread.class);
        private TServer server;

        public ServerThread(TProcessor processor, int port, int selectorThreadNum, int workThreadNum) throws TTransportException {
            TNonblockingServerSocket tServerTransport = new TNonblockingServerSocket(port);
            TThreadedSelectorServer.Args tArgs = new TThreadedSelectorServer.Args(tServerTransport);
            TProcessorFactory processorFactory = new TProcessorFactory(processor);
            tArgs.processorFactory(processorFactory);
            if (selectorThreadNum > 0) {
                tArgs.selectorThreads(selectorThreadNum);
            }
            if (workThreadNum > 0) {
                tArgs.workerThreads(workThreadNum);
            }
            tArgs.protocolFactory(new TCompactProtocol.Factory());
            server = new TThreadedSelectorServer(tArgs);
        }

        @Override
        public void run() {
            try {
                server.serve();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        @Override
        public void close() throws IOException {
            if (server != null) {
                server.stop();
            }
        }
    }
}
