package com.ttyc.datagroup.commons.thrift.client;

import com.ttyc.datagroup.commons.HostAndPort;
import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class ThriftClientFactory extends BasePoolableObjectFactory<TServiceClient> {


    private final TServiceClientFactory<TServiceClient> clientFactory;

    private final HostAndPort serviceHostAndPort;

    private final int connectionTimeout;

    private final int socketTimeout;

    public ThriftClientFactory(TServiceClientFactory<TServiceClient> clientFactory, HostAndPort serviceHostAndPort
            , int connectionTimeout, int socketTimeout) {
        this.clientFactory = clientFactory;
        this.serviceHostAndPort = serviceHostAndPort;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void destroyObject(TServiceClient obj) throws Exception {
        obj.getInputProtocol().getTransport().close();
        obj.getOutputProtocol().getTransport().close();
    }

    @Override
    public boolean validateObject(TServiceClient client) {
        TTransport pin = client.getInputProtocol().getTransport();
        TTransport pout = client.getOutputProtocol().getTransport();
        return pin.isOpen() && pout.isOpen();
    }

    @Override
    public void activateObject(TServiceClient obj) throws Exception {
    }

    @Override
    public void passivateObject(TServiceClient obj) throws Exception {
    }

    @Override
    public TServiceClient makeObject() throws Exception {
        TSocket tsocket = new TSocket(serviceHostAndPort.getHost(), serviceHostAndPort.getPort()
                , socketTimeout, connectionTimeout);
        TTransport transport = new TFramedTransport(tsocket);
        TProtocol protocol = new TCompactProtocol(transport);
        TServiceClient client = this.clientFactory.getClient(protocol);
        transport.open();
        return client;
    }

    public HostAndPort getServiceHostAndPort() {
        return serviceHostAndPort;
    }
}
