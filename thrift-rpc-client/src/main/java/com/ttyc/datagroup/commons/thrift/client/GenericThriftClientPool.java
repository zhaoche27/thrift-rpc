package com.ttyc.datagroup.commons.thrift.client;

import com.ttyc.datagroup.commons.Weight;
import com.ttyc.datagroup.commons.thrift.ThriftServiceServerWeight;
import com.ttyc.datagroup.commons.util.AtomicPositiveInteger;
import com.ttyc.datagroup.commons.util.Constants;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class GenericThriftClientPool implements Weight, Closeable {

    //服务权重
    private volatile ThriftServiceServerWeight thriftServiceServerWeight;

    private final GenericObjectPool<TServiceClient> genericObjectPool;

    private final int defaultWeight;

    private final AtomicPositiveInteger transportFailHappenCount = new AtomicPositiveInteger(0);

    public GenericThriftClientPool(ThriftServiceServerWeight thriftServiceServerWeight, GenericObjectPool.Config poolConfig
            , TServiceClientFactory<TServiceClient> tServiceClientFactory, int connectionTimeout, int socketTimeout) {
        this.thriftServiceServerWeight = thriftServiceServerWeight;
        this.defaultWeight = thriftServiceServerWeight.getWeight();
        genericObjectPool = new GenericObjectPool(new ThriftClientFactory(tServiceClientFactory
                , thriftServiceServerWeight.getHostAndPort(), connectionTimeout, socketTimeout), poolConfig);
    }


    private void setDefaultTransportFailHappenCount() {
        transportFailHappenCount.set(0);
    }

    public String getThriftServiceServerAddressAndPort() {
        return thriftServiceServerWeight.getHostAndPort().toString();
    }

    public void setThriftServiceServerWeight(ThriftServiceServerWeight thriftServiceServerWeight) {
        this.thriftServiceServerWeight = thriftServiceServerWeight;
        setDefaultTransportFailHappenCount();
    }

    public TServiceClient getClient() throws Exception {
        return genericObjectPool.borrowObject();
    }

    /**
     * 回收客户端
     *
     * @param client
     * @param isValid
     * @throws Exception
     */
    public void recycleClient(TServiceClient client, boolean isValid) throws Exception {
        if (isValid) {
            if (client != null) {
                genericObjectPool.returnObject(client);
            }
            setWeight(defaultWeight);
        } else {
            demotionWeight();
            if (client != null) {
                genericObjectPool.invalidateObject(client);
            }
        }
    }

    /**
     * 降级权重
     */
    private void demotionWeight() {
        transportFailHappenCount.incrementAndGet();
        if (this.transportFailHappenCount.get() >= Constants.MAX_CLIENT_CONNECTION_FAIL_COUNT) {
            int w = getWeight() / Constants.DEFAULT_SERVER_WEIGHT_STEP;
            setWeight(w > 0 ? w : Constants.DEFAULT_SERVER_MIN_WEIGHT);
        }
    }


    private void setWeight(int weight) {
        thriftServiceServerWeight.setWeight(weight);
        setDefaultTransportFailHappenCount();
    }


    @Override
    public int getWeight() {
        return thriftServiceServerWeight.getWeight();
    }


    @Override
    public String getKey() {
        return thriftServiceServerWeight.getHostAndPort().toString();
    }

    @Override
    public void close() throws IOException {
        if (genericObjectPool != null) {
            try {
                genericObjectPool.close();
            } catch (Exception e) {

            }
        }
    }
}
