package com.ttyc.datagroup.commons.thrift.client;

import com.ttyc.datagroup.commons.Weight;
import com.ttyc.datagroup.commons.thrift.ThriftServiceServerWeight;
import com.ttyc.datagroup.commons.thrift.client.loadbalance.RandomLoadBalance;
import com.ttyc.datagroup.commons.thrift.client.loadbalance.RoundRobinLoadBalance;
import com.ttyc.datagroup.commons.thrift.exceptions.ThriftClientException;
import com.ttyc.datagroup.commons.thrift.monitor.MonitorZkInfo;
import com.ttyc.datagroup.commons.thrift.monitor.ThriftMonitor;
import com.ttyc.datagroup.commons.util.Constants;
import com.ttyc.datagroup.commons.util.Nets;
import com.ttyc.datagroup.commons.util.Systems;
import com.ttyc.datagroup.commons.zookeeper.ChildListener;
import com.ttyc.datagroup.commons.zookeeper.StateListener;
import com.ttyc.datagroup.commons.zookeeper.ZkEnv;
import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoche on 2017/2/6.
 * ThriftClient 客户端池
 * 请使用单例模式
 */
public class ThriftClientPool implements Closeable, ChildListener, StateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftClientPool.class);

    private final ConcurrentHashMap<String, GenericThriftClientPool> MAP = new ConcurrentHashMap<String, GenericThriftClientPool>();

    private final String serviceClassName;

    private final LoadBalance loadBalance;

    private final String serverRootPath;

    private final String clientPath;

    private final ZookeeperClient zookeeperClient;

    private final GenericObjectPool.Config poolConfig;

    private final String thriftClientExceptionMsg;

    private final Object proxyClient;

    private final int connectionTimeout;

    private final int socketTimeout;

    private final TServiceClientFactory<TServiceClient> tServiceClientFactory;

    // 访问服务所需的权限token
    private final String authToken;

    public ThriftClientPool(ZookeeperClient zookeeperClient, ZkEnv zkEnv, String authToken, Class serviceClazz) {
        this(zookeeperClient, zkEnv, authToken, serviceClazz, Constants.DEFAULT_CLIENT_CONNECTION_NUM);
    }

    public ThriftClientPool(ZookeeperClient zookeeperClient, ZkEnv zkEnv, String authToken
            , Class serviceClazz, int maxClientConnectionNum) {
        this(zookeeperClient, zkEnv, authToken, serviceClazz
                , maxClientConnectionNum, Constants.DEFAULT_CONNECTION_TIMEOUT_MS
                , Constants.DEFAULT_SOCKET_TIMEOUT_MS, Constants.DEFAULT_RETRY_COUNT);
    }

    public ThriftClientPool(ZookeeperClient zookeeperClient, ZkEnv zkEnv
            , String authToken, Class serviceClazz, int maxClientConnectionNum, int connectionTimeout, int socketTimeout, int retryCount) {
        this(zookeeperClient, zkEnv, authToken, serviceClazz
                , maxClientConnectionNum, connectionTimeout, socketTimeout, retryCount, LoadBalance.LOAD_BALANCE_RANDOM);
    }

    public ThriftClientPool(ZookeeperClient zookeeperClient, ZkEnv zkEnv
            , final String authToken, Class serviceClazz, int maxClientConnectionNum, int connectionTimeout
            , int socketTimeout, int retryCount, String loadBalanceName) {
        try {
            this.zookeeperClient = zookeeperClient;
            this.serviceClassName = serviceClazz.getName();
            this.authToken = authToken;
            if (serviceClassName.equals(authToken)) {
                throw new IllegalArgumentException("authToken not equal to serviceClassName");
            }
            String untilVersionPath = String.format(Constants.THRIFT_SERVER_REGISTER_ZOOKEEPER_PATH_ROOT, zkEnv.getMark()) + "/" + this.serviceClassName;
            this.clientPath = untilVersionPath + "/client/" + Nets.getLocalIp() + "-" + Systems.getPid();
            ThriftMonitor.addMonitorZkInfoMap(authToken, new MonitorZkInfo(this.clientPath, true, zookeeperClient));
            this.serverRootPath = untilVersionPath + "/server";
            this.thriftClientExceptionMsg = "zk node :" + serverRootPath + " not children, Please provide the service";
            zookeeperClient.create(clientPath, null, true);
            zookeeperClient.addStateListener(this);
            if (LoadBalance.LOAD_BALANCE_RANDOM_ROBIN.equals(loadBalanceName)) {
                loadBalance = new RoundRobinLoadBalance();
            } else {
                loadBalance = new RandomLoadBalance();
            }
            this.connectionTimeout = connectionTimeout;
            this.socketTimeout = socketTimeout;
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            poolConfig = new GenericObjectPool.Config();
            poolConfig.maxActive = maxClientConnectionNum;
            poolConfig.maxIdle = maxClientConnectionNum;
            poolConfig.numTestsPerEvictionRun = -1;
            poolConfig.maxWait = Constants.MAX_WAIT_CLIENT_CONNECTION_MS;
            poolConfig.timeBetweenEvictionRunsMillis = (long) (poolConfig.minEvictableIdleTimeMillis * 1.2);
            Class iFaceClass = classLoader.loadClass(serviceClassName + "$Iface");
            Class<TServiceClientFactory<TServiceClient>> tServiceClientFactoryClass
                    = (Class<TServiceClientFactory<TServiceClient>>) classLoader.loadClass(serviceClassName + "$Client$Factory");
            tServiceClientFactory = tServiceClientFactoryClass.newInstance();
            final int maxRetryCount = retryCount < 0 ? 0 : retryCount;
            this.proxyClient = Proxy.newProxyInstance(classLoader, new Class[]{iFaceClass}, new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    int i = 0;
                    Object object = null;
                    GenericThriftClientPool genericThriftClientPool = null;
                    TServiceClient client = null;
                    String methodName = method.getName();
                    boolean flag;
                    while (true) {
                        flag = true;
                        try {
                            genericThriftClientPool = getGenericThriftClientPool();
                            client = genericThriftClientPool.getClient();
                            object = method.invoke(client, args);
                            break;
                        } catch (Exception e) {
                            TTransportException tTransportException = null;
                            if (e instanceof InvocationTargetException) {
                                Throwable t = ((InvocationTargetException) e).getTargetException();
                                if (t != null) {
                                    if (t instanceof TTransportException) {
                                        tTransportException = (TTransportException) t;
                                    } else {
                                        throw t;
                                    }
                                }
                            } else if (e instanceof TTransportException) {
                                tTransportException = (TTransportException) e;
                            }
                            if (tTransportException != null) {
                                flag = false;
                                if (maxRetryCount == 0 || i == maxRetryCount || tTransportException.getCause() == null) {
                                    if (tTransportException.getCause() != null) {
                                        ThriftMonitor.recordOneClientCallTransferFail(authToken, methodName);
                                    }
                                    LOGGER.error("Client call method :{}() request service({}-{}),retry count:{},happen exception:{} "
                                            , method.getName()
                                            , genericThriftClientPool.getThriftServiceServerAddressAndPort(), serviceClassName, i, tTransportException.getMessage());
                                    throw tTransportException;
                                } else {
                                    LOGGER.warn("Client call method :{}() request service({}-{}),retry count:{},happen exception:{} ", method.getName()
                                            , genericThriftClientPool.getThriftServiceServerAddressAndPort(), serviceClassName, i, tTransportException.getMessage());
                                }
                            } else {
                                throw e;
                            }
                        } finally {
                            if (genericThriftClientPool != null) {
                                genericThriftClientPool.recycleClient(client, flag);
                            }
                            ThriftMonitor.recordOneClientCall(authToken, methodName);
                        }
                        i++;
                    }
                    return object;
                }
            });
        } catch (Exception e) {
            throw new ThriftClientException(e);
        }
    }

    /**
     * 初始化
     *
     * @throws Exception
     */
    public void init() throws Exception {
        loadMAP(null);
        zookeeperClient.addChildListener(serverRootPath, this);
    }

    private void loadMAP(List<String> children) throws Exception {
        List<ThriftServiceServerWeight> list = getThriftServiceWeights(children);
        if (list.isEmpty()) {
            MAP.clear();
            throw new ThriftClientException(thriftClientExceptionMsg);
        } else {
            GenericThriftClientPool genericThriftClientPool;
            String hostAndPortString;
            Set<String> set = new HashSet<>(MAP.keySet());
            for (ThriftServiceServerWeight thriftServiceServerWeight : list) {
                hostAndPortString = thriftServiceServerWeight.getHostAndPort().toString();
                genericThriftClientPool = MAP.get(hostAndPortString);
                if (genericThriftClientPool == null) {
                    genericThriftClientPool = new GenericThriftClientPool(thriftServiceServerWeight, poolConfig
                            , tServiceClientFactory, connectionTimeout, socketTimeout);
                    MAP.put(hostAndPortString, genericThriftClientPool);
                } else {
                    genericThriftClientPool.setThriftServiceServerWeight(thriftServiceServerWeight);
                }
                set.remove(hostAndPortString);
            }
            for (String hp : set) {
                genericThriftClientPool = MAP.remove(hp);
                if (genericThriftClientPool != null) {
                    genericThriftClientPool.close();
                }
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("load service name:{}, server list:{}", serviceClassName, MAP.keySet());
            }
        }
    }

    /**
     * 获取推荐的clientpool
     *
     * @return
     */
    private GenericThriftClientPool getGenericThriftClientPool() {
        GenericThriftClientPool genericThriftClientPool = null;
        List<Weight> weights = new ArrayList<>(32);
        String hostAndPortString;
        while (genericThriftClientPool == null) {
            weights.clear();
            weights.addAll(MAP.values());
            if (weights.isEmpty()) {
                throw new ThriftClientException(thriftClientExceptionMsg);
            } else {
                Weight weight = loadBalance.select(weights);
                hostAndPortString = weight.getKey();
                genericThriftClientPool = MAP.get(hostAndPortString);
                if (genericThriftClientPool != null) {
                    break;
                }
            }
        }
        return genericThriftClientPool;
    }

    /**
     * 获取客户端
     *
     * @return
     */
    public Object getThriftClient() {
        return proxyClient;
    }

    /**
     * 获取服务提供者
     *
     * @return
     */
    private List<ThriftServiceServerWeight> getThriftServiceWeights(List<String> children) {
        List<String> list = children;
        if (list == null) {
            list = zookeeperClient.getChildren(serverRootPath);
        }
        list.remove(Nets.LOCALHOST);
        List<ThriftServiceServerWeight> thriftServiceServerWeights = new ArrayList<ThriftServiceServerWeight>();
        if (list.isEmpty()) {
            return thriftServiceServerWeights;
        }
        ThriftServiceServerWeight thriftServiceServerWeight;
        for (String str : list) {
            thriftServiceServerWeight = ThriftServiceServerWeight.parse(str);
            thriftServiceServerWeights.add(thriftServiceServerWeight);
        }
        return thriftServiceServerWeights;
    }

    @Override
    public void close() throws IOException {
        for (GenericThriftClientPool genericThriftClientPool : MAP.values()) {
            genericThriftClientPool.close();
        }
    }

    @Override
    public void childChanged(String path, List<String> children) {
        try {
            if (serverRootPath.equals(path)) {
                loadMAP(children);
            } else {
                LOGGER.error("childChanged path:{},serverRootPath:{}, not appearance consistent", path, serverRootPath);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    @Override
    public void stateChanged(int connected) {
        if (connected == StateListener.RECONNECTED) {
            zookeeperClient.create(clientPath, null, true);
        }
    }
}
