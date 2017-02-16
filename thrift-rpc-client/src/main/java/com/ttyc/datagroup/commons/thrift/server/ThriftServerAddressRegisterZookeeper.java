package com.ttyc.datagroup.commons.thrift.server;

import com.ttyc.datagroup.commons.thrift.ThriftServiceServerWeight;
import com.ttyc.datagroup.commons.thrift.monitor.MonitorZkInfo;
import com.ttyc.datagroup.commons.thrift.monitor.ThriftMonitor;
import com.ttyc.datagroup.commons.util.Constants;
import com.ttyc.datagroup.commons.zookeeper.StateListener;
import com.ttyc.datagroup.commons.zookeeper.ZkEnv;
import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class ThriftServerAddressRegisterZookeeper implements ThriftServerAddressRegister, StateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftServerAddressRegisterZookeeper.class);

    private final ZookeeperClient zookeeperClient;

    private final String rootPath;

    private Set<String> servicePathSet = new LinkedHashSet<>();

    public ThriftServerAddressRegisterZookeeper(ZookeeperClient zookeeperClient, ZkEnv zkEnv) {
        this.zookeeperClient = zookeeperClient;
        rootPath = String.format(Constants.THRIFT_SERVER_REGISTER_ZOOKEEPER_PATH_ROOT, zkEnv.getMark());
        //创建根节点
        zookeeperClient.create(rootPath, null, false);
        zookeeperClient.addStateListener(this);
    }

    @Override
    public void register(String serviceName, ThriftServiceServerWeight thriftServiceServerWeight) {
        String servicePath = rootPath + "/" + serviceName + "/server/" + thriftServiceServerWeight.toString();
        String serviceMonitorPath = rootPath + "/" + serviceName + "/monitor/" + thriftServiceServerWeight.getHostAndPort().toString();
        servicePathSet.add(servicePath);
        ThriftMonitor.addMonitorZkInfoMap(serviceName
                , new MonitorZkInfo(serviceMonitorPath, false, zookeeperClient));
        zookeeperClient.create(servicePath, null, true);
        zookeeperClient.create(serviceMonitorPath, null, true);
        LOGGER.info("register thrift rpc service:{}", servicePath);
    }

    @Override
    public void close() throws IOException {
        zookeeperClient.close();
    }

    @Override
    public void stateChanged(int connected) {
        if (connected == StateListener.RECONNECTED) {
            for (String servicePath : servicePathSet) {
                zookeeperClient.create(servicePath, null, true);
            }
        }
    }
}
