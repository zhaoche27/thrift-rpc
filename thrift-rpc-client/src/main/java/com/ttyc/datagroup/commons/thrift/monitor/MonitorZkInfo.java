package com.ttyc.datagroup.commons.thrift.monitor;

import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;

/**
 * Created by zhaoche on 2017/2/10.
 */
public class MonitorZkInfo {

    private final String monitorPath;
    private final boolean isHavePrefix;
    private final ZookeeperClient zookeeperClient;

    public MonitorZkInfo(String monitorPath, boolean isHavePrefix, ZookeeperClient zookeeperClient) {
        this.monitorPath = monitorPath;
        this.isHavePrefix = isHavePrefix;
        this.zookeeperClient = zookeeperClient;
    }

    public boolean isHavePrefix() {
        return isHavePrefix;
    }

    public String getMonitorPath() {
        return monitorPath;
    }

    public ZookeeperClient getZookeeperClient() {
        return zookeeperClient;
    }

}
