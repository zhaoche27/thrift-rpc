package com.ttyc.datagroup.commons.zookeeper.zkclient;

import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import com.ttyc.datagroup.commons.zookeeper.ZookeeperTransporter;

/**
 * Created by zhaoche on 2017/2/4.
 */
public class ZKClientZookeeperTransporter implements ZookeeperTransporter {
    public ZookeeperClient connect(String zkConnectionString) {
        return new ZKClientZookeeperClient(zkConnectionString);
    }
}
