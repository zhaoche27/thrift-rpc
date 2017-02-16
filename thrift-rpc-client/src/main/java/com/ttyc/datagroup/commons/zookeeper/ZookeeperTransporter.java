package com.ttyc.datagroup.commons.zookeeper;


/**
 * Created by zhaoche on 2017/2/4.
 */
public interface ZookeeperTransporter {
    ZookeeperClient connect(String zkConnectionString);
}
