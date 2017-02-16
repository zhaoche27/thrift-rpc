package com.ttyc.datagroup.commons.zookeeper;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/4.
 */
public interface ZookeeperClient {

    String getZkConnectionString();

    void create(String path, String data, boolean ephemeral);

    void writeData(String path, String data);

    void delete(String path);

    List<String> getChildren(String path);

    List<String> addChildListener(String path, ChildListener listener);

    void removeChildListener(String path, ChildListener listener);

    void addStateListener(StateListener listener);

    void removeStateListener(StateListener listener);

    boolean isConnected();

    void close();

    String getData(String path);
}
