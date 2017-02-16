package com.ttyc.datagroup.commons.zookeeper;

/**
 * Created by zhaoche on 2017/2/4.
 */
public interface StateListener {

    int DISCONNECTED = 0;

    int CONNECTED = 1;

    int RECONNECTED = 2;

    void stateChanged(int connected);
}
