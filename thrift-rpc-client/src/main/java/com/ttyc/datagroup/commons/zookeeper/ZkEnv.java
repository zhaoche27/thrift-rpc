package com.ttyc.datagroup.commons.zookeeper;

/**
 * Created by zhaoche on 2017/2/4.
 */
public enum ZkEnv {

    BETA("/beta"), PRE("/pre"), PROD("");

    private final String mark;

    public String getMark() {
        return mark;
    }

    private ZkEnv(String mark) {
        this.mark = mark;
    }
}
