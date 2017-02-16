package com.ttyc.datagroup.commons.util;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class Constants {
    public static final String THRIFT_SERVER_REGISTER_ZOOKEEPER_PATH_ROOT = "%s/common/service/thrift-rpc/servers";
    public static final int DEFAULT_SERVER_WEIGHT = 10;
    public static final int DEFAULT_SERVER_WEIGHT_STEP = 2;
    public static final int DEFAULT_SERVER_MIN_WEIGHT = 1;
    public static final int DEFAULT_CLIENT_CONNECTION_NUM = 10;
    public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 1000;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 5000;
    public static final int DEFAULT_RETRY_COUNT = 1;
    public static long MAX_WAIT_CLIENT_CONNECTION_MS = 1000;
    public static int MAX_CLIENT_CONNECTION_FAIL_COUNT = 1;
}
