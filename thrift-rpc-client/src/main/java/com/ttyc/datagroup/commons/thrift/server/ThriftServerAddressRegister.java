package com.ttyc.datagroup.commons.thrift.server;

import com.ttyc.datagroup.commons.thrift.ThriftServiceServerWeight;

import java.io.Closeable;

/**
 * Created by zhaoche on 2017/2/6.
 */
public interface ThriftServerAddressRegister extends Closeable {

    /**
     * 发布服务接口
     *
     * @param service                   服务接口名称，一个产品中不能重复
     * @param thriftServiceServerWeight 服务发布的地址和端口及权重
     */
    void register(String service, ThriftServiceServerWeight thriftServiceServerWeight);
}
