package com.ttyc.datagroup.commons.thrift.server;

import com.ttyc.datagroup.commons.zookeeper.ZookeeperClient;
import com.ttyc.datagroup.commons.zookeeper.zkclient.ZKClientZookeeperTransporter;
import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class ThriftServerAddressRegisterZookeeperTest extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
        BasicConfigurator.configure();
    }

    public void testRegister() throws Exception {
        ZookeeperClient zookeeperClient = new ZKClientZookeeperTransporter().connect("10.10.104.4:2181,10.10.103.230:2181,10.10.112.18:2181");/*
        ThriftServerAddressRegister thriftServerAddressRegister = new ThriftServerAddressRegisterZookeeper(zookeeperClient, ZkEnv.BETA);
        thriftServerAddressRegister.register("com.ttyc.test", "1.0", new ThriftServiceServerWeight(new HostAndPort(Nets.getLocalIp(), 9999), 10));*/
        String serverRootPath = "/beta/common/service/thrift-rpc/servers/com.ttyc.datagroups.demo.thrift.tutorial.Calculator/1.0/server";
        System.out.println("=============================");
        List<String> list = zookeeperClient.getChildren(serverRootPath);
        for (String s : list) {
            System.out.println(s + "\t" + zookeeperClient.getData(serverRootPath + "/" + s));
        }
    }

    public void testGetData() {
        ZookeeperClient zookeeperClient = new ZKClientZookeeperTransporter().connect("10.10.104.4:2181,10.10.103.230:2181,10.10.112.18:2181");
        System.out.println(zookeeperClient.getData("/beta/common/service/thrift-rpc/servers/demo.thrift.tutorial.Calculator/monitor/192.168.1.102:9988"));
        zookeeperClient.close();
    }
}