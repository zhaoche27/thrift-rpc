package com.ttyc.datagroup.commons.thrift;

import com.ttyc.datagroup.commons.HostAndPort;

/**
 * Created by zhaoche on 2017/2/6.
 * 服务权重
 */
public class ThriftServiceServerWeight {

    private final HostAndPort hostAndPort;
    private volatile int weight;

    public ThriftServiceServerWeight(HostAndPort hostAndPort, int weight) {
        this.hostAndPort = hostAndPort;
        this.weight = weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ThriftServiceServerWeight that = (ThriftServiceServerWeight) o;

        return hostAndPort != null ? hostAndPort.equals(that.hostAndPort) : that.hostAndPort == null;

    }

    public static ThriftServiceServerWeight parse(String hostAndPortAndWeight) {
        String[] hostAndPortAndWeightArray = hostAndPortAndWeight.split(":");
        return new ThriftServiceServerWeight(new HostAndPort(hostAndPortAndWeightArray[0], Integer.valueOf(hostAndPortAndWeightArray[1])), Integer.valueOf(hostAndPortAndWeightArray[2]));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(hostAndPort.toString());
        sb.append(":").append(weight);
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return hostAndPort != null ? hostAndPort.hashCode() : 0;
    }

    public HostAndPort getHostAndPort() {
        return hostAndPort;
    }

    public int getWeight() {
        return weight;
    }
}
