package com.ttyc.datagroup.commons.thrift.client;

import com.ttyc.datagroup.commons.Weight;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/6.
 */
public interface LoadBalance {

    String LOAD_BALANCE_RANDOM = "random";
    String LOAD_BALANCE_RANDOM_ROBIN = "roundrobin";

    Weight select(List<Weight> weights);
}
