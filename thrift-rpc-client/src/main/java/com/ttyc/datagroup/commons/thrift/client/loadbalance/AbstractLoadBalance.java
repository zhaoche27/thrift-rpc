package com.ttyc.datagroup.commons.thrift.client.loadbalance;

import com.ttyc.datagroup.commons.Weight;
import com.ttyc.datagroup.commons.thrift.client.LoadBalance;

import java.util.List;

/**
 * Created by zhaoche on 2017/2/6.
 */
public abstract class AbstractLoadBalance implements LoadBalance {

    public Weight select(List<Weight> weights) {
        if (weights == null || weights.size() == 0)
            return null;
        if (weights.size() == 1)
            return weights.get(0);
        return doSelect(weights);
    }

    protected abstract Weight doSelect(List<Weight> weights);

    protected int getWeight(Weight weight) {
        return weight.getWeight();
    }

}
