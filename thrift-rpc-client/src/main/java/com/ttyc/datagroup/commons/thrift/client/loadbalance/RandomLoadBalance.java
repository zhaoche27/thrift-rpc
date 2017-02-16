package com.ttyc.datagroup.commons.thrift.client.loadbalance;

import com.ttyc.datagroup.commons.Weight;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * random load balance.
 * Created by zhaoche on 2017/2/6.
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    @Override
    protected Weight doSelect(List<Weight> weights) {
        int length = weights.size(); // 总个数
        int totalWeight = 0; // 总权重
        boolean sameWeight = true; // 权重是否都一样
        for (int i = 0; i < length; i++) {
            int weight = getWeight(weights.get(i));
            totalWeight += weight; // 累计总权重
            if (sameWeight && i > 0 && weight != getWeight(weights.get(i - 1))) {
                sameWeight = false; // 计算所有权重是否一样
            }
        }
        if (totalWeight > 0 && !sameWeight) {
            // 如果权重不相同且权重大于0则按总权重数随机
            int offset = random.nextInt(totalWeight);
            // 并确定随机值落在哪个片断上
            for (int i = 0; i < length; i++) {
                offset -= getWeight(weights.get(i));
                if (offset < 0) {
                    return weights.get(i);
                }
            }
        }
        // 如果权重相同或权重为0则均等随机
        return weights.get(random.nextInt(length));
    }
}
