package com.ttyc.datagroup.commons.thrift.client.loadbalance;

import com.ttyc.datagroup.commons.Weight;
import com.ttyc.datagroup.commons.util.AtomicPositiveInteger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by zhaoche on 2017/2/6.
 */
public class RoundRobinLoadBalance extends AbstractLoadBalance {

    private final ConcurrentMap<String, AtomicPositiveInteger> sequences = new ConcurrentHashMap<String, AtomicPositiveInteger>();

    private static final class IntegerWrapper {

        public IntegerWrapper(int value) {
            this.value = value;
        }

        private int value;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void decrement() {
            this.value--;
        }
    }

    @Override
    protected Weight doSelect(List<Weight> weights) {
        String key = weights.get(0).getKey();
        int length = weights.size(); // 总个数
        int maxWeight = 0; // 最大权重
        int minWeight = Integer.MAX_VALUE; // 最小权重
        final LinkedHashMap<Weight, IntegerWrapper> invokerToWeightMap = new LinkedHashMap<Weight, IntegerWrapper>();
        int weightSum = 0;
        for (int i = 0; i < length; i++) {
            int weight = getWeight(weights.get(i));
            maxWeight = Math.max(maxWeight, weight); // 累计最大权重
            minWeight = Math.min(minWeight, weight); // 累计最小权重
            if (weight > 0) {
                invokerToWeightMap.put(weights.get(i), new IntegerWrapper(weight));
                weightSum += weight;
            }
        }
        AtomicPositiveInteger sequence = sequences.get(key);
        if (sequence == null) {
            sequences.putIfAbsent(key, new AtomicPositiveInteger());
            sequence = sequences.get(key);
        }
        int currentSequence = sequence.getAndIncrement();
        if (maxWeight > 0 && minWeight < maxWeight) { // 权重不一样
            int mod = currentSequence % weightSum;
            for (int i = 0; i < maxWeight; i++) {
                for (Map.Entry<Weight, IntegerWrapper> each : invokerToWeightMap.entrySet()) {
                    final Weight k = each.getKey();
                    final IntegerWrapper v = each.getValue();
                    if (mod == 0 && v.getValue() > 0) {
                        return k;
                    }
                    if (v.getValue() > 0) {
                        v.decrement();
                        mod--;
                    }
                }
            }
        }
        // 取模轮循
        return weights.get(currentSequence % length);
    }
}
