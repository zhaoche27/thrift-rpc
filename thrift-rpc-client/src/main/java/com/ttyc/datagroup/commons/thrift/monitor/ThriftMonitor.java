package com.ttyc.datagroup.commons.thrift.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoche on 2017/2/10.
 */
public class ThriftMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftMonitor.class);
    private static final String CALLED_SUFFIX = "_called";
    private static final String CALL_SUFFIX = "_call";
    private static final String CALL_TRANSFER_FAIL_SUFFIX = "_call_transfer_fail";
    private static final String CALL_EXCEPTION_FAIL_SUFFIX = "_call_exception_fail";
    private static final String CALLED_EXCEPTION_SUFFIX = CALLED_SUFFIX + "_exception";
    private static final String COUNT_SUFFIX = "_count";
    private static final String USE_TIME_MS_SUFFIX = "_use_time_ms";
    private static final String MAX_USE_MS_SUFFIX = "_max" + USE_TIME_MS_SUFFIX;

    private static final ConcurrentHashMap<MethodCallValue, CountAndTime> monitorIndexMap = new ConcurrentHashMap<>();

    private static final ConcurrentHashMap<String, MonitorZkInfo> monitorZkInfoMap = new ConcurrentHashMap<>();

    static {
        Timer timer = new Timer("ThriftMonitor", true);
        Calendar calendar = Calendar.getInstance();
        int seconds = calendar.get(Calendar.SECOND);
        int milliSeconds = calendar.get(Calendar.MILLISECOND);
        milliSeconds = (120 - seconds) * 1000 - milliSeconds;
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    Map<String, Set<MethodCallValue>> hashMap = aggregateTargets();
                    StringBuilder monitorResult;
                    MonitorZkInfo monitorZkInfo;
                    for (Map.Entry<String, Set<MethodCallValue>> entry : hashMap.entrySet()) {
                        monitorZkInfo = monitorZkInfoMap.get(entry.getKey());
                        if (monitorZkInfo != null) {
                            monitorResult = new StringBuilder();
                            for (MethodCallValue methodCallValue : entry.getValue()) {
                                if (monitorZkInfo.isHavePrefix()) {
                                    monitorResult.append(methodCallValue.getCaller()).append("=");
                                }
                                monitorResult.append(methodCallValue.getMethodName())
                                        .append("=").append(methodCallValue.getTargetName())
                                        .append("=").append(methodCallValue.getValue()).append("\n");
                            }
                            monitorZkInfo.getZookeeperClient().writeData(monitorZkInfo.getMonitorPath(), monitorResult.toString());
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Write service monitor zk path :{},data:\n{}", monitorZkInfo.getMonitorPath(), monitorResult);
                            }
                        } else {
                            LOGGER.error("Caller name:{}, not exist ThriftMonitor", entry.getKey());
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }, milliSeconds, 60000);
    }


    /**
     * 记录客户端调用次数
     *
     * @param projectMask 客户端
     * @param methodName  方法名
     */
    public static void recordOneClientCall(String projectMask, String methodName) {
        recordMany(projectMask, methodName, CALL_SUFFIX, 1, 0);
    }

    /**
     * 记录客户端调用数据传输失败次数
     *
     * @param projectMask 客户端
     * @param methodName  方法名
     */
    public static void recordOneClientCallTransferFail(String projectMask, String methodName) {
        recordMany(projectMask, methodName, CALL_TRANSFER_FAIL_SUFFIX, 1, 0);
    }


    /**
     * 记录客户端调用异常失败次数
     *
     * @param projectMask 客户端
     * @param methodName  方法名
     */
    public static void recordOneClientCallExceptionFail(String projectMask, String methodName) {
        recordMany(projectMask, methodName, CALL_EXCEPTION_FAIL_SUFFIX, 1, 0);
    }

    /**
     * 记录调用
     *
     * @param caller     服务名
     * @param methodName 方法名
     * @param userTime
     */
    public static void recordOneCalled(String caller, String methodName, long userTime) {
        recordMany(caller, methodName, CALLED_SUFFIX, 1, userTime);
    }

    /**
     * 记录调用出现异常
     *
     * @param caller     服务名
     * @param methodName 方法名
     * @param userTime
     */
    public static void recordOneCalledException(String caller, String methodName, long userTime) {
        recordMany(caller, methodName, CALLED_EXCEPTION_SUFFIX, 1, userTime);
    }

    /**
     * 该指标多次所用的时间
     *
     * @param caller     服务名
     * @param methodName 方法名
     * @param targetName 指标名
     * @param count      次数
     * @param useTime    所使用的时间
     */
    public static void recordMany(String caller, String methodName, String targetName, long count, long useTime) {
        MethodCallValue target = new MethodCallValue(caller, methodName, targetName);
        CountAndTime countAndTime = monitorIndexMap.get(target);
        if (countAndTime == null) {
            monitorIndexMap.putIfAbsent(target, new CountAndTime(count, useTime));
        } else {
            countAndTime.addCountAndTime(count, useTime);
        }
    }

    /**
     * 单位分钟内,只记录最大的一次
     *
     * @param caller     服务名
     * @param methodName 方法名
     * @param value
     */
    public static void maxRecordMany(String caller, String methodName, long value) {
        MethodCallValue target = new MethodCallValue(caller, methodName, CALLED_SUFFIX + MAX_USE_MS_SUFFIX);
        CountAndTime countAndTime = monitorIndexMap.get(target);
        if (countAndTime == null) {
            monitorIndexMap.putIfAbsent(target, new CountAndTime(value, 0));
        } else {
            countAndTime.maxCountAndTime(value);
        }
    }

    /**
     * 开始统计计算
     */
    private static HashMap<String, Set<MethodCallValue>> aggregateTargets() throws CloneNotSupportedException {
        Map<MethodCallValue, CountAndTime> hashMap = new HashMap<>();
        Iterator<MethodCallValue> iterator = monitorIndexMap.keySet().iterator();
        MethodCallValue key = null, cloneKey = null;
        while (iterator.hasNext()) {
            key = iterator.next();
            hashMap.put(key, monitorIndexMap.get(key));
            iterator.remove();
        }
        HashMap<String, Set<MethodCallValue>> targetMap = new HashMap<>();
        long value;
        String caller;
        Set<MethodCallValue> methodCallValues;
        for (Map.Entry<MethodCallValue, CountAndTime> entry : hashMap.entrySet()) {
            key = entry.getKey();
            caller = key.getCaller();
            methodCallValues = targetMap.get(caller);
            if (methodCallValues == null) {
                methodCallValues = new HashSet<>();
                targetMap.put(caller, methodCallValues);
            }
            value = entry.getValue().count;
            if (key.getTargetName().endsWith(MAX_USE_MS_SUFFIX)) {
                key.setValue(value);
                methodCallValues.add(key);
            } else if (value > 0) {
                String targetName = key.getTargetName();
                key.setTargetName(targetName + COUNT_SUFFIX);
                key.setValue(value);
                methodCallValues.add(key);
                long useTimes = entry.getValue().useTime;
                if (useTimes > 0) {
                    cloneKey = (MethodCallValue) key.clone();
                    cloneKey.setTargetName(targetName + USE_TIME_MS_SUFFIX);
                    cloneKey.setValue(useTimes);
                    methodCallValues.add(cloneKey);
                }
            }
        }
        return targetMap;
    }

    public static void addMonitorZkInfoMap(String name, MonitorZkInfo monitorZkInfo) {
        monitorZkInfoMap.put(name, monitorZkInfo);
    }


    private static class MethodCallValue implements Cloneable {

        private final String caller;
        private final String methodName;
        private String targetName;
        private long value;

        public MethodCallValue(String caller, String methodName, String targetName) {
            this.caller = caller;
            this.methodName = methodName;
            this.targetName = targetName;
        }

        public void setValue(long value) {
            this.value = value;
        }

        public void setTargetName(String targetName) {
            this.targetName = targetName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MethodCallValue that = (MethodCallValue) o;
            if (!caller.equals(that.caller)) return false;
            if (!methodName.equals(that.methodName)) return false;
            return targetName.equals(that.targetName);
        }

        @Override
        public int hashCode() {
            int result = caller.hashCode();
            result = 31 * result + methodName.hashCode();
            result = 31 * result + targetName.hashCode();
            return result;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            return super.clone();
        }

        public String getCaller() {
            return caller;
        }

        public String getMethodName() {
            return methodName;
        }

        public String getTargetName() {
            return targetName;
        }

        public long getValue() {
            return value;
        }

        @Override
        public String toString() {
            return caller + "." + methodName + "()" + targetName;
        }
    }

    public static void main(String[] args) {
        MethodCallValue methodCallValue = new MethodCallValue("service", "mehod1", "target1");
        try {
            recordOneCalled("service", "method1", 1000);
            recordOneCalled("service", "method1", 1000);
            recordOneCalled("service", "method1", 1000);

            recordOneCalled("service", "method2", 1000);
            recordOneCalled("service", "method2", 1000);

            Random random = new Random();
            maxRecordMany("service", "method2", random.nextInt(1000));
            maxRecordMany("service", "method2", random.nextInt(1000));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static class CountAndTime {
        private long count;
        private long useTime;

        public CountAndTime(long count, long useTime) {
            this.count = count;
            this.useTime = useTime;
        }

        public synchronized void addCountAndTime(long count, long useTime) {
            this.count += count;
            this.useTime += useTime;
        }

        public synchronized void maxCountAndTime(long value) {
            if (this.count < value) {
                this.count = value;
            }
        }

        @Override
        public String toString() {
            return "CountAndTime{" +
                    "count=" + count +
                    ", useTime=" + useTime +
                    '}';
        }
    }

}
