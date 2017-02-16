package org.apache.thrift;

import com.ttyc.datagroup.commons.thrift.monitor.ThriftMonitor;
import org.apache.thrift.protocol.*;
import org.apache.thrift.transport.TServiceException;

import java.util.Collections;
import java.util.Map;

public abstract class TBaseProcessor<I> implements TProcessor {
    private final I iface;
    private String serviceName;
    private final Map<String, ProcessFunction<I, ? extends TBase>> processMap;

    protected TBaseProcessor(I iface, Map<String, ProcessFunction<I, ? extends TBase>> processFunctionMap) {
        this.iface = iface;
        this.processMap = processFunctionMap;
        Class ifaceClass = iface.getClass();
        for (Class clazz : ifaceClass.getInterfaces()) {
            String clazzSimpleName = clazz.getSimpleName();
            if (!clazzSimpleName.equals("Iface")) {
                continue;
            }
            serviceName = clazz.getEnclosingClass().getName();
        }
        if (serviceName == null) {
            throw new TServiceException(ifaceClass.getSimpleName() + ",not acquire the service name");
        }
    }

    public Map<String, ProcessFunction<I, ? extends TBase>> getProcessMapView() {
        return Collections.unmodifiableMap(processMap);
    }

    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
        boolean isError = true;
        String methodName = null;
        long startMs = System.currentTimeMillis();
        try {
            TMessage msg = in.readMessageBegin();
            methodName = msg.name;
            ProcessFunction fn = processMap.get(msg.name);
            if (fn == null) {
                TProtocolUtil.skip(in, TType.STRUCT);
                in.readMessageEnd();
                TApplicationException x = new TApplicationException(TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
                out.writeMessageBegin(new TMessage(msg.name, TMessageType.EXCEPTION, msg.seqid));
                x.write(out);
                out.writeMessageEnd();
                out.getTransport().flush();
                return true;
            }
            fn.process(msg.seqid, in, out, iface);
            isError = false;
            return true;
        } catch (TException e) {
            throw e;
        } finally {
            if (methodName != null) {
                long useMs = System.currentTimeMillis() - startMs;
                if (isError) {
                    ThriftMonitor.recordOneCalledException(serviceName, methodName, 0);
                } else {
                    ThriftMonitor.recordOneCalled(serviceName, methodName, useMs);
                    ThriftMonitor.maxRecordMany(serviceName, methodName, useMs);
                }
            }
        }
    }
}
