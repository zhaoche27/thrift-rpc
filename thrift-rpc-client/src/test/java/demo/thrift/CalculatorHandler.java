package demo.thrift;

import demo.thrift.shared.SharedStruct;
import demo.thrift.tutorial.Calculator;
import demo.thrift.tutorial.InvalidOperationException;
import demo.thrift.tutorial.Work;
import org.apache.thrift.TException;

/**
 * Created by zhaoche on 2017/2/3.
 */
public class CalculatorHandler implements Calculator.Iface {
    @Override
    public void ping() throws TException {
        System.out.println("ping()");
    }

    @Override
    public int add(int num1, int num2) throws TException {
        return num1 + num2;
    }

    @Override
    public int calculate(int logid, Work w) throws InvalidOperationException, TException {
        if (logid == 0) {
            throw new InvalidOperationException(500, "logid =" + logid);
        }
        return logid;
    }

    @Override
    public void zip() throws TException {

    }

    @Override
    public SharedStruct getStruct(int key) throws TException {
        SharedStruct sharedStruct = new SharedStruct();
        sharedStruct.setKey(key);
        sharedStruct.setValue("赵明强");
        return sharedStruct;
    }
}
