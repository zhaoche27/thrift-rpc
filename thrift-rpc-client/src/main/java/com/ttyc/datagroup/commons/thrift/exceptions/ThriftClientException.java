package com.ttyc.datagroup.commons.thrift.exceptions;

/**
 * Created by zhaoche on 2017/2/4.
 */
public class ThriftClientException extends RuntimeException {

    public ThriftClientException(String message) {
        super(message);
    }

    public ThriftClientException(Throwable e) {
        super(e);
    }

    public ThriftClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
