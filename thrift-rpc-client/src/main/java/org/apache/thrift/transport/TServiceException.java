package org.apache.thrift.transport;

/**
 * Created by zhaoche on 2017/2/10.
 */
public class TServiceException extends RuntimeException {

    public TServiceException() {
    }

    public TServiceException(String message) {
        super(message);
    }

    public TServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    public TServiceException(Throwable cause) {
        super(cause);
    }

    public TServiceException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
