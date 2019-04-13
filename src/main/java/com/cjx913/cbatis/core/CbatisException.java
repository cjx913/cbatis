package com.cjx913.cbatis.core;

public class CbatisException extends RuntimeException {
    public CbatisException() {
    }

    public CbatisException(String message) {
        super(message);
    }

    public CbatisException(String message, Throwable cause) {
        super(message, cause);
    }

    public CbatisException(Throwable cause) {
        super(cause);
    }

    public CbatisException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
