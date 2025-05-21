package org.fortishop.deliveryservice.exception.delivery;


import org.fortishop.deliveryservice.global.exception.BaseException;
import org.fortishop.deliveryservice.global.exception.BaseExceptionType;

public class DeliveryException extends BaseException {
    private final BaseExceptionType exceptionType;

    public DeliveryException(BaseExceptionType exceptionType) {
        this.exceptionType = exceptionType;
    }

    @Override
    public BaseExceptionType getExceptionType() {
        return exceptionType;
    }
}
