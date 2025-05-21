package org.fortishop.deliveryservice.exception.delivery;

import org.fortishop.deliveryservice.global.exception.BaseExceptionType;
import org.springframework.http.HttpStatus;

public enum DeliveryExceptionType implements BaseExceptionType {
    DELIVERY_NOT_FOUND("D001", "해당 주문에 대한 배송 정보가 존재하지 않습니다.", HttpStatus.NOT_FOUND);;

    private final String errorCode;
    private final String errorMessage;
    private final HttpStatus httpStatus;

    DeliveryExceptionType(String errorCode, String errorMessage, HttpStatus httpStatus) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.httpStatus = httpStatus;
    }

    @Override
    public String getErrorCode() {
        return this.errorCode;
    }

    @Override
    public String getErrorMessage() {
        return this.errorMessage;
    }

    @Override
    public HttpStatus getHttpStatus() {
        return this.httpStatus;
    }
}
