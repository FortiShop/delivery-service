package org.fortishop.deliveryservice.global.exception;

import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.global.ErrorResponse;
import org.fortishop.deliveryservice.global.Responder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(BaseException.class)
    public ResponseEntity<ErrorResponse> handleBaseEx(BaseException exception) {
        String errorCode = exception.getExceptionType().getErrorCode();
        String errorMessage = exception.getExceptionType().getErrorMessage();
        log.error("BaseException errorCode() : {}", errorCode);
        log.error("BaseException errorMessage() : {}", errorMessage);
        return Responder.error(errorCode, errorMessage, exception.getExceptionType().getHttpStatus());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException e) {
        log.warn("IllegalArgumentException: {}", e.getMessage());
        return Responder.error("400", "잘못된 요청입니다.", HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleEx(Exception e) {
        log.error("Unhandled Exception: {}", e.getMessage(), e);
        return Responder.error("S001", e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
