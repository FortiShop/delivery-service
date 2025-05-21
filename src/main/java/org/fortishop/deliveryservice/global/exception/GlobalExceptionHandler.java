package org.fortishop.deliveryservice.global.exception;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.fortishop.deliveryservice.global.ErrorResponse;
import org.fortishop.deliveryservice.global.Responder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

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

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidationEx(MethodArgumentNotValidException ex) {
        String errorMessage = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .map(err -> Objects.toString(err.getDefaultMessage(), "입력값 오류"))
                .findFirst()
                .orElse("입력값이 올바르지 않습니다.");

        log.warn("Validation Error: {}", errorMessage);
        return Responder.error("VALIDATION_ERROR", errorMessage, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatchEx(MethodArgumentTypeMismatchException ex) {
        String parameterName = ex.getName();
        String requiredType = ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "알 수 없음";
        String errorMessage = String.format("'%s' 파라미터는 '%s' 타입이어야 합니다.", parameterName, requiredType);

        log.warn("Type Mismatch Error: {}", errorMessage);
        return Responder.error("TYPE_MISMATCH", errorMessage, HttpStatus.BAD_REQUEST);
    }
}
