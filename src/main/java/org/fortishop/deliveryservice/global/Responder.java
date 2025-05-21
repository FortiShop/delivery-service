package org.fortishop.deliveryservice.global;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class Responder {

    public static <T> ResponseEntity<T> success(T data) {
        return new ResponseEntity<>(data, HttpStatus.OK);
    }

    public static <T> ResponseEntity<T> success(HttpStatus status) {
        return new ResponseEntity<>(status);
    }

    public static <T> ResponseEntity<T> success(T data, HttpStatus status) {
        return new ResponseEntity<>(data, status);
    }

    public static ResponseEntity<ErrorResponse> error(String errorCode, String message, HttpStatus status) {
        ErrorResponse errorResponse = new ErrorResponse(errorCode, message);
        return new ResponseEntity<>(errorResponse, status);
    }

    public static void error(HttpServletResponse response, String errorCode, String message, HttpStatus status)
            throws IOException {
        response.setStatus(status.value());
        response.setContentType("application/json;charset=UTF-8");
        String body = String.format("{\"errorCode\":\"%s\", \"message\":\"%s\"}", errorCode, message);
        response.getWriter().write(body);
    }
}
