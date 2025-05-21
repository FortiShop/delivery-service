package org.fortishop.deliveryservice.global;

import java.time.LocalDateTime;
import lombok.Getter;

@Getter
public class ErrorResponse {
    private final String errorCode;
    private final String errorMessage;
    private final LocalDateTime timestamp;

    public ErrorResponse(String errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.timestamp = LocalDateTime.now();
    }
}
