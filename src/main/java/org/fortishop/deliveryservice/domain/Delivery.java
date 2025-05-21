package org.fortishop.deliveryservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "deliveries")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class Delivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long orderId;

    @Enumerated(EnumType.STRING)
    private DeliveryStatus status;

    @Column(columnDefinition = "TEXT")
    private String address;

    private String trackingNumber;

    private String deliveryCompany;

    private LocalDateTime startedAt;

    private LocalDateTime completedAt;

    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
        this.updatedAt = this.createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public void updateTrackingInfo(String trackingNumber, String company) {
        this.trackingNumber = trackingNumber;
        this.deliveryCompany = company;
    }

    public void updateAddress(String address) {
        this.address = address;
    }

    public void startDelivery(LocalDateTime startedAt) {
        this.status = DeliveryStatus.SHIPPED;
        this.startedAt = startedAt;
    }

    public void completeDelivery(LocalDateTime completedAt) {
        this.status = DeliveryStatus.DELIVERED;
        this.completedAt = completedAt;
    }

    public void prepare() {
        this.status = DeliveryStatus.READY;
    }
}
