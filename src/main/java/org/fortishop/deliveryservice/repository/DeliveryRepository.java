package org.fortishop.deliveryservice.repository;

import java.util.List;
import java.util.Optional;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeliveryRepository extends JpaRepository<Delivery, Long> {

    Optional<Delivery> findByOrderId(Long orderId);

    List<Delivery> findAllByStatus(DeliveryStatus status);
}
