package org.fortishop.deliveryservice.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.fortishop.deliveryservice.dto.request.AddressUpdateRequest;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.dto.request.TrackingUpdateRequest;
import org.fortishop.deliveryservice.dto.response.DeliveryResponse;
import org.fortishop.deliveryservice.exception.delivery.DeliveryException;
import org.fortishop.deliveryservice.kafka.DeliveryKafkaProducer;
import org.fortishop.deliveryservice.repository.DeliveryRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeliveryServiceImplTest {

    @InjectMocks
    private DeliveryServiceImpl deliveryService;

    @Mock
    private DeliveryRepository deliveryRepository;

    @Mock
    private DeliveryKafkaProducer kafkaProducer;

    private final Long orderId = 1L;

    @Test
    @DisplayName("배송 준비 등록 성공")
    void createDelivery_success() {
        // given
        DeliveryRequest request = new DeliveryRequest(orderId, "서울시 강남구");
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .address("서울시 강남구")
                .status(DeliveryStatus.READY)
                .build();

        when(deliveryRepository.save(any())).thenReturn(delivery);

        // when
        DeliveryResponse result = deliveryService.createDelivery(request);

        // then
        assertThat(result.getOrderId()).isEqualTo(orderId);
        assertThat(result.getStatus()).isEqualTo(DeliveryStatus.READY);
        verify(deliveryRepository).save(any());
    }

    @Test
    @DisplayName("존재하지 않는 주문 배송 조회 실패")
    void getByOrderId_notFound() {
        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliveryService.getByOrderId(orderId))
                .isInstanceOf(DeliveryException.class);
    }

    @Test
    @DisplayName("결제 실패 보상 트랜잭션 - 배송 상태 READY일 경우 취소")
    void compensateDelivery_ready_cancelled() {
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .status(DeliveryStatus.READY)
                .build();

        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.of(delivery));

        deliveryService.compensateDeliveryOnPaymentFailure(orderId);

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
    }

    @Test
    @DisplayName("배송 시작 시 상태 변경 및 Kafka 발행")
    void startDelivery_success() {
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .status(DeliveryStatus.READY)
                .build();

        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.of(delivery));

        deliveryService.startDelivery(orderId);

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.SHIPPED);
        verify(kafkaProducer).sendDeliveryStarted(orderId);
    }

    @Test
    @DisplayName("배송 완료 처리 시 상태 변경 및 Kafka 발행")
    void completeDelivery_success() {
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .status(DeliveryStatus.SHIPPED)
                .build();

        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.of(delivery));

        deliveryService.completeDelivery(orderId);

        assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);
        verify(kafkaProducer).sendDeliveryCompleted(orderId);
    }

    @Test
    @DisplayName("배송 주소 수정 성공")
    void updateAddress_success() {
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .status(DeliveryStatus.READY)
                .address("서울시 강남구")
                .build();

        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.of(delivery));

        AddressUpdateRequest request = new AddressUpdateRequest("서울시 서초구");
        deliveryService.updateAddress(orderId, request);

        assertThat(delivery.getAddress()).isEqualTo("서울시 서초구");
    }

    @Test
    @DisplayName("운송장 정보 수정 성공")
    void updateTracking_success() {
        Delivery delivery = Delivery.builder()
                .orderId(orderId)
                .status(DeliveryStatus.READY)
                .build();

        when(deliveryRepository.findByOrderId(orderId)).thenReturn(Optional.of(delivery));

        TrackingUpdateRequest request = new TrackingUpdateRequest("TRACK1234", "한진택배");
        deliveryService.updateTracking(orderId, request);

        assertThat(delivery.getTrackingNumber()).isEqualTo("TRACK1234");
        assertThat(delivery.getDeliveryCompany()).isEqualTo("한진택배");
    }

    @Test
    @DisplayName("배송 상태별 조회 성공")
    void getByStatus_success() {
        Delivery d1 = Delivery.builder().orderId(1L).status(DeliveryStatus.READY).build();
        Delivery d2 = Delivery.builder().orderId(2L).status(DeliveryStatus.READY).build();

        when(deliveryRepository.findAllByStatus(DeliveryStatus.READY))
                .thenReturn(List.of(d1, d2));

        List<DeliveryResponse> responses = deliveryService.getByStatus(DeliveryStatus.READY);

        assertThat(responses).hasSize(2);
        assertThat(responses.get(0).getStatus()).isEqualTo(DeliveryStatus.READY);
    }
}
