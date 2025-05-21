package org.fortishop.deliveryservice;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.fortishop.deliveryservice.domain.Delivery;
import org.fortishop.deliveryservice.domain.DeliveryStatus;
import org.fortishop.deliveryservice.dto.request.AddressUpdateRequest;
import org.fortishop.deliveryservice.dto.request.DeliveryRequest;
import org.fortishop.deliveryservice.dto.request.TrackingUpdateRequest;
import org.fortishop.deliveryservice.dto.response.DeliveryResponse;
import org.fortishop.deliveryservice.repository.DeliveryRepository;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.config.location=classpath:/application-test.yml",
                "spring.profiles.active=test"
        }
)
@Testcontainers
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DeliveryServiceIntegrationTest {

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    DeliveryRepository deliveryRepository;

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0")
            .withDatabaseName("fortishop")
            .withUsername("test")
            .withPassword("test");

    @Container
    static GenericContainer<?> zookeeper = new GenericContainer<>(DockerImageName.parse("bitnami/zookeeper:3.8.1"))
            .withExposedPorts(2181)
            .withEnv("ALLOW_ANONYMOUS_LOGIN", "yes")
            .withNetwork(Network.SHARED)
            .withNetworkAliases("zookeeper");

    @Container
    static GenericContainer<?> kafka = new GenericContainer<>(DockerImageName.parse("bitnami/kafka:3.6.0"))
            .withExposedPorts(9092, 9093)
            .withNetwork(Network.SHARED)
            .withNetworkAliases("kafka")
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka").withHostConfig(
                    Objects.requireNonNull(cmd.getHostConfig())
                            .withPortBindings(
                                    new PortBinding(Ports.Binding.bindPort(9092), new ExposedPort(9092)),
                                    new PortBinding(Ports.Binding.bindPort(9093), new ExposedPort(9093))
                            )
            ))
            .withEnv("KAFKA_BROKER_ID", "1")
            .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
            .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .withEnv("KAFKA_CFG_LISTENERS", "PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093")
            .withEnv("KAFKA_CFG_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092,EXTERNAL://localhost:9093")
            .withEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .withEnv("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "true")
            .waitingFor(Wait.forLogMessage(".*\\[KafkaServer id=\\d+] started.*\\n", 1));

    @Container
    static GenericContainer<?> kafkaUi = new GenericContainer<>(DockerImageName.parse("provectuslabs/kafka-ui:latest"))
            .withExposedPorts(8080)
            .withEnv("KAFKA_CLUSTERS_0_NAME", "fortishop-cluster")
            .withEnv("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS", "PLAINTEXT://kafka:9092")
            .withEnv("KAFKA_CLUSTERS_0_ZOOKEEPER", "zookeeper:2181")
            .withNetwork(Network.SHARED)
            .withNetworkAliases("kafka-ui");

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        mysql.start();
        zookeeper.start();
        kafka.start();
        kafkaUi.start();

        registry.add("spring.datasource.url", mysql::getJdbcUrl);
        registry.add("spring.datasource.username", mysql::getUsername);
        registry.add("spring.datasource.password", mysql::getPassword);
        registry.add("spring.kafka.bootstrap-servers", () -> kafka.getHost() + ":" + kafka.getMappedPort(9093));
    }

    private static boolean topicCreated = false;

    @BeforeAll
    void initKafkaTopics() throws Exception {
        System.out.println("Kafka UI is available at: http://" + kafkaUi.getHost() + ":" + kafkaUi.getMappedPort(8080));
        String bootstrapServers = kafka.getHost() + ":" + kafka.getMappedPort(9093);
        List<String> topics = List.of(
                "order.created",
                "payment.completed",
                "payment.failed",
                "delivery.started",
                "delivery.completed"
        );
        if (!topicCreated) {
            for (String topic : topics) {
                createTopicIfNotExists(topic, bootstrapServers);
            }
            topicCreated = true;
        }
    }

    private static void createTopicIfNotExists(String topic, String bootstrapServers) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(config)) {
            Set<String> existingTopics = admin.listTopics().names().get(3, TimeUnit.SECONDS);
            if (!existingTopics.contains(topic)) {
                try {
                    admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                            .all().get(3, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                        System.out.println("Topic already exists: " + topic);
                    } else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to check or create topic: " + topic, e);
        }
    }

    @BeforeEach
    void cleanDb() {
        deliveryRepository.deleteAll();
    }

    private String getBaseUrl(String path) {
        return "http://localhost:" + port + path;
    }

    @Test
    @DisplayName("배송 생성 API 호출 시 배송이 READY 상태로 DB에 저장된다")
    void createDelivery_success() {
        // given
        DeliveryRequest request = new DeliveryRequest(1001L, "서울특별시 강남구");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DeliveryRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<DeliveryResponse> res = restTemplate.exchange(
                getBaseUrl("/api/delivery"),
                HttpMethod.POST,
                entity,
                DeliveryResponse.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        DeliveryResponse body = res.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getOrderId()).isEqualTo(1001L);
        assertThat(body.getAddress()).isEqualTo("서울특별시 강남구");
        assertThat(body.getStatus()).isEqualTo(DeliveryStatus.READY);
    }

    @Test
    @DisplayName("배송 생성 시 주소가 없으면 400 Bad Request가 발생한다")
    void createDelivery_missingAddress() {
        // given
        DeliveryRequest request = new DeliveryRequest(1001L, null);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DeliveryRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery"),
                HttpMethod.POST,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("배송 생성 시 orderId가 없으면 400 Bad Request가 발생한다")
    void createDelivery_missingOrderId() {
        // given
        DeliveryRequest request = new DeliveryRequest(null, "서울특별시 강남구");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<DeliveryRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery"),
                HttpMethod.POST,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("배송 단건 조회 API는 존재하는 배송 정보를 반환한다")
    void getDelivery_success() {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(2001L)
                .address("서울 강서구")
                .status(DeliveryStatus.READY)
                .build());

        // when
        ResponseEntity<DeliveryResponse> res = restTemplate.getForEntity(
                getBaseUrl("/api/delivery/" + saved.getOrderId()), DeliveryResponse.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        DeliveryResponse body = res.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getOrderId()).isEqualTo(2001L);
        assertThat(body.getStatus()).isEqualTo(DeliveryStatus.READY);
    }

    @Test
    @DisplayName("배송 단건 조회 시 존재하지 않으면 404 또는 예외 발생")
    void getDelivery_notFound() {
        // when
        ResponseEntity<String> res = restTemplate.getForEntity(
                getBaseUrl("/api/delivery/99999"), String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("배송 상태별 조회 API는 해당 상태의 배송 목록을 반환한다")
    void getDeliveriesByStatus_success() {
        // given
        deliveryRepository.saveAll(List.of(
                Delivery.builder().orderId(3001L).address("서울 송파구").status(DeliveryStatus.READY).build(),
                Delivery.builder().orderId(3002L).address("서울 서초구").status(DeliveryStatus.READY).build()
        ));

        // when
        ResponseEntity<List<DeliveryResponse>> res = restTemplate.exchange(
                getBaseUrl("/api/delivery?status=READY"),
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<>() {
                }
        );

        List<DeliveryResponse> deliveries = res.getBody();
        assertThat(deliveries).isNotNull();

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
    }

    @Test
    @DisplayName("배송 상태별 조회 시 잘못된 상태값이면 400 Bad Request가 발생한다")
    void getDeliveries_invalidStatus() {
        // when
        ResponseEntity<String> res = restTemplate.getForEntity(
                getBaseUrl("/api/delivery?status=INVALID_STATUS"), String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("배송 주소 수정 API는 주소를 정상적으로 변경한다")
    void updateAddress_success() {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(4001L)
                .address("서울 구로구")
                .status(DeliveryStatus.READY)
                .build());

        AddressUpdateRequest request = new AddressUpdateRequest("서울 중구");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<AddressUpdateRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<Void> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/" + saved.getOrderId() + "/address"),
                HttpMethod.PATCH,
                entity,
                Void.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        Delivery updated = deliveryRepository.findByOrderId(4001L).orElseThrow();
        assertThat(updated.getAddress()).isEqualTo("서울 중구");
    }

    @Test
    @DisplayName("배송 주소 수정 시 존재하지 않는 배송이면 예외가 발생한다")
    void updateAddress_notFound() {
        AddressUpdateRequest request = new AddressUpdateRequest("서울 강서구");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<AddressUpdateRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/99999/address"),
                HttpMethod.PATCH,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("배송 주소 수정 시 address가 누락되면 400 Bad Request가 발생한다")
    void updateAddress_missingField() {
        Map<String, Object> request = Map.of(); // 빈 request

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/4001/address"),
                HttpMethod.PATCH,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("운송장 정보 수정 API는 운송장 번호와 택배사를 정상 변경한다")
    void updateTracking_success() {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(4002L)
                .address("서울 용산구")
                .status(DeliveryStatus.READY)
                .build());

        TrackingUpdateRequest request = new TrackingUpdateRequest("TRACK123", "CJ대한통운");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<TrackingUpdateRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<Void> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/" + saved.getOrderId() + "/tracking"),
                HttpMethod.PATCH,
                entity,
                Void.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);
        Delivery updated = deliveryRepository.findByOrderId(4002L).orElseThrow();
        assertThat(updated.getTrackingNumber()).isEqualTo("TRACK123");
        assertThat(updated.getDeliveryCompany()).isEqualTo("CJ대한통운");
    }

    @Test
    @DisplayName("운송장 수정 시 필드가 누락되면 400 Bad Request가 발생한다")
    void updateTracking_missingFields() {
        Map<String, Object> request = Map.of("trackingNumber", "12345"); // deliveryCompany 없음

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/4002/tracking"),
                HttpMethod.PATCH,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("운송장 수정 시 배송 정보가 없으면 예외가 발생한다")
    void updateTracking_notFound() {
        TrackingUpdateRequest request = new TrackingUpdateRequest("TRACK999", "우체국");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<TrackingUpdateRequest> entity = new HttpEntity<>(request, headers);

        // when
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/99999/tracking"),
                HttpMethod.PATCH,
                entity,
                String.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("배송 시작 처리 API는 배송 상태를 SHIPPED로 바꾸고 Kafka 이벤트를 발행한다")
    void startDelivery_success() {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(6001L)
                .address("서울특별시 마포구")
                .status(DeliveryStatus.READY)
                .trackingNumber("TRACK6001")
                .deliveryCompany("CJ대한통운")
                .build());

        // when
        ResponseEntity<Void> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/" + saved.getOrderId() + "/start"),
                HttpMethod.PATCH,
                null,
                Void.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);

        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Delivery updated = deliveryRepository.findByOrderId(saved.getOrderId()).orElseThrow();
                    assertThat(updated.getStatus()).isEqualTo(DeliveryStatus.SHIPPED);
                    assertThat(updated.getStartedAt()).isNotNull();
                });

        // Kafka 메시지 자체 검증은 별도 consumer 테스트에서 수행 예정
    }

    @Test
    @DisplayName("배송 시작 처리 시 존재하지 않는 orderId이면 예외가 발생한다")
    void startDelivery_notFound() {
        ResponseEntity<String> res = restTemplate.exchange(
                getBaseUrl("/api/delivery/99999/start"),
                HttpMethod.PATCH,
                null,
                String.class
        );

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("배송 완료 처리 API는 상태를 DELIVERED로 바꾸고 Kafka 이벤트를 발행한다")
    void completeDelivery_success() {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(6002L)
                .address("서울특별시 관악구")
                .status(DeliveryStatus.SHIPPED)
                .trackingNumber("TRACK6002")
                .deliveryCompany("우체국택배")
                .build());

        // when
        ResponseEntity<Void> res = restTemplate.postForEntity(
                getBaseUrl("/api/delivery/" + saved.getOrderId() + "/complete"),
                null,
                Void.class
        );

        // then
        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.OK);

        await()
                .atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    Delivery updated = deliveryRepository.findByOrderId(saved.getOrderId()).orElseThrow();
                    assertThat(updated.getStatus()).isEqualTo(DeliveryStatus.DELIVERED);
                    assertThat(updated.getCompletedAt()).isNotNull();
                });
    }

    @Test
    @DisplayName("배송 완료 처리 시 배송 정보가 없으면 예외가 발생한다")
    void completeDelivery_notFound() {
        ResponseEntity<String> res = restTemplate.postForEntity(
                getBaseUrl("/api/delivery/99999/complete"),
                null,
                String.class
        );

        assertThat(res.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("Kafka order.created 수신 시 배송이 READY 상태로 생성된다")
    void kafka_orderCreated_createsDelivery() {
        // given
        Map<String, Object> payload = Map.of(
                "orderId", 7001L,
                "memberId", 1L,
                "totalPrice", 20000,
                "address", "서울 강남구",
                "items", List.of(Map.of("productId", 101L, "quantity", 2, "price", 10000)),
                "createdAt", LocalDateTime.now().toString(),
                "traceId", UUID.randomUUID().toString()
        );

        sendKafkaMessage("order.created", "7001", payload);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Delivery delivery = deliveryRepository.findByOrderId(7001L).orElseThrow();
            assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.READY);
            assertThat(delivery.getAddress()).isEqualTo("서울 강남구");
        });
    }

    @Test
    @DisplayName("Kafka payment.completed 수신 시 배송 상태가 SHIPPED로 변경된다")
    void kafka_paymentCompleted_startsDelivery() {
        // given
        deliveryRepository.save(Delivery.builder()
                .orderId(7002L)
                .status(DeliveryStatus.READY)
                .address("서울 서초구")
                .trackingNumber("T7002")
                .deliveryCompany("한진택배")
                .build());

        Map<String, Object> payload = Map.of(
                "orderId", 7002L,
                "paymentId", 999L,
                "paidAmount", 20000,
                "method", "CARD",
                "timestamp", LocalDateTime.now().toString(),
                "traceId", UUID.randomUUID().toString()
        );

        sendKafkaMessage("payment.completed", "7002", payload);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Delivery delivery = deliveryRepository.findByOrderId(7002L).orElseThrow();
            assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.SHIPPED);
            assertThat(delivery.getStartedAt()).isNotNull();
        });
    }

    @Test
    @DisplayName("Kafka payment.failed 수신 시 배송 상태가 CANCELLED로 변경된다 (보상 트랜잭션)")
    void kafka_paymentFailed_cancelsDelivery() {
        // given
        deliveryRepository.save(Delivery.builder()
                .orderId(7003L)
                .status(DeliveryStatus.READY)
                .address("서울 중구")
                .build());

        Map<String, Object> payload = Map.of(
                "orderId", 7003L,
                "reason", "카드 한도 초과",
                "timestamp", LocalDateTime.now().toString(),
                "traceId", UUID.randomUUID().toString()
        );

        sendKafkaMessage("payment.failed", "7003", payload);

        // then
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            Delivery delivery = deliveryRepository.findByOrderId(7003L).orElseThrow();
            assertThat(delivery.getStatus()).isEqualTo(DeliveryStatus.CANCELLED);
        });
    }

    @Test
    @DisplayName("배송 시작 시 delivery.started 메시지가 정확히 발행된다")
    void deliveryStarted_kafkaMessageContents() throws Exception {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(8001L)
                .address("서울 양천구")
                .status(DeliveryStatus.READY)
                .trackingNumber("TRK8001")
                .deliveryCompany("로젠택배")
                .build());

        ObjectMapper objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getHost() + ":" + kafka.getMappedPort(9093));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "delivery-service-test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("delivery.started"));
            consumer.poll(Duration.ofMillis(100));

            // when
            restTemplate.exchange(
                    getBaseUrl("/api/delivery/" + saved.getOrderId() + "/start"),
                    HttpMethod.PATCH,
                    HttpEntity.EMPTY,
                    Void.class
            );

            // then
            await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        boolean matched = false;

                        for (ConsumerRecord<String, String> record : records.records("delivery.started")) {
                            System.out.printf("📌 Kafka Record key=%s, value=%s%n", record.key(), record.value());

                            String rawJson = record.value();
                            JsonNode root = objectMapper.readTree(rawJson);
                            JsonNode json = root.isTextual() ? objectMapper.readTree(root.asText()) : root;

                            assertThat(json.get("orderId").asLong()).isEqualTo(saved.getOrderId());
                            assertThat(json.get("deliveryId").asLong()).isEqualTo(saved.getId());
                            assertThat(json.get("trackingNumber").asText()).isEqualTo("TRK8001");
                            assertThat(json.get("company").asText()).isEqualTo("로젠택배");
                            assertThat(json.get("startedAt").asText()).isNotBlank();
                            assertThat(json.get("traceId").asText()).isNotBlank();

                            matched = true;
                            break;
                        }
                        assertThat(matched).isTrue();
                    });
        }
    }

    @Test
    @DisplayName("배송 완료 시 delivery.completed 메시지가 정확히 발행된다")
    void deliveryCompleted_kafkaMessageContents() throws Exception {
        // given
        Delivery saved = deliveryRepository.save(Delivery.builder()
                .orderId(8002L)
                .address("서울 동작구")
                .status(DeliveryStatus.SHIPPED)
                .trackingNumber("TRK8002")
                .deliveryCompany("CJ대한통운")
                .build());

        ObjectMapper objectMapper = new ObjectMapper();

        // Kafka consumer 설정
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getHost() + ":" + kafka.getMappedPort(9093));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "delivery-service-test-completed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("delivery.completed"));
            consumer.poll(Duration.ofMillis(100));

            // when
            restTemplate.postForEntity(
                    getBaseUrl("/api/delivery/" + saved.getOrderId() + "/complete"),
                    null,
                    Void.class
            );

            // then
            await()
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(500))
                    .untilAsserted(() -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        boolean matched = false;

                        for (ConsumerRecord<String, String> record : records.records("delivery.completed")) {
                            System.out.printf("📌 Kafka Record key=%s, value=%s%n", record.key(), record.value());

                            String rawJson = record.value();
                            JsonNode root = objectMapper.readTree(rawJson);
                            JsonNode json = root.isTextual() ? objectMapper.readTree(root.asText()) : root;

                            assertThat(json.get("orderId").asLong()).isEqualTo(saved.getOrderId());
                            assertThat(json.get("deliveryId").asLong()).isEqualTo(saved.getId());
                            assertThat(json.get("completedAt").asText()).isNotBlank();
                            assertThat(json.get("traceId").asText()).isNotBlank();

                            matched = true;
                            break;
                        }
                        assertThat(matched).isTrue();
                    });
        }
    }

    private void sendKafkaMessage(String topic, String key, Object value) {
        KafkaProducer<String, Object> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getHost() + ":" + kafka.getMappedPort(9093),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.springframework.kafka.support.serializer.JsonSerializer.class
        ));
        producer.send(new ProducerRecord<>(topic, key, value));
        producer.flush();
        producer.close();
    }
}
