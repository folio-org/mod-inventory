package org.folio.inventory.dataimport.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.handlers.QMEventTypes;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.util.OkapiConnectionParams;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.vertx.kafka.client.producer.KafkaProducer.createShared;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class OrderEventService {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();
  private final int maxDistributionNumber = Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumerVerticle.maxDistributionNumber", "100"));
  private Vertx vertx;
  private KafkaConfig kafkaConfig;
  private KafkaProducer<String, String> producer;

  public OrderEventService(Vertx vertx, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    createProducer(kafkaConfig, QM_INVENTORY_INSTANCE_UPDATED);
  }

  public void executeOrderLogicIfNeeded(DataImportEventPayload eventPayload) {
    if (ifSendingEventForOrderShouldBeApplied(eventPayload)) {
      sendEvent(eventPayload, "DI_ORDER_READY_FOR_POST_PROCESSING")
        .recover(throwable -> sendErrorEvent(eventPayload, throwable));
    }
  }

  private Future<Boolean> sendEvent(DataImportEventPayload eventPayload, String eventType) {
    OkapiConnectionParams params = prepareParams(eventPayload);
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNumber);
    return sendEventWithPayload(Json.encode(eventPayload), eventType, key, producer, params);
  }

  private OkapiConnectionParams prepareParams(DataImportEventPayload eventPayload) {
    OkapiConnectionParams params = new OkapiConnectionParams(vertx);
    params.setOkapiUrl(eventPayload.getOkapiUrl());
    params.setTenantId(eventPayload.getTenant());
    params.setToken(eventPayload.getToken());
    Map<String, String> kafkaHeaders = new HashMap<>();
    kafkaHeaders.put("x-okapi-url", eventPayload.getOkapiUrl());
    kafkaHeaders.put("x-okapi-tenant", eventPayload.getTenant());
    kafkaHeaders.put("x-okapi-token", eventPayload.getToken());
    kafkaHeaders.put("recordId", eventPayload.getContext().get("recordId"));
    kafkaHeaders.put("chunkId", eventPayload.getContext().get("chunkId"));
    params.setHeaders(kafkaHeaders);
    return params;
  }

  private boolean ifSendingEventForOrderShouldBeApplied(DataImportEventPayload dataImportEventPayload) {
    List<ProfileSnapshotWrapper> actionProfiles = dataImportEventPayload.getProfileSnapshot()
      .getChildSnapshotWrappers()
      .stream()
      .filter(e -> e.getContentType() == ProfileSnapshotWrapper.ContentType.ACTION_PROFILE)
      .collect(Collectors.toList());

    for (ProfileSnapshotWrapper actionProfile : actionProfiles) {
      LinkedHashMap<String, String> content = new ObjectMapper().convertValue(actionProfile.getContent(), LinkedHashMap.class);
      if (content.get("folioRecord").equals("ORDER") && content.get("action").equals("CREATE")) {
        String currentProfileId = dataImportEventPayload.getCurrentNode().getProfileId();
        ProfileSnapshotWrapper lastActionProfile = actionProfiles.get(actionProfiles.size() - 1);
        if (lastActionProfile.getProfileId().equals(currentProfileId)) {
          return true;
        }
      }
    }
    return false;
  }

  private Future<Boolean> sendErrorEvent(DataImportEventPayload eventPayload, Throwable throwable) {
    eventPayload.getContext().put("ERROR", throwable.getMessage());
    return sendEvent(eventPayload, "DI_ORDER_ERROR");
  }

  private Future<Boolean> sendEventWithPayload(String eventPayload, String eventType,
                                               String key, KafkaProducer<String, String> producer,
                                               OkapiConnectionParams params) {
    KafkaProducerRecord<String, String> record = createRecord(eventPayload, eventType, key, params);
    Promise<Boolean> promise = Promise.promise();
    producer.write(record, war -> {
      if (war.succeeded()) {
        LOGGER.info("Event with type: {} was sent to kafka with headers: {}!!! key: {}!!!! topic: {}!!!!! value:{}!!!!!! record:{}!!!!!!!", eventType, record.headers(), record.key(), record.topic(), record.value(), record.record());
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("Write error for event {}:", eventType, cause);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

  private KafkaProducerRecord<String, String> createRecord(String eventPayload, String eventType, String key,
                                                           OkapiConnectionParams params) {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(ConsumerWrapperUtil.constructModuleName()));

    String topicName = formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), params.getTenantId(), eventType);

    var record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeadersFromMap(params.getHeaders()));
    return record;
  }

  private void createProducer(KafkaConfig kafkaConfig, QMEventTypes eventType) {
    var producerName = eventType.name() + "_Producer";
    KafkaProducer<String, String> producer = createShared(vertx, producerName, kafkaConfig.getProducerProps());
    this.producer = producer;
  }
}
