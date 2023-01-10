package org.folio.inventory.dataimport.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.util.OkapiConnectionParams;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static io.vertx.kafka.client.producer.KafkaProducer.createShared;
import static java.lang.String.format;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class OrderEventService {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();
  public static final String ORDER_POST_PROCESSING_PRODUCER_NAME = "DI_ORDER_READY_FOR_POST_PROCESSING_Producer";
  public static final String JOB_PROFILE_SNAPSHOT_ID = "JOB_PROFILE_SNAPSHOT_ID";
  public static final String DI_ORDER_READY_FOR_POST_PROCESSING = "DI_ORDER_READY_FOR_POST_PROCESSING";
  public static final String ORDER_TYPE = "ORDER";
  public static final String FOLIO_RECORD = "folioRecord";
  public static final String ACTION_FIELD = "action";
  public static final String CREATE_ACTION = "CREATE";
  public static final String DI_ERROR = "DI_ERROR";
  public static final String ERROR_KEY = "ERROR";
  private final int maxDistributionNumber = Integer.parseInt(System.getProperty("inventory.kafka.DataImportConsumerVerticle.maxDistributionNumber", "100"));
  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;
  private KafkaProducer<String, String> producer;
  private final ProfileSnapshotCache profileSnapshotCache;

  public OrderEventService(Vertx vertx, KafkaConfig kafkaConfig, ProfileSnapshotCache profileSnapshotCache) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    createProducer(kafkaConfig);
    this.profileSnapshotCache = profileSnapshotCache;
  }

  private void createProducer(KafkaConfig kafkaConfig) {
    this.producer = createShared(vertx, ORDER_POST_PROCESSING_PRODUCER_NAME, kafkaConfig.getProducerProps());
  }

  public Future<Void> executeOrderLogicIfNeeded(DataImportEventPayload eventPayload, Context context) {
    Promise<Void> promise = Promise.promise();
    String jobProfileSnapshotId = eventPayload.getContext().get(JOB_PROFILE_SNAPSHOT_ID);
    profileSnapshotCache.get(jobProfileSnapshotId, context)
      .toCompletionStage()
      .thenCompose(snapshotOptional -> snapshotOptional
        .map(profileSnapshot -> checkIfOrderLogicExistsAndSendPostProcessingEventIfNeeded(eventPayload, profileSnapshot))
        .orElse(CompletableFuture.failedFuture((new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", eventPayload.getContext().get("JOB_PROFILE_SNAPSHOT_ID")))))))
      .whenComplete((processed, throwable) -> {
        if (throwable != null) {
          promise.fail(throwable);
          LOGGER.error(throwable.getMessage());
        } else {
          promise.complete();
          LOGGER.debug(format("Job profile snapshot with id '%s' was retrieved from cache", jobProfileSnapshotId));
        }
      });
    return promise.future();
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

  private CompletableFuture<Void> checkIfOrderLogicExistsAndSendPostProcessingEventIfNeeded(DataImportEventPayload eventPayload, ProfileSnapshotWrapper profileSnapshotWrapper) {
    List<ProfileSnapshotWrapper> actionProfiles = profileSnapshotWrapper
      .getChildSnapshotWrappers()
      .stream()
      .filter(e -> e.getContentType() == ProfileSnapshotWrapper.ContentType.ACTION_PROFILE)
      .collect(Collectors.toList());

    if (!actionProfiles.isEmpty() && checkIfOrderActionProfileExists(actionProfiles) && checkIfCurrentProfileIsTheLastOne(eventPayload, actionProfiles)) {
      sendEvent(eventPayload, DI_ORDER_READY_FOR_POST_PROCESSING)
        .recover(throwable -> sendErrorEvent(eventPayload, throwable));
    }
    return CompletableFuture.completedFuture(null);
  }

  private static boolean checkIfCurrentProfileIsTheLastOne(DataImportEventPayload eventPayload, List<ProfileSnapshotWrapper> actionProfiles) {
    String currentMappingProfileId = eventPayload.getCurrentNode().getProfileId();
    ProfileSnapshotWrapper lastActionProfile = actionProfiles.get(actionProfiles.size() - 1);
    List<ProfileSnapshotWrapper> childSnapshotWrappers = lastActionProfile.getChildSnapshotWrappers();
    String mappingProfileId = StringUtils.EMPTY;
    if (childSnapshotWrappers != null && childSnapshotWrappers.get(0) != null && Objects.equals(childSnapshotWrappers.get(0).getContentType().value(), "MAPPING_PROFILE")) {
      mappingProfileId = childSnapshotWrappers.get(0).getProfileId();
    }
    return mappingProfileId.equals(currentMappingProfileId);
  }

  private static boolean checkIfOrderActionProfileExists(List<ProfileSnapshotWrapper> actionProfiles) {
    for (ProfileSnapshotWrapper actionProfile : actionProfiles) {
      LinkedHashMap<String, String> content = new ObjectMapper().convertValue(actionProfile.getContent(), LinkedHashMap.class);
      if (content.get(FOLIO_RECORD).equals(ORDER_TYPE) && content.get(ACTION_FIELD).equals(CREATE_ACTION)) {
        return true;
      }
    }
    return false;
  }

  private Future<Boolean> sendEventWithPayload(String eventPayload, String eventType,
                                               String key, KafkaProducer<String, String> producer,
                                               OkapiConnectionParams params) {
    KafkaProducerRecord<String, String> kafkaRecord = createRecord(eventPayload, eventType, key, params);
    Promise<Boolean> promise = Promise.promise();
    producer.write(kafkaRecord, war -> {
      if (war.succeeded()) {
        LOGGER.info("Event with type: {} was sent to kafka", eventType);
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("Write error for event {}:", eventType, cause);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

  private Future<Boolean> sendErrorEvent(DataImportEventPayload eventPayload, Throwable throwable) {
    eventPayload.getContext().put(ERROR_KEY, throwable.getMessage());
    return sendEvent(eventPayload, DI_ERROR);
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

    var kafkaRecord = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    kafkaRecord.addHeaders(kafkaHeadersFromMap(params.getHeaders()));
    return kafkaRecord;
  }
}
