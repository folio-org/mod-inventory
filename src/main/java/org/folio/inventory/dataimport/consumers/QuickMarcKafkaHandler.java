package org.folio.inventory.dataimport.consumers;

import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_ERROR;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_HOLDINGS_UPDATED;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.QMEventTypes;
import org.folio.inventory.dataimport.handlers.actions.AuthorityUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.HoldingsUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.quickmarc.AbstractQuickMarcEventHandler;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateAuthorityQuickMarcEventHandler;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateHoldingsQuickMarcEventHandler;
import org.folio.inventory.dataimport.handlers.quickmarc.UpdateInstanceQuickMarcEventHandler;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.services.HoldingsCollectionService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.util.OkapiConnectionParams;

public class QuickMarcKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(QuickMarcKafkaHandler.class);

  private static final AtomicLong indexer = new AtomicLong();
  private static final String RECORD_TYPE_KEY = "RECORD_TYPE";
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.qm.ol.retry.number", "5"));

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final HoldingsUpdateDelegate holdingsUpdateDelegate;
  private final AuthorityUpdateDelegate authorityUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final int maxDistributionNumber;
  private final KafkaConfig kafkaConfig;
  private final Vertx vertx;

  public QuickMarcKafkaHandler(Vertx vertx, Storage storage, int maxDistributionNumber, KafkaConfig kafkaConfig,
                               PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper, HoldingsCollectionService holdingsCollectionService) {
    this.vertx = vertx;
    this.maxDistributionNumber = maxDistributionNumber;
    this.kafkaConfig = kafkaConfig;
    this.instanceUpdateDelegate = new InstanceUpdateDelegate(storage);
    this.holdingsUpdateDelegate = new HoldingsUpdateDelegate(storage, holdingsCollectionService);
    this.authorityUpdateDelegate = new AuthorityUpdateDelegate(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    var kafkaHeaders = kafkaHeadersToMap(record.headers());
    var params = new OkapiConnectionParams(kafkaHeaders, vertx);
    var context = constructContext(params.getTenantId(), params.getToken(), params.getOkapiUrl(),
      kafkaHeaders.get(OKAPI_USER_ID), kafkaHeaders.get(OKAPI_REQUEST_ID));
    Event event = Json.decodeValue(record.value(), Event.class);
    LOGGER.info("Quick marc event payload has been received with event type: {}", event.getEventType());
    return getEventPayload(event)
      .compose(eventPayload -> processPayload(eventPayload, context)
        .compose(recordType -> sendEvent(eventPayload, getReplyEventType(recordType), params))
        .recover(throwable -> sendErrorEvent(params, eventPayload, throwable))
        .map(ar -> record.key()), th -> {
        LOGGER.error("Update record state was failed while handle event, {}", th.getMessage());
        return Future.failedFuture(th.getMessage());
      });
  }

  private QMEventTypes getReplyEventType(Record.RecordType recordType) {
    if (recordType == Record.RecordType.MARC_BIB) {
      return QM_INVENTORY_INSTANCE_UPDATED;
    } else if (recordType == Record.RecordType.MARC_AUTHORITY) {
      return QM_INVENTORY_AUTHORITY_UPDATED;
    } else {
      return QM_INVENTORY_HOLDINGS_UPDATED;
    }
  }

  private Future<Boolean> sendErrorEvent(OkapiConnectionParams params, Map<String, String> eventPayload,
                                         Throwable throwable) {
    eventPayload.put("ERROR", throwable.getMessage());
    return sendEvent(eventPayload, QM_ERROR, params);
  }

  private Future<Record.RecordType> processPayload(Map<String, String> eventPayload, Context context) {
    try {
      var recordType = Record.RecordType.fromValue(eventPayload.get(RECORD_TYPE_KEY));
      return processWithRetry(eventPayload, context, recordType);
    } catch (Exception e) {
      LOGGER.warn("Error during processPayload: ", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Record.RecordType> processWithRetry(Map<String, String> eventPayload, Context context, Record.RecordType recordType) {
    int currentRetryNumber = eventPayload.containsKey(CURRENT_RETRY_NUMBER)
      ? Integer.parseInt(eventPayload.get(CURRENT_RETRY_NUMBER))
      : 0;

    return getQuickMarcEventHandler(context, recordType).handle(eventPayload)
      .map(recordType)
      .recover(throwable -> {
        if (throwable instanceof OptimisticLockingException && currentRetryNumber < MAX_RETRIES_COUNT) {
          LOGGER.warn("Optimistic locking error occurred. Retrying {}/{}...", currentRetryNumber + 1, MAX_RETRIES_COUNT);
          eventPayload.put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
          return processWithRetry(eventPayload, context, recordType);
        } else {
          LOGGER.error("Failed to process payload after {} retries. Error: {}", currentRetryNumber, throwable.getMessage());
          eventPayload.remove(CURRENT_RETRY_NUMBER);
          return Future.failedFuture(throwable);
        }
      });
  }

  private AbstractQuickMarcEventHandler<?> getQuickMarcEventHandler(Context context, Record.RecordType recordType) {
    if (Record.RecordType.MARC_BIB == recordType) {
      return new UpdateInstanceQuickMarcEventHandler(instanceUpdateDelegate, context, precedingSucceedingTitlesHelper);
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      return new UpdateHoldingsQuickMarcEventHandler(holdingsUpdateDelegate, context);
    } else if (Record.RecordType.MARC_AUTHORITY == recordType){
      return new UpdateAuthorityQuickMarcEventHandler(authorityUpdateDelegate, context);
    } else {
      LOGGER.warn("Can't process record type {}", recordType);
      throw new EventProcessingException("Can't process record type " + recordType);
    }
  }

  private KafkaProducer<String, String> createProducer(KafkaConfig kafkaConfig, QMEventTypes eventType) {
    return new SimpleKafkaProducerManager(vertx, kafkaConfig).createShared(eventType.name());
  }

  @SuppressWarnings("unchecked")
  private Future<Map<String, String>> getEventPayload(Event event) {
    try {
      var eventPayload = Json.decodeValue(event.getEventPayload(), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (Exception e) {
      LOGGER.warn("Error during decode {} : {}", event.getEventPayload(), e);
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> sendEvent(Object eventPayload, QMEventTypes eventType, OkapiConnectionParams params) {
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNumber);
    KafkaProducer<String, String> producer = createProducer(kafkaConfig, eventType);
    return sendEventWithPayload(Json.encode(eventPayload), eventType.name(), key, producer, params);
  }

  private Future<Boolean> sendEventWithPayload(String eventPayload, String eventType,
                                               String key, KafkaProducer<String, String> producer,
                                               OkapiConnectionParams params) {
    KafkaProducerRecord<String, String> producerRecord = createRecord(eventPayload, eventType, key, params);

    Promise<Boolean> promise = Promise.promise();
    producer.send(producerRecord)
      .<Void>mapEmpty()
      .eventually(v -> producer.flush())
      .eventually(v -> producer.close())
      .onSuccess(res -> {
        LOGGER.info("Event with type: {} was sent to kafka", eventType);
        promise.complete(true);
      })
      .onFailure(err -> {
        Throwable cause = err.getCause();
        LOGGER.error("Write error for event {}:", eventType, cause);
        promise.fail(cause);
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

    var producerRecord = new KafkaProducerRecordBuilder<String, Object>(event.getEventMetadata().getTenantId())
      .key(key)
      .value(event)
      .topic(topicName)
      .build();

    producerRecord.addHeaders(kafkaHeadersFromMap(params.getHeaders()));
    return producerRecord;
  }
}
