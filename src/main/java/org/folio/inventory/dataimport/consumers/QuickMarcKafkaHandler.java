package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.dataimport.handlers.QMEventTypes;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.UpdateInstanceEventHandler;
import org.folio.inventory.dataimport.util.ConsumerWrapperUtil;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.util.OkapiConnectionParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static io.vertx.kafka.client.producer.KafkaProducer.createShared;
import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_ERROR;
import static org.folio.inventory.dataimport.handlers.QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

public class QuickMarcKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(QuickMarcKafkaHandler.class);

  private static final AtomicLong indexer = new AtomicLong();

  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;
  private final int maxDistributionNumber;
  private final KafkaConfig kafkaConfig;
  private final KafkaInternalCache kafkaInternalCache;
  private final Vertx vertx;
  private final Map<QMEventTypes, KafkaProducer<String, String>> producerMap = new HashMap<>();

  public QuickMarcKafkaHandler(Vertx vertx, Storage storage, int maxDistributionNumber, KafkaConfig kafkaConfig,
                               KafkaInternalCache kafkaInternalCache, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    this.vertx = vertx;
    this.maxDistributionNumber = maxDistributionNumber;
    this.kafkaConfig = kafkaConfig;
    this.kafkaInternalCache = kafkaInternalCache;
    this.instanceUpdateDelegate = new InstanceUpdateDelegate(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
    createProducer(kafkaConfig, QM_INVENTORY_INSTANCE_UPDATED);
    createProducer(kafkaConfig, QM_ERROR);
  }

  private void createProducer(KafkaConfig kafkaConfig, QMEventTypes eventType) {
    var producerName = eventType.name() + "_Producer";
    KafkaProducer<String, String> producer = createShared(vertx, producerName, kafkaConfig.getProducerProps());
    producerMap.put(eventType, producer);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    var params = new OkapiConnectionParams(kafkaHeadersToMap(record.headers()), vertx);
    var context = constructContext(params.getTenantId(), params.getToken(), params.getOkapiUrl());
    Event event = Json.decodeValue(record.value(), Event.class);
    if (!kafkaInternalCache.containsByKey(event.getId())) {
      LOGGER.info(format("Quick marc event payload has been received with event type: %s", event.getEventType()));
      kafkaInternalCache.putToCache(event.getId());
      return getEventPayload(event)
        .compose(eventPayload -> new UpdateInstanceEventHandler(instanceUpdateDelegate, context, precedingSucceedingTitlesHelper).handle(eventPayload)
            .compose(ar -> sendEvent(eventPayload, QM_INVENTORY_INSTANCE_UPDATED, params))
            .recover(throwable -> {
              eventPayload.put("ERROR", throwable.getMessage());
              return sendEvent(eventPayload, QM_ERROR, params);
            })
            .map(ar -> record.key()), th -> Future.failedFuture(th.getMessage()
          )
        );
    }
    return Future.succeededFuture();
  }

  @SuppressWarnings("unchecked")
  private Future<HashMap<String, String>> getEventPayload(Event event) {
    try {
      var eventPayload = Json.decodeValue(ZIPArchiver.unzip(event.getEventPayload()), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Boolean> sendEvent(Object eventPayload, QMEventTypes eventType, OkapiConnectionParams params) {
    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNumber);
    var producer = producerMap.get(eventType);
    return sendEventWithPayload(Json.encode(eventPayload), eventType.name(), key, producer, params);
  }

  private Future<Boolean> sendEventWithPayload(String eventPayload, String eventType,
                                               String key, KafkaProducer<String, String> producer,
                                               OkapiConnectionParams params) {
    KafkaProducerRecord<String, String> record;
    try {
      record = createRecord(eventPayload, eventType, key, params);
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", eventType, e);
      return Future.failedFuture(e);
    }

    Promise<Boolean> promise = Promise.promise();
    producer.write(record, war -> {
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

  private KafkaProducerRecord<String, String> createRecord(String eventPayload, String eventType, String key,
                                                           OkapiConnectionParams params) throws IOException {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(ZIPArchiver.zip(eventPayload))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(ConsumerWrapperUtil.constructModuleName()));

    String topicName = formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), params.getTenantId(), eventType);

    var record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeadersFromMap(params.getHeaders()));
    return record;
  }

}
