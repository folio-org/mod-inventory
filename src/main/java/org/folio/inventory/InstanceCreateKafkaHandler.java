package org.folio.inventory;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.util.Strings;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.kafka.AsyncRecordHandler;
import org.folio.inventory.kafka.KafkaConfig;
import org.folio.inventory.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class InstanceCreateKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceCreateKafkaHandler.class);
  private final AtomicInteger messageCounter = new AtomicInteger(1);
  private CreateInstanceEventHandler createInstanceEventHandler;
  private KafkaConfig kafkaConfig;
  private Vertx vertx;
  private final int maxDistributionNum;
  private final static String DI_COMPLETED = "DI_COMPLETED";
  private final static String DI_ERROR = "DI_ERROR";


  public InstanceCreateKafkaHandler(CreateInstanceEventHandler createInstanceEventHandler,
                                    KafkaConfig kafkaConfig,
                                    Vertx vertx, int maxDistributionNum) {
    super();
    this.createInstanceEventHandler = createInstanceEventHandler;
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    this.maxDistributionNum = maxDistributionNum;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Future<String> future = Future.future();
    Event event = new JsonObject(record.value()).mapTo(Event.class);
    try {
      DataImportEventPayload payload = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(DataImportEventPayload.class);
      List<KafkaHeader> kafkaHeaders = record.headers();
      OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);
      LOGGER.debug("Payload with marc record has been received, starting creating instances... ");
      createInstanceEventHandler.handle(payload)
        .whenComplete((result, throwable) -> {
          String eventType = (result != null && throwable == null) ? "DI_COMPLETED" : "DI_ERROR";

          String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), okapiConnectionParams.getTenantId(), eventType);
          KafkaProducer<String, String> kafkaProducer = KafkaProducer.createShared(vertx, eventType + "_Producer", kafkaConfig.getProducerProps());
          kafkaProducer.write(createKafkaProducerRecord(result, eventType, topicName, kafkaHeaders, okapiConnectionParams), ar -> {
            kafkaProducer.close();
            LOGGER.info("A message has been written into the " + topicName);
          });
//          if (result != null && throwable == null) {
////            String eventType = result.getEventType();
//            String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), okapiConnectionParams.getTenantId(), DI_COMPLETED);
//            KafkaProducer<String, String> kafkaProducer = KafkaProducer.createShared(vertx, DI_COMPLETED + "_Producer", kafkaConfig.getProducerProps());
//            kafkaProducer.write(createKafkaProducerRecord(result, DI_COMPLETED, topicName, kafkaHeaders, okapiConnectionParams), ar -> kafkaProducer.close());
//          } else {
//            String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), okapiConnectionParams.getTenantId(), DI_ERROR);
//            KafkaProducer<String, String> kafkaProducer = KafkaProducer.createShared(vertx, DI_ERROR + "_Producer", kafkaConfig.getProducerProps());
//            kafkaProducer.write(createKafkaProducerRecord(result, DI_ERROR, topicName, kafkaHeaders, okapiConnectionParams), ar -> kafkaProducer.close());
//          }
          future.complete();
        });
      return future;

    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error("Can't process the source record to create instance: ", e);
      return Future.failedFuture(e);
    }
  }

  private KafkaProducerRecord<String, String> createKafkaProducerRecord(DataImportEventPayload eventPayload, String eventType, String topicName, List<KafkaHeader> kafkaHeaders, OkapiConnectionParams okapiConnectionParams) {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType);
    try {
      event.withEventPayload(ZIPArchiver.zip(Json.encode(eventPayload)));
    } catch (Exception e) {
      LOGGER.error("Error during zipping payload", e);
    }
    event.withEventMetadata(new EventMetadata()
      .withTenantId(okapiConnectionParams.getTenantId())
      .withEventTTL(1)
      .withPublishedBy(PubSubClientUtils.constructModuleName()));

    String key = String.valueOf(messageCounter.incrementAndGet() % maxDistributionNum);

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);
    record.addHeader("id", eventPayload.getJobExecutionId());
    LOGGER.debug("Event payload for create instance was prepared: messageCounter " + messageCounter + " record: " + record);
    return record;
  }

  //TODO: utility method must be moved out from here
  private OkapiConnectionParams fromKafkaHeaders(List<KafkaHeader> headers) {
    Map<String, String> okapiHeaders = headers
      .stream()
      .collect(Collectors.groupingBy(KafkaHeader::key,
        Collectors.reducing(Strings.EMPTY,
          header -> {
            Buffer value = header.value();
            return Objects.isNull(value) ? "" : value.toString();
          },
          (a, b) -> Strings.isNotBlank(a) ? a : b)));

    return new OkapiConnectionParams(okapiHeaders, vertx);
  }
}
