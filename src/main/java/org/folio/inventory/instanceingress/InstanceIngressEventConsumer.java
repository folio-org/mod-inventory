package org.folio.inventory.instanceingress;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.CREATE_INSTANCE;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.UPDATE_INSTANCE;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.instanceingress.handler.InstanceIngressCreateEventHandler;
import org.folio.inventory.instanceingress.handler.InstanceIngressEventHandler;
import org.folio.inventory.instanceingress.handler.InstanceIngressUpdateEventHandler;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.util.OkapiConnectionParams;

public class InstanceIngressEventConsumer implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressEventConsumer.class);

  private final Vertx vertx;

  public InstanceIngressEventConsumer(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    var params = new OkapiConnectionParams(kafkaHeadersToMap(consumerRecord.headers()), vertx);
    var context = constructContext(params.getTenantId(), params.getToken(), params.getOkapiUrl());
    var event = Json.decodeValue(consumerRecord.value(), InstanceIngressEvent.class);
    LOGGER.info("Instance ingress event has been received with event type: {}", event.getEventType());
    return
    Future.succeededFuture(event.getEventPayload())
      .compose(eventPayload -> processPayload(eventPayload, event.getEventType(), context)
        .map(ar -> consumerRecord.key()), th -> {
        LOGGER.error("Update record state was failed while handle event, {}", th.getMessage());
        return Future.failedFuture(th.getMessage());
      });
  }

  private Future<InstanceIngressEvent.EventType> processPayload(InstanceIngressPayload eventPayload,
                                                   InstanceIngressEvent.EventType eventType,
                                                   Context context) {
    try {
      Promise<InstanceIngressEvent.EventType> promise = Promise.promise();
      var handler = getInstanceIngressEventHandler(eventType, context).handle(eventPayload);
      handler.whenComplete((res, ex) -> {
        if (ex != null) {
          promise.fail(ex);
        } else {
          promise.complete(eventType);
        }
      });

      return promise.future();
    } catch (Exception e) {
      LOGGER.warn("Error during processPayload: ", e);
      return Future.failedFuture(e);
    }
  }

  private InstanceIngressEventHandler getInstanceIngressEventHandler(InstanceIngressEvent.EventType eventType,
                                                                     Context context) {
    if (eventType == CREATE_INSTANCE) {
      return new InstanceIngressCreateEventHandler(context);
    } else if (eventType == UPDATE_INSTANCE) {
      return new InstanceIngressUpdateEventHandler(context);
    } else {
      LOGGER.warn("Can't process eventType {}", eventType);
      throw new EventProcessingException("Can't process eventType " + eventType);
    }
  }
}
