package org.folio.inventory.instanceingress;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_REQUEST_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.CREATE_INSTANCE;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.UPDATE_INSTANCE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.instanceingress.handler.CreateInstanceIngressEventHandler;
import org.folio.inventory.instanceingress.handler.InstanceIngressEventHandler;
import org.folio.inventory.instanceingress.handler.UpdateInstanceIngressEventHandler;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;

@RequiredArgsConstructor
public class InstanceIngressEventConsumer implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressEventConsumer.class);
  private final Vertx vertx;
  private final Storage storage;
  private final HttpClient client;
  private final MappingMetadataCache mappingMetadataCache;

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(consumerRecord.headers());
    var event = Json.decodeValue(consumerRecord.value(), InstanceIngressEvent.class);
    var context = constructContext(getTenantId(event, kafkaHeaders),
      kafkaHeaders.get(OKAPI_TOKEN_HEADER), kafkaHeaders.get(OKAPI_URL_HEADER),
      kafkaHeaders.get(OKAPI_USER_ID), kafkaHeaders.get(OKAPI_REQUEST_ID));
    LOGGER.info("Instance ingress event has been received with event type: {}", event.getEventType());
    return Future.succeededFuture(event.getEventPayload())
      .compose(eventPayload -> processEvent(event, context)
        .map(ar -> consumerRecord.key()), th -> {
        LOGGER.error("Update record state was failed while handle event, {}", th.getMessage());
        return Future.failedFuture(th.getMessage());
      });
  }

  private static String getTenantId(InstanceIngressEvent event,
                                    Map<String, String> kafkaHeaders) {
    return Optional.ofNullable(event.getTenant())
      .orElseGet(() -> kafkaHeaders.get(OKAPI_TENANT_HEADER));
  }

  private Future<InstanceIngressEvent.EventType> processEvent(InstanceIngressEvent event, Context context) {
    try {
      Promise<InstanceIngressEvent.EventType> promise = Promise.promise();
      getInstanceIngressEventHandler(event.getEventType(), context).handle(event)
        .whenComplete((res, ex) -> {
          if (ex != null) {
            promise.fail(ex);
          } else {
            promise.complete(event.getEventType());
          }
        });
      return promise.future();
    } catch (Exception e) {
      LOGGER.warn("Error during processPayload: ", e);
      return Future.failedFuture(e);
    }
  }

  private InstanceIngressEventHandler getInstanceIngressEventHandler(InstanceIngressEvent.EventType eventType, Context context) {
    var precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
    if (eventType == CREATE_INSTANCE) {
      var idStorageService = new InstanceIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx)));
      return new CreateInstanceIngressEventHandler(precedingSucceedingTitlesHelper, mappingMetadataCache, idStorageService, client, context, storage);
    } else if (eventType == UPDATE_INSTANCE) {
      return new UpdateInstanceIngressEventHandler(precedingSucceedingTitlesHelper, mappingMetadataCache, client, context, storage);
    } else {
      LOGGER.warn("Can't process eventType {}", eventType);
      throw new EventProcessingException("Can't process eventType " + eventType);
    }
  }

}
