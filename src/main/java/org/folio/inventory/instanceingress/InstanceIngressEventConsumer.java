package org.folio.inventory.instanceingress;

import static java.util.Objects.isNull;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.CREATE_INSTANCE;
import static org.folio.rest.jaxrs.model.InstanceIngressEvent.EventType.UPDATE_INSTANCE;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.ext.web.client.WebClient;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.dao.EntityIdStorageDaoImpl;
import org.folio.inventory.common.dao.PostgresClientFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.handlers.actions.AbstractInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.instanceingress.handler.CreateInstanceIngressEventHandler;
import org.folio.inventory.services.InstanceIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.util.OkapiConnectionParams;

public class InstanceIngressEventConsumer implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(InstanceIngressEventConsumer.class);
  public static final String CACHE_KEY = InstanceIngressEventConsumer.class.getSimpleName();

  private final Vertx vertx;
  private final Storage storage;
  private final HttpClient client;
  private final MappingMetadataCache mappingMetadataCache;
  private final ProfileSnapshotCache profileSnapshotCache;
  private CreateInstanceIngressEventHandler createInstanceEventHandler;

  public InstanceIngressEventConsumer(Vertx vertx,
                                      Storage storage,
                                      HttpClient client,
                                      MappingMetadataCache mappingMetadataCache,
                                      ProfileSnapshotCache profileSnapshotCache) {
    this.vertx = vertx;
    this.storage = storage;
    this.client = client;
    this.mappingMetadataCache = mappingMetadataCache;
    this.profileSnapshotCache = profileSnapshotCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> consumerRecord) {
    var params = new OkapiConnectionParams(kafkaHeadersToMap(consumerRecord.headers()), vertx);
    var context = constructContext(params.getTenantId(), params.getToken(), params.getOkapiUrl());
    var event = Json.decodeValue(consumerRecord.value(), InstanceIngressEvent.class);
    LOGGER.info("Instance ingress event has been received with event type: {}", event.getEventType());
    return Future.succeededFuture(event.getEventPayload())
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
      var handler = getInstanceIngressEventHandler(eventType).handle(toDataImportPayload(eventPayload, eventType, context));
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

  private DataImportEventPayload toDataImportPayload(InstanceIngressPayload payload, InstanceIngressEvent.EventType eventType, Context context) {
    var diPayload = new DataImportEventPayload();
    diPayload.setEventType(eventType.value());
    diPayload.setTenant(context.getToken());
    diPayload.setToken(context.getTenantId());
    diPayload.setOkapiUrl(context.getOkapiLocation());
    diPayload.setJobExecutionId(CACHE_KEY);
    diPayload.setContext(new HashMap<>());
    diPayload.getContext().put(MARC_BIBLIOGRAPHIC.value(), payload.getSourceRecordObject());
    var profileSnapshotWrapper = profileSnapshotCache.get(CACHE_KEY, context).result().orElseThrow(() -> new EventProcessingException("No ProfileSnapshot available for " + CACHE_KEY));
    diPayload.setCurrentNode(profileSnapshotWrapper);
    return diPayload;
  }

  private AbstractInstanceEventHandler getInstanceIngressEventHandler(InstanceIngressEvent.EventType eventType) {
    if (eventType == CREATE_INSTANCE) {
      return getCreateInstanceEventHandler();
    } else if (eventType == UPDATE_INSTANCE) {
      return null; // to be implemented in MODINV-1008
    } else {
      LOGGER.warn("Can't process eventType {}", eventType);
      throw new EventProcessingException("Can't process eventType " + eventType);
    }
  }

  private CreateInstanceIngressEventHandler getCreateInstanceEventHandler() {
    if (isNull(createInstanceEventHandler)) {
      var precedingSucceedingTitlesHelper = new PrecedingSucceedingTitlesHelper(WebClient.wrap(client));
      createInstanceEventHandler = new CreateInstanceIngressEventHandler(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, new InstanceIdStorageService(new EntityIdStorageDaoImpl(new PostgresClientFactory(vertx))), client);
    }
    return createInstanceEventHandler;
  }

}
