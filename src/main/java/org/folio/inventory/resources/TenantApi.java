package org.folio.inventory.resources;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.impl.KafkaProducerRecordImpl;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.kafka.KafkaConfig;
import org.folio.inventory.kafka.KafkaProducerManager;
import org.folio.inventory.kafka.KafkaTopicServiceImpl;
import org.folio.inventory.kafka.PubSubConfig;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.util.pubsub.PubSubClientUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;

public class TenantApi {

  private static final Logger LOG = LoggerFactory.getLogger(TenantApi.class);
  private static final String TENANT_API_PATH = "/_/tenant";
  private static final String OKAPI_TENANT_HEADER = "x-okapi-tenant";

  public void register(Router router, ConfigRetriever config, Storage storage, HttpClient client) {
    router.post(TENANT_API_PATH).handler(ar -> postTenant(ar, config, storage, client));
  }

  private void postTenant(RoutingContext routingContext, ConfigRetriever config, Storage storage, HttpClient client) {
    MultiMap headersMap = routingContext.request().headers();
    String tenantId = TenantTool.calculateTenantId(headersMap.get(OKAPI_TENANT_HEADER));
    LOG.info("sending postTenant for " + tenantId);

    Map<String, String> okapiHeaders = getOkapiHeaders(routingContext);

    getConfig(config)
      .compose(ar -> new KafkaTopicServiceImpl(kafkaAdminClient(routingContext.vertx(), ar), ar).createTopics(Arrays.asList("DI_INVENTORY_INSTANCE_CREATED", "DI_SRS_MARC_BIB_RECORD_CREATED", "DI_COMPLETED", "DI_ERROR", "QM_INVENTORY_INSTANCE_UPDATED", "QM_ERROR"), tenantId))
      .compose(ar -> getConfig(config))
      .compose(ar -> subscribe("DI_SRS_MARC_BIB_RECORD_CREATED", createInstance(storage, client, tenantId, ar), tenantId, routingContext.vertx(), ar))
      .setHandler(ar ->
        routingContext.response()
          .setStatusCode(HttpResponseStatus.NO_CONTENT.code())
          .end());
  }

  private Map<String, String> getOkapiHeaders(RoutingContext rc) {
    Map<String, String> okapiHeaders = new HashMap<>();
    rc.request().headers().forEach(headerEntry -> {
      String headerKey = headerEntry.getKey().toLowerCase();
      if (headerKey.startsWith("x-okapi")) {
        okapiHeaders.put(headerKey, headerEntry.getValue());
      }
    });
    return okapiHeaders;
  }

  private Future<Void> registerModuleToPubsub(Map<String, String> headers, Vertx vertx) {
    Future<Void> future = Future.future();
    PubSubClientUtils.registerModule(new OkapiConnectionParams(headers, vertx))
      .whenComplete((registrationAr, throwable) -> {
        if (throwable == null) {
          LOG.info("Module was successfully registered as publisher/subscriber in mod-pubsub");
          future.complete();
        } else {
          LOG.error("Error during module registration in mod-pubsub", throwable);
          future.fail(throwable);
        }
      });
    return future;
  }

  public Future<Boolean> subscribe(String eventName, Handler<KafkaConsumerRecord<String, String>> handler, String tenantId, Vertx vertx, KafkaConfig kafkaConfig) {
    Future<Boolean> promise = Future.future();
    String topicName = new PubSubConfig(kafkaConfig.getEnvId(), tenantId, eventName).getTopicName();
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
    KafkaConsumer.<String, String>create(vertx, consumerProps)
      .subscribe(topicName, ar -> {
        if (ar.succeeded()) {
          LOG.info(format("Subscribed to topic {%s}", topicName));
          promise.complete(true);
        } else {
          LOG.error(format("Could not subscribe to some of the topic {%s}", topicName), ar.cause());
          promise.fail(ar.cause());
        }
      }).handler(handler);
    return promise;
  }

  private Future<KafkaConfig> getConfig(ConfigRetriever configRetriever) {
    Future<KafkaConfig> config = Future.future();
    configRetriever.getConfig(ar -> {
      config.complete(new KafkaConfig(ar.result()));
    });
    return config;
  }

  public KafkaAdminClient kafkaAdminClient(Vertx vertx, KafkaConfig config) {
    Map<String, String> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaUrl());
    return KafkaAdminClient.create(vertx, configs);
  }

  private Handler<KafkaConsumerRecord<String, String>> createInstance(Storage storage, HttpClient client, String tenantId, KafkaConfig kafkaConfig) {
    return record -> {
      try {
        Event event = new JsonObject(record.value()).mapTo(Event.class);
        DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(DataImportEventPayload.class);
        new CreateInstanceEventHandler(storage, client).handle(eventPayload)
          .thenApply(ar -> {
            sendEventWithPayload(JsonObject.mapFrom(ar).encode(), "DI_INVENTORY_INSTANCE_CREATED", tenantId, kafkaConfig)
              .compose(v -> sendEventWithPayload(JsonObject.mapFrom(ar).encode(), "DI_COMPLETED", tenantId, kafkaConfig))
              .result();
            return ar;
          }).exceptionally(exception -> {
          sendEventWithPayload(JsonObject.mapFrom(eventPayload).encode(), "DI_DI_ERROR", tenantId, kafkaConfig).result();
          return eventPayload;
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    };
  }

  private Future<Boolean> sendEventWithPayload(String eventPayload, String eventType, String tenantId, KafkaConfig kafkaConfig) {
    Future<Boolean> promise = Future.future();
    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      sendEvent(event, tenantId, kafkaConfig).setHandler(v -> promise.complete(true));
    } catch (Exception e) {
      LOG.error("Failed to send {} event to mod-pubsub", e, eventType);
      promise.fail(e);
    }
    return promise;
  }

  public Future<Boolean> sendEvent(Event event, String tenantId, KafkaConfig kafkaConfig) {
    Future<Boolean> promise = Future.future();
    PubSubConfig config = new PubSubConfig(kafkaConfig.getEnvId(), tenantId, event.getEventType());

    //TODO: it should be enough here to have a single or shared producers
    KafkaProducerManager.getKafkaProducer().write(new KafkaProducerRecordImpl<>(config.getTopicName(), Json.encode(event)), done -> {
      if (done.succeeded()) {
        LOG.info("Sent {} event with id '{}' to topic {}", event.getEventType(), event.getId(), config.getTopicName());
        promise.complete(true);
      } else {
        String errorMessage = "Event was not sent";
        LOG.error(errorMessage, done.cause());
        promise.fail(done.cause());
      }
    });

    return promise;
  }


}
