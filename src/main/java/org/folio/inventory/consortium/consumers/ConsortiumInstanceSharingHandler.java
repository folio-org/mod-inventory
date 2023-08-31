package org.folio.inventory.consortium.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import liquibase.pro.packaged.L;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.model.ConsortiumEvenType;
import org.folio.inventory.consortium.model.SharingInstance;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.LinkUpdateReport;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.EntityLinksKafkaTopic.LINKS_STATS;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);

  private static final String OKAPI_TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";

  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;
  private final Map<String, KafkaProducer<String, String>> producerList = new HashMap<>();

  public ConsortiumInstanceSharingHandler(Vertx vertx, Storage storage, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();

      String tenantId = record.key();
      LOGGER.info("handle :: CONSORTIUM_INSTANCE_SHARING_INIT from tenant {}", tenantId);
      SharingInstance sharingInstance = Json.decodeValue(record.value(), SharingInstance.class);

      String instanceId = sharingInstance.getInstanceIdentifier().toString();
      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for instanceId: {}, sourceTenant: {}, targetTenant: {}",
        instanceId, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());

      LOGGER.info("OKAPI_TOKEN_HEADER = {}", kafkaHeaders.get(OKAPI_TOKEN_HEADER));
      LOGGER.info("OKAPI_URL_HEADER = {}", kafkaHeaders.get(OKAPI_URL_HEADER));

      //make GET request by Instance UUID on target (consortium) tenant, if exists - (publish error event?), if not - proceed
      Context targetTenantContext = EventHandlingUtil.constructContext(sharingInstance.getTargetTenantId(),
        kafkaHeaders.get(OKAPI_TOKEN_HEADER), kafkaHeaders.get(OKAPI_URL_HEADER));
      LOGGER.info("handle :: targetTenantContext : tenantId : {}", targetTenantContext.getTenantId());

      InstanceCollection targetInstanceCollection = storage.getInstanceCollection(targetTenantContext);
      LOGGER.info("handle :: targetInstanceCollection : {}", targetInstanceCollection);

      Context sourceTenantContext = EventHandlingUtil.constructContext(sharingInstance.getSourceTenantId(),
        kafkaHeaders.get(OKAPI_TOKEN_HEADER), kafkaHeaders.get(OKAPI_URL_HEADER));
      LOGGER.info("handle :: sourceTenantContext : tenantId : {}", sharingInstance.getSourceTenantId());

      InstanceCollection sourceInstanceCollection = storage.getInstanceCollection(sourceTenantContext);
      LOGGER.info("handle :: sourceInstanceCollection : {}", sourceInstanceCollection);

      getInstanceById(instanceId, sharingInstance.getTargetTenantId(), targetInstanceCollection)
        .onFailure(failure -> {
          if (failure.getClass().equals(NotFoundException.class)) {
            LOGGER.info("handle :: instance {} not found on target tenant: {}",
              instanceId, sharingInstance.getTargetTenantId());
            getInstanceById(instanceId, sharingInstance.getSourceTenantId(), sourceInstanceCollection)
              .onSuccess(srcInstance -> {
                if (srcInstance == null) {
                  String errorMessage = format("handle :: instance %s not found on source tenant: %s",
                    instanceId, sharingInstance.getSourceTenantId());
                  LOGGER.error(errorMessage);
                  promise.fail(errorMessage);
                } else {
                  LOGGER.info("handle :: Publishing instance {} with source {} from {} tenant to {} tenant",
                    instanceId, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId(), srcInstance.getSource());
//            if ("FOLIO".equals(instanceToPublish.getSource())) {
                  JsonObject jsonInstanceToPublishOnTargetTenant = srcInstance.getJsonForStorage();
                  jsonInstanceToPublishOnTargetTenant.remove("hrid");
                  addInstance(Instance.fromJson(jsonInstanceToPublishOnTargetTenant), targetInstanceCollection).onSuccess(
                    publishedInstance -> {
                      LOGGER.info("handle :: Updating source to 'CONSORTIUM-FOLIO' for instance {}", instanceId);
                      JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
                      jsonInstanceToPublish.put("source", "CONSORTIUM-FOLIO");
                      updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                        .onSuccess(updatesSourceInstance -> {
                          LOGGER.info("handle :: source 'CONSORTIUM-FOLIO' updated to instance {}", instanceId);
                          sendEventToKafka(tenantId, instanceId, "Completed.", ConsortiumEvenType.CONSORTIUM_INSTANCE_SHARING_COMPLETE,
                            kafkaConfig, KafkaHeaderUtils.kafkaHeadersFromMap(kafkaHeaders));
                          promise.complete();
                        }).onFailure(error -> {
                          String errorMessage = format("Error update Instance by id %s on the source tenant %s. Error: %s",
                            instanceId, sharingInstance.getTargetTenantId(), error.getCause());
                          LOGGER.error(errorMessage);
                          promise.fail(error);
                        });
                    }
                  ).onFailure(e -> {
                    String errorMessage = format("Error save Instance by id %s on the target tenant %s. Error: %s",
                      instanceId, sharingInstance.getTargetTenantId(), e.getCause());
                    LOGGER.error(errorMessage);
                    promise.fail(e);
                  });
                  // publish CONSORTIUM_INSTANCE_SHARING_COMPLETE or DI_ERROR???
//              }
                }
              })
              .onFailure(err -> {
                String errorMessage = format("Error retrieving Instance by id %s from source tenant %s. Error: %s",
                  instanceId, sharingInstance.getSourceTenantId(), err);
                LOGGER.error(errorMessage);
                promise.fail(errorMessage);
              });
          }
        })
        .onSuccess(instanceOnTargetTenant -> {
          String errorMessage = format("handle :: instance %s is present on target tenant: %s",
            instanceId, sharingInstance.getTargetTenantId());
          LOGGER.error(errorMessage);
          promise.fail(errorMessage);
        });
      return promise.future();
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<Instance> getInstanceById(String instanceId, String tenantId, InstanceCollection instanceCollection) {
    LOGGER.trace("getInstanceById :: instanceId: {} on tenant: {}", instanceId, tenantId);
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.warn("getInstanceById :: Can't find Instance by id: {} on tenant: {}", instanceId, tenantId);
          promise.fail(new NotFoundException(format("Can't find Instance by id: %s on tenant: %s", instanceId, tenantId)));
        } else {
          LOGGER.trace("getInstanceById :: Instance with id {} is present on tenant: {}", instanceId, tenantId);
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("getInstanceById :: Error retrieving Instance by id %s on tenant %s - %s, status code %s",
          instanceId, tenantId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.add(instance, success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOGGER.info("Duplicated event received by InstanceId: {}. Ignoring...", instance.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Instance id: %s", instance.getId())));
        } else {
          LOGGER.error(format("Error posting Instance %s cause %s, status code %s", instance.getId(), failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private Future<Instance> updateInstanceInStorage(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(failure.getReason()));
        } else {
          LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void sendEventToKafka(String tenantId, String instanceId, String message,
                                ConsortiumEvenType consortiumEvenType,
                                KafkaConfig kafkaConfig, List<KafkaHeader> kafkaHeaders) {
    try {
      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, consortiumEvenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createKafkaMessage(instanceId, message, topicName, kafkaHeaders);
      getProducer(tenantId, topicName).write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Event with type {}, was sent to kafka", consortiumEvenType.value());
        } else {
          var cause = ar.cause();
          LOGGER.info("Failed to sent event {}, cause: {}", consortiumEvenType.value(), cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, cause {}", consortiumEvenType.value(), e);
    }
  }

  private KafkaProducerRecord<String, String> createKafkaMessage(String instanceId, String message,
                                                                 String topicName, List<KafkaHeader> kafkaHeaders) {
    LOGGER.debug("createKafkaMessage :: instanceId: {}, message: {}, topicName: {}", instanceId, message, topicName);
    KafkaProducerRecord<String, String> kafkaRecord = KafkaProducerRecord.create(topicName, instanceId, message);
    kafkaRecord.addHeaders(kafkaHeaders);
    return kafkaRecord;
  }

  private KafkaProducer<String, String> getProducer(String tenantId, String topicName) {
    LOGGER.debug("getProducer :: tenantId: {}", tenantId);
    if (producerList.get(tenantId) == null) {
      LOGGER.debug("getProducer :: trying to create producer for tenantId: {}", tenantId);
      producerList.put(tenantId, KafkaProducer.createShared(vertx, topicName, kafkaConfig.getProducerProps()));
    }
    return producerList.get(tenantId);
  }

}
