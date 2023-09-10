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
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.model.SharingInstance;
import org.folio.inventory.consortium.model.SharingInstanceEventType;
import org.folio.inventory.consortium.model.SharingStatus;
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
import org.folio.kafka.exception.DuplicateEventException;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.consortium.model.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;
import static org.folio.inventory.consortium.model.SharingStatus.COMPLETE;
import static org.folio.inventory.consortium.model.SharingStatus.ERROR;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingHandler.class);

  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;

  public ConsortiumInstanceSharingHandler(Vertx vertx, Storage storage, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> event) {
    try {
      Promise<String> promise = Promise.promise();
      String consortiumId = event.key();
      LOGGER.info("handle :: CONSORTIUM_INSTANCE_SHARING_INIT from consortiumId {}", consortiumId);
      SharingInstance sharingInstance = Json.decodeValue(event.value(), SharingInstance.class);

      String instanceId = sharingInstance.getInstanceIdentifier().toString();
      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(event.headers());

      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for instanceId: {}, sourceTenant: {}, targetTenant: {}",
        instanceId, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());
      String tenantId = kafkaHeaders.get(TENANT.toLowerCase());

      Context targetTenantContext = EventHandlingUtil.constructContext(sharingInstance.getTargetTenantId(),
        kafkaHeaders.get(TOKEN.toLowerCase()), kafkaHeaders.get(URL.toLowerCase()));
      InstanceCollection targetInstanceCollection = storage.getInstanceCollection(targetTenantContext);

      Context sourceTenantContext = EventHandlingUtil.constructContext(sharingInstance.getSourceTenantId(),
        kafkaHeaders.get(TOKEN.toLowerCase()), kafkaHeaders.get(URL.toLowerCase()));
      InstanceCollection sourceInstanceCollection = storage.getInstanceCollection(sourceTenantContext);

      LOGGER.info("handle :: checking is InstanceId={} exists on tenant {}", instanceId, sharingInstance.getTargetTenantId());
      getInstanceById(instanceId, sharingInstance.getTargetTenantId(), targetInstanceCollection)
        .onFailure(failure -> {
          if (failure.getClass().equals(NotFoundException.class)) {
            LOGGER.info("handle :: Instance with InstanceId={} not found on target tenant: {}", instanceId, sharingInstance.getTargetTenantId());
            getInstanceById(instanceId, sharingInstance.getSourceTenantId(), sourceInstanceCollection)
              .onSuccess(srcInstance -> {
                if (srcInstance == null) {
                  String errorMessage = format("Instance with InstanceId=%s not found on source tenant: %s", instanceId, sharingInstance.getSourceTenantId());
                  sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                  promise.fail(errorMessage);
                } else {
                  LOGGER.info("handle :: Publishing Instance with InstanceId={} and with source {} from {} tenant to {} tenant", instanceId, srcInstance.getSource(), sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());
                  if (FOLIO.getValue().equals(srcInstance.getSource())) {
                    publishInstanceToTenantSpecificCollection(srcInstance, targetInstanceCollection)
                      .onSuccess(publishedInstance -> {
                        LOGGER.info("handle :: Updating source to 'CONSORTIUM-FOLIO' for Instance with InstanceId={}", instanceId);
                        JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
                        jsonInstanceToPublish.put("source", CONSORTIUM_FOLIO.getValue());
                        updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                          .onSuccess(updatesSourceInstance -> {
                            LOGGER.info("handle :: Source '{}' updated for Instance with InstanceId={}", CONSORTIUM_FOLIO.getValue(), instanceId);
                            sendCompleteEventToKafka(tenantId, sharingInstance, COMPLETE, EMPTY, event.headers());
                            promise.complete();
                          }).onFailure(error -> {
                            String errorMessage = format("Error update Instance by InstanceId=%s on the source tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), error.getCause());
                            sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                            promise.fail(error);
                          });
                      })
                      .onFailure(publishFailure -> {
                        String errorMessage = format("Error save Instance by InstanceId=%s on the target tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), publishFailure.getCause());
                        sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                        promise.fail(publishFailure);
                      });
                  } else if (MARC.getValue().equals(srcInstance.getSource())) {
                    String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s. Because source is %s",
                      instanceId, sharingInstance.getTargetTenantId(), srcInstance.getSource());
                    sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                    promise.fail(errorMessage);
                  } else {
                    String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s. Because source is %s",
                      instanceId, sharingInstance.getTargetTenantId(), srcInstance.getSource());
                    sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                    promise.fail(errorMessage);
                  }
                }
              })
              .onFailure(err -> {
                String errorMessage = format("Error retrieving Instance by InstanceId=%s from source tenant %s. Error: %s", instanceId, sharingInstance.getSourceTenantId(), err);
                sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
                promise.fail(errorMessage);
              });
          } else {
            String errorMessage = format("Error checking Instance by InstanceId=%s on target tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), failure.getMessage());
            sendErrorResponseAndPrintLogMessage(tenantId, errorMessage, sharingInstance, event.headers());
            promise.fail(errorMessage);
          }
        })
        .onSuccess(instanceOnTargetTenant -> {
          String warningMessage = format("Instance with InstanceId=%s is present on target tenant: %s", instanceId, sharingInstance.getTargetTenantId());
          sendCompleteEventToKafka(tenantId, sharingInstance, COMPLETE, warningMessage, event.headers());
          promise.fail(warningMessage);
        });
      return promise.future();
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", event.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private void sendErrorResponseAndPrintLogMessage(String tenantId, String errorMessage, SharingInstance sharingInstance, List<KafkaHeader> kafkaHeaders) {
    LOGGER.error("handle:: {}", errorMessage);
    sendCompleteEventToKafka(tenantId, sharingInstance, ERROR, errorMessage, kafkaHeaders);
  }

  private Future<Instance> publishInstanceToTenantSpecificCollection(Instance instance, InstanceCollection instanceCollection) {
    JsonObject jsonInstance = instance.getJsonForStorage();
    jsonInstance.remove(HRID_KEY);
    return addInstance(Instance.fromJson(jsonInstance), instanceCollection);
  }

  private Future<Instance> getInstanceById(String instanceId, String tenantId, InstanceCollection instanceCollection) {
    LOGGER.info("getInstanceById :: InstanceId={} on tenant: {}", instanceId, tenantId);
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.warn("getInstanceById :: Can't find Instance by InstanceId={} on tenant: {}", instanceId, tenantId);
          promise.fail(new NotFoundException(format("Can't find Instance by InstanceId=%s on tenant: %s", instanceId, tenantId)));
        } else {
          LOGGER.debug("getInstanceById :: Instance with InstanceId={} is present on tenant: {}", instanceId, tenantId);
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("getInstanceById :: Error retrieving Instance by InstanceId=%s on tenant %s - %s, status code %s",
          instanceId, tenantId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    LOGGER.info("addInstance :: InstanceId={}", instance.getId());
    Promise<Instance> promise = Promise.promise();
    instanceCollection.add(instance, success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(UNIQUE_ID_ERROR_MESSAGE)) {
          LOGGER.info("addInstance :: Duplicated event received by InstanceId={}. Ignoring...", instance.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by InstanceId=%s", instance.getId())));
        } else {
          LOGGER.error(format("addInstance :: Error posting Instance with InstanceId=%s cause %s, status code %s", instance.getId(), failure.getReason(), failure.getStatusCode()));
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
          LOGGER.error(format("Error updating Instance with InstanceId=%s, status code %s", failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void sendCompleteEventToKafka(String tenantId, SharingInstance sharingInstance,
                                        SharingStatus status, String errorMessage, List<KafkaHeader> kafkaHeaders) {

    SharingInstanceEventType evenType = CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {
      LOGGER.info("sendEventToKafka :: tenantId: {}, instance with InstanceId={}, status: {}, message: {}",
        tenantId, sharingInstance.getInstanceIdentifier(), status.getValue(), errorMessage);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createProducerRecord(topicName, sharingInstance, status, errorMessage, kafkaHeaders);
      createProducer(tenantId, topicName).write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Event with type {}, was sent to kafka", evenType.value());
        } else {
          var cause = ar.cause();
          LOGGER.info("Failed to sent event {}, cause: {}", evenType.value(), cause);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to send an event for eventType {}, cause {}", evenType.value(), e);
    }
  }

  private KafkaProducerRecord<String, String> createProducerRecord(String topicName, SharingInstance sharingInstance,
                                                                   SharingStatus status, String errorMessage, List<KafkaHeader> kafkaHeaders) {
    LOGGER.info("createKafkaMessage :: Instance with InstanceId={}, status: {}, {}topicName: {}",
      sharingInstance.getInstanceIdentifier(), status,
      status.equals(ERROR) ? " message: " + errorMessage + ", " : EMPTY, topicName);

    sharingInstance.setStatus(status);
    if (sharingInstance.getStatus().equals(ERROR)) {
      sharingInstance.setError(errorMessage);
    } else {
      sharingInstance.setError(EMPTY);
    }

    String eventPayload = Json.encode(sharingInstance);
    return KafkaProducerRecord.create(topicName, sharingInstance.getInstanceIdentifier().toString(), eventPayload).addHeaders(kafkaHeaders);
  }

  private KafkaProducer<String, String> createProducer(String tenantId, String topicName) {
    LOGGER.info("createProducer :: tenantId: {}, topicName: {}", tenantId, topicName);
    return KafkaProducer.createShared(vertx, topicName + "_Producer", kafkaConfig.getProducerProps());
  }

}
