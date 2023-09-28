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
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingInstanceEventType;
import org.folio.inventory.consortium.entities.SharingStatus;
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
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;
import static org.folio.inventory.consortium.entities.SharingStatus.COMPLETE;
import static org.folio.inventory.consortium.entities.SharingStatus.ERROR;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
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
      SharingInstance sharingInstanceMetadata = Json.decodeValue(event.value(), SharingInstance.class);

      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(event.headers());
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();

      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for instanceId: {}, sourceTenant: {}, targetTenant: {}",
        instanceId, sharingInstanceMetadata.getSourceTenantId(), sharingInstanceMetadata.getTargetTenantId());

      Context targetTenantContext = EventHandlingUtil.constructContext(sharingInstanceMetadata.getTargetTenantId(),
        kafkaHeaders.get(TOKEN.toLowerCase()), kafkaHeaders.get(URL.toLowerCase()));
      InstanceCollection targetInstanceCollection = storage.getInstanceCollection(targetTenantContext);

      Context sourceTenantContext = EventHandlingUtil.constructContext(sharingInstanceMetadata.getSourceTenantId(),
        kafkaHeaders.get(TOKEN.toLowerCase()), kafkaHeaders.get(URL.toLowerCase()));
      InstanceCollection sourceInstanceCollection = storage.getInstanceCollection(sourceTenantContext);

      return checkIsInstanceExistsOnTargetTenant(sharingInstanceMetadata, targetInstanceCollection,
        sourceInstanceCollection, kafkaHeaders);
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", event.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<String> checkIsInstanceExistsOnTargetTenant(SharingInstance sharingInstanceMetadata,
                                                             InstanceCollection targetInstanceCollection,
                                                             InstanceCollection sourceInstanceCollection,
                                                             Map<String, String> kafkaHeaders) {
    LOGGER.info("checkIsInstanceExistsOnTargetTenant :: InstanceId={} on tenant: {}",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());
    return getInstanceById(sharingInstanceMetadata.getInstanceIdentifier().toString(),
      sharingInstanceMetadata.getTargetTenantId(), targetInstanceCollection)
      .compose(instance -> {
        String warningMessage = format("Instance with InstanceId=%s is present on target tenant: %s",
          sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());
        sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, warningMessage, kafkaHeaders);
        return Future.succeededFuture(warningMessage);
      }, throwable -> publishInstance(sharingInstanceMetadata, sourceInstanceCollection,
        targetInstanceCollection, kafkaHeaders));
  }

  private Future<String> publishInstance(SharingInstance sharingInstanceMetadata, InstanceCollection sourceInstanceCollection,
                                         InstanceCollection targetInstanceCollection, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();

    return getInstanceById(instanceId, sourceTenant, sourceInstanceCollection)
      .compose(srcInstance -> {
        if ("FOLIO".equals(srcInstance.getSource())) {
          JsonObject jsonInstance = srcInstance.getJsonForStorage();
          jsonInstance.remove(HRID_KEY);
          return addInstance(srcInstance, targetTenant, targetInstanceCollection)
            .compose(addedInstance -> {
              JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
              jsonInstanceToPublish.put("source", CONSORTIUM_FOLIO.getValue());
              return updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                .map(ignored -> "Instance has been updated successfully")
                .onSuccess(ignored -> {
                  String message = format("Instance with InstanceId=%s has been shared to the target tenant %s",
                    instanceId, targetTenant);
                  sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
                }).onFailure(throwable -> {
                  String errorMessage = format("Error updating Instance with InstanceId=%s on source tenant %s.",
                    instanceId, sourceTenant);
                  sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                });
            }).onFailure(throwable -> {
              String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s.",
                instanceId, targetTenant);
              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
            });
        } else if ("MARC".equals(srcInstance.getSource())) {
          String errorMessage = format("Error sharing Instance with InstanceId=%s and source=MARC to the target tenant %s. " +
            "Not implemented yet.", instanceId, targetTenant);
          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
          return Future.failedFuture(errorMessage);
        } else {
          String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s. Because source is %s",
            instanceId, targetTenant, srcInstance.getSource());
          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
          return Future.failedFuture(errorMessage);
        }
      }, throwable -> {
        String errorMessage = format("Error retrieving Instance by InstanceId=%s from source tenant %s.", instanceId, sourceTenant);
        if (throwable != null && throwable.getCause() != null)
          errorMessage += " Error: " + throwable.getCause();
        sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
        return Future.failedFuture(errorMessage);
      });
  }

  private void sendErrorResponseAndPrintLogMessage(String errorMessage, SharingInstance sharingInstance, Map<String, String> kafkaHeaders) {
    LOGGER.error("handle:: {}", errorMessage);
    sendCompleteEventToKafka(sharingInstance, ERROR, errorMessage, kafkaHeaders);
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

  private Future<Instance> addInstance(Instance instance, String tenant, InstanceCollection instanceCollection) {
    LOGGER.info("addInstance :: Instance with InstanceId={} to tenant {}", instance.getId(), tenant);
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

  private void sendCompleteEventToKafka(SharingInstance sharingInstance, SharingStatus status, String errorMessage, Map<String, String> kafkaHeaders) {

    SharingInstanceEventType evenType = CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {

      String tenantId = kafkaHeaders.get(TENANT.toLowerCase());
      List<KafkaHeader> kafkaHeadersList = convertKafkaHeadersMap(kafkaHeaders);

      LOGGER.info("sendEventToKafka :: tenantId: {}, instance with InstanceId={}, status: {}, message: {}",
        tenantId, sharingInstance.getInstanceIdentifier(), status.getValue(), errorMessage);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createProducerRecord(topicName, sharingInstance, status, errorMessage, kafkaHeadersList);
      var kafkaProducer = createProducer(tenantId, topicName);
      kafkaProducer.send(kafkaRecord)
        .onSuccess(res -> LOGGER.info("Event with type {}, was sent to kafka", evenType.value()))
        .onFailure(err -> {
          var cause = err.getCause();
          LOGGER.info("Failed to sent event {}, cause: {}", evenType.value(), cause);
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

    return new KafkaProducerRecordBuilder<String, Object>(sharingInstance.getTargetTenantId())
      .key(sharingInstance.getInstanceIdentifier().toString())
      .value(sharingInstance)
      .topic(topicName)
      .build()
      .addHeaders(kafkaHeaders);
  }

  private KafkaProducer<String, String> createProducer(String tenantId, String topicName) {
    LOGGER.info("createProducer :: tenantId: {}, topicName: {}", tenantId, topicName);
    return new SimpleKafkaProducerManager(vertx, kafkaConfig).createShared(topicName);
  }

  private List<KafkaHeader> convertKafkaHeadersMap(Map<String, String> kafkaHeaders) {
    return new ArrayList<>(List.of(
      KafkaHeader.header(URL, kafkaHeaders.get(URL)),
      KafkaHeader.header(TENANT, kafkaHeaders.get(TENANT)),
      KafkaHeader.header(TOKEN, kafkaHeaders.get(TOKEN)))
    );
  }

}
