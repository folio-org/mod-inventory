package org.folio.inventory.consortium.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.model.ConsortiumEnumStatus;
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
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.services.KafkaProducerRecordBuilder;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);

  private static final String OKAPI_TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";

  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;
  private final Map<String, KafkaProducer<String, String>> producerList = new HashMap<>();

  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperTool.getMapper();

  public ConsortiumInstanceSharingHandler(Vertx vertx, Storage storage, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();

      String consortiumId = record.key();
      LOGGER.info("handle :: CONSORTIUM_INSTANCE_SHARING_INIT from consortiumId {}", consortiumId);
      SharingInstance sharingInstance = Json.decodeValue(record.value(), SharingInstance.class);

      String instanceId = sharingInstance.getInstanceIdentifier().toString();
      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for instanceId: {}, sourceTenant: {}, targetTenant: {}",
        instanceId, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());

      LOGGER.info("OKAPI_TOKEN_HEADER = {}", kafkaHeaders.get(OKAPI_TOKEN_HEADER));
      LOGGER.info("OKAPI_URL_HEADER = {}", kafkaHeaders.get(OKAPI_URL_HEADER));
      LOGGER.info("OKAPI_TENANT = {}", kafkaHeaders.get("x-okapi-tenant"));
      String tenantId = kafkaHeaders.get("x-okapi-tenant");

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

      LOGGER.info("handle :: checking is instance {} exists on tenant {}", instanceId, sharingInstance.getTargetTenantId());
      getInstanceById(instanceId, sharingInstance.getTargetTenantId(), targetInstanceCollection)
        .onFailure(failure -> {
          if (failure.getClass().equals(NotFoundException.class)) {
            LOGGER.info("handle :: instance {} not found on target tenant: {}", instanceId, sharingInstance.getTargetTenantId());
            getInstanceById(instanceId, sharingInstance.getSourceTenantId(), sourceInstanceCollection)
              .onSuccess(srcInstance -> {
                if (srcInstance == null) {
                  String errorMessage = format("handle :: instance %s not found on source tenant: %s", instanceId, sharingInstance.getSourceTenantId());
                  LOGGER.error(errorMessage);
                  sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
                  promise.fail(errorMessage);
                } else {
                  LOGGER.info("handle :: Publishing instance {} with source {} from {} tenant to {} tenant", instanceId, srcInstance.getSource(), sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());
//            if ("FOLIO".equals(instanceToPublish.getSource())) {
                  publishInstanceToTenant(srcInstance, targetInstanceCollection)
                    .onSuccess(publishedInstance -> {
                      LOGGER.info("handle :: Updating source to 'CONSORTIUM-FOLIO' for instance {}", instanceId);
                      JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
                      jsonInstanceToPublish.put("source", "CONSORTIUM-FOLIO");
                      updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                        .onSuccess(updatesSourceInstance -> {
                          LOGGER.info("handle :: source 'CONSORTIUM-FOLIO' updated to instance {}", instanceId);
                          sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.COMPLETE, kafkaConfig, kafkaHeaders);
                          promise.complete();
                        }).onFailure(error -> {
                          String errorMessage = format("Error update Instance by id %s on the source tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), error.getCause());
                          LOGGER.error(errorMessage);
                          sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
                          promise.fail(error);
                        });
                    })
                    .onFailure(publishFailure -> {
                      String errorMessage = format("Error save Instance by id %s on the target tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), publishFailure.getCause());
                      LOGGER.error(errorMessage);
                      sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
                      promise.fail(publishFailure);
                    });
//              }
                }
              })
              .onFailure(err -> {
                String errorMessage = format("Error retrieving Instance by id %s from source tenant %s. Error: %s", instanceId, sharingInstance.getSourceTenantId(), err);
                LOGGER.error(errorMessage);
                sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
                promise.fail(errorMessage);
              });
          }
          String errorMessage = format("Error checking Instance by id %s on target tenant %s. Error: %s", instanceId, sharingInstance.getTargetTenantId(), failure.getMessage());
          LOGGER.error(errorMessage);
          sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
          promise.fail(errorMessage);
        })
        .onSuccess(instanceOnTargetTenant -> {
          String errorMessage = format("handle :: instance %s is present on target tenant: %s", instanceId, sharingInstance.getTargetTenantId());
          LOGGER.error(errorMessage);
          sendEventToKafka(tenantId, sharingInstance, ConsortiumEnumStatus.ERROR, errorMessage, kafkaConfig, kafkaHeaders);
          promise.fail(errorMessage);
        });
      return promise.future();
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<Instance> publishInstanceToTenant(Instance instance, InstanceCollection instanceCollection) {
    JsonObject jsonInstance = instance.getJsonForStorage();
    jsonInstance.remove("hrid");
    return addInstance(Instance.fromJson(jsonInstance), instanceCollection);
  }

  private Future<Instance> getInstanceById(String instanceId, String tenantId, InstanceCollection instanceCollection) {
    LOGGER.info("getInstanceById :: instanceId: {} on tenant: {}", instanceId, tenantId);
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

  private void sendEventToKafka(String tenantId, SharingInstance sharingInstance, ConsortiumEnumStatus status,
                                KafkaConfig kafkaConfig, Map<String, String> kafkaHeaders) {
    sendEventToKafka(tenantId, sharingInstance, status, null, kafkaConfig, kafkaHeaders);
  }

  private void sendEventToKafka(String tenantId, SharingInstance sharingInstance, ConsortiumEnumStatus status, String message,
                                KafkaConfig kafkaConfig, Map<String, String> kafkaHeaders) {

    ConsortiumEvenType evenType = ConsortiumEvenType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {
      LOGGER.info("sendEventToKafka :: tenantId: {}, instanceId: {}, message: {}, consortiumEvenType: {}",
        tenantId, sharingInstance.getInstanceIdentifier(), message, evenType.value());

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      LOGGER.info("sendEventToKafka :: topicName: {}", topicName);

      KafkaProducerRecord<String, String> kafkaRecord = createKafkaMessage(tenantId, sharingInstance, status, message, topicName, kafkaHeaders);
      getProducer(tenantId, topicName).write(kafkaRecord, ar -> {
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

  private KafkaProducerRecord<String, String> createKafkaMessage(String tenantId, SharingInstance sharingInstance, ConsortiumEnumStatus status,
                                                                 String errorMessage, String topicName, Map<String, String> kafkaHeaders) throws JsonProcessingException {
    LOGGER.info("createKafkaMessage :: instanceId: {}, status: {}, {}topicName: {}",
      sharingInstance.getInstanceIdentifier(), status, status.equals(ConsortiumEnumStatus.ERROR) ? errorMessage + ", " : EMPTY, topicName);

    KafkaProducerRecordBuilder<String, String> builder = new KafkaProducerRecordBuilder<>(tenantId);
    sharingInstance.setStatus(status);
    if (sharingInstance.getStatus().equals(ConsortiumEnumStatus.ERROR))
      sharingInstance.setError(errorMessage);

    String data = Json.encode(sharingInstance);
    return builder.value(data).topic(topicName).propagateOkapiHeaders(kafkaHeaders).build();
  }

  private KafkaProducer<String, String> getProducer(String tenantId, String topicName) {
    LOGGER.info("getProducer :: tenantId: {}", tenantId);
    if (producerList.get(tenantId) == null) {
      LOGGER.info("getProducer :: trying to create producer for tenantId: {}", tenantId);
      producerList.put(tenantId, KafkaProducer.createShared(vertx, topicName, kafkaConfig.getProducerProps()));
    }
    return producerList.get(tenantId);
  }

}
