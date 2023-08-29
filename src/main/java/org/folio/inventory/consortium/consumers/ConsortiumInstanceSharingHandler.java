package org.folio.inventory.consortium.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.model.SharingInstance;
import org.folio.inventory.dataimport.consumers.DataImportKafkaHandler;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;
import java.util.UUID;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(DataImportKafkaHandler.class);

  private static final String OKAPI_TOKEN_HEADER = "X-Okapi-Token";
  private static final String OKAPI_URL_HEADER = "X-Okapi-Url";

  private Vertx vertx;

  private Storage storage;

  public ConsortiumInstanceSharingHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      LOGGER.info("handle :: record.key : {}", record.key());
      Promise<String> promise = Promise.promise();
      SharingInstance sharingInstance = Json.decodeValue(record.value(), SharingInstance.class);

      String instanceId = sharingInstance.getInstanceIdentifier().toString();
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      LOGGER.info("Event 'sharing instance' has been received for instanceId: {}, sourceTenant: {}, targetTenant: {}",
        instanceId, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId());

      LOGGER.info("OKAPI_TOKEN_HEADER = {}", headersMap.get(OKAPI_TOKEN_HEADER));
      LOGGER.info("OKAPI_URL_HEADER = {}", headersMap.get(OKAPI_URL_HEADER));

      //make GET request by Instance UUID on target (consortium) tenant, if exists - (publish error event?), if not - proceed
      Context targetTenantContext = EventHandlingUtil.constructContext(sharingInstance.getTargetTenantId(),
        headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      LOGGER.info("handle :: targetTenantContext : tenantId : {}", targetTenantContext.getTenantId());

      InstanceCollection targetInstanceCollection = storage.getInstanceCollection(targetTenantContext);
      LOGGER.info("handle :: targetInstanceCollection : {}", targetInstanceCollection);

      Context sourceTenantContext = EventHandlingUtil.constructContext(sharingInstance.getSourceTenantId(),
        headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
      LOGGER.info("handle :: sourceTenantContext : tenantId : {}", targetTenantContext.getTenantId());

      InstanceCollection sourceInstanceCollection = storage.getInstanceCollection(sourceTenantContext);
      LOGGER.info("handle :: sourceInstanceCollection : {}", sourceInstanceCollection);

      getInstanceById(instanceId, targetInstanceCollection)
        .onSuccess(instanceOnTargetTenant -> {
          if (instanceOnTargetTenant == null) {
            LOGGER.info("handle :: instance {} not found on target tenant: {}",
              instanceId, sharingInstance.getTargetTenantId());
            getInstanceById(instanceId, targetInstanceCollection)
              .onSuccess(instanceOnSourceTenant -> {
                if (instanceOnSourceTenant == null) {
                  String errorMessage = format("handle :: instance {} not found on source tenant: {}",
                    instanceId, sharingInstance.getSourceTenantId());
                  LOGGER.error(errorMessage);
                  promise.fail(errorMessage);
                } else {
                  LOGGER.info("handle :: Instance {} from {} tenant with source {}", instanceId,
                    sharingInstance.getSourceTenantId(), instanceOnSourceTenant.getSource());
//            if ("FOLIO".equals(instanceToPublish.getSource())) {
                  addInstance(instanceOnSourceTenant, targetInstanceCollection).onSuccess(
                    publishedInstance ->  {
                      LOGGER.info("handle :: Updating source to 'CONSORTIUM-FOLIO' for instance {}", instanceId);
                      JsonObject jsonInstanceToPublish = instanceOnSourceTenant.getJsonForStorage();
                      jsonInstanceToPublish.put("source", "CONSORTIUM-FOLIO");
                      updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                        .onSuccess(updatesSourceInstance -> {
                          LOGGER.info("handle :: source 'CONSORTIUM-FOLIO' updated to instance {}", instanceId);
                          promise.complete();
                        }).onFailure(error -> {
                          String errorMessage = format("Error update Instance by id %s on the source tenant %s. Error: %s",
                            instanceId, sharingInstance.getTargetTenantId(), error.getCause());
                          LOGGER.error(errorMessage);
                          promise.fail(error);
                        });
                    }
                  ).onFailure( e -> {
                    String errorMessage = format("Error save Instance by id %s on the target tenant %s. Error: %s",
                      instanceId, sharingInstance.getTargetTenantId(), e.getCause());
                    LOGGER.error(errorMessage);
                    promise.fail(e);
                  });
                  //TODO: send Instance to the target tenant
                  // make PUT request to update source to CONSORTIUM-FOLIO, set HRID (changing logic of PUT endpoint is our of scope of this task)
                  // publish CONSORTIUM_INSTANCE_SHARING_COMPLETE or DI_ERROR???
//              }
                }
              })
              .onFailure(failure -> {
                String errorMessage = format("Error retrieving Instance by id %s from source tenant %s. Error: %s",
                  instanceId, sharingInstance.getSourceTenantId(), failure);
                LOGGER.error(errorMessage);
                promise.fail(errorMessage);
              });
          } else {
            String errorMessage = format("handle :: instance {} is present on target tenant: {}",
              instanceOnTargetTenant.getId(), sharingInstance.getTargetTenantId());
            LOGGER.error(errorMessage);
            promise.fail(errorMessage);
          }
        })
        .onFailure(failure -> {
          String errorMessage = format("Error retrieving Instance by id %s from target tenant %s. Error: %s",
            instanceId, sharingInstance.getTargetTenantId(), failure);
          LOGGER.error(errorMessage);
          promise.fail(errorMessage);
        });
      return promise.future();
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  public Future<String> handle1(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> promise = Promise.promise();
      SharingInstance sharingInstance = Json.decodeValue(record.value(), SharingInstance.class);

      String instanceId = sharingInstance.getInstanceIdentifier().toString();
      Map<String, String> headersMap = KafkaHeaderUtils.kafkaHeadersToMap(record.headers());
      LOGGER.info("Data import event payload has been received with event type: {}, instanceId: {}",
        record.key(), instanceId);

      //make GET request by Instance UUID on target (consortium) tenant, if exists - (publish error event?), if not - proceed
      Context targetTenantContext = EventHandlingUtil.constructContext(sharingInstance.getTargetTenantId(),
        headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));

      InstanceCollection targetInstanceCollection = storage.getInstanceCollection(targetTenantContext);
      targetInstanceCollection.findById(UUID.randomUUID().toString(), instanceTargetSerachSuccess -> { //UUID.randomUUID().toString() -> instanceId
        if (instanceTargetSerachSuccess.getResult() == null) {
          //if source FOLIO - make GET request by Instance UUID on source tenant and POST on target tenant with source=FOLIO
          Context sourceTenantContext = EventHandlingUtil.constructContext(sharingInstance.getSourceTenantId(),
            headersMap.get(OKAPI_TOKEN_HEADER), headersMap.get(OKAPI_URL_HEADER));
          InstanceCollection sourceInstanceCollection = storage.getInstanceCollection(sourceTenantContext);
          sourceInstanceCollection.findById(instanceId, instanceSourceSerachSuccess -> {
            if (instanceSourceSerachSuccess.getResult() == null) {
              String errorMessage = String.format("Can't find Instance by id %s on source tenant: %s", instanceId, sharingInstance.getSourceTenantId());
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            } else {
              Instance instanceToPublish = instanceSourceSerachSuccess.getResult();
//            if ("FOLIO".equals(instanceToPublish.getSource())) {
                addInstance(instanceToPublish, targetInstanceCollection).onSuccess(
                  publishedInstance ->  {
                    JsonObject jsonInstanceToPublish = instanceToPublish.getJsonForStorage();
                    jsonInstanceToPublish.put("source", "CONSORTIUM-FOLIO");
                    updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
                      .onSuccess(updatesSourceInstance -> {
                        promise.complete();
                      }).onFailure(error -> {
                        String errorMessage = format("Error update Instance by id %s on the source tenant %s. Error: %s",
                          instanceId, sharingInstance.getTargetTenantId(), error.getCause());
                        LOGGER.error(errorMessage);
                        promise.fail(error);
                      });
                  }
                ).onFailure( e -> {
                  String errorMessage = format("Error save Instance by id %s on the target tenant %s. Error: %s",
                    instanceId, sharingInstance.getTargetTenantId(), e.getCause());
                  LOGGER.error(errorMessage);
                  promise.fail(e);
                });
               //TODO: send Instance to the target tenant
               // make PUT request to update source to CONSORTIUM-FOLIO, set HRID (changing logic of PUT endpoint is our of scope of this task)
               // publish CONSORTIUM_INSTANCE_SHARING_COMPLETE or DI_ERROR???
//              }
            }
          },
            failure -> {
              String errorMessage = format("Error retrieving Instance by id %s from source tenant %s - %s, status code %s",
                instanceId, sharingInstance.getSourceTenantId(), failure.getReason(), failure.getStatusCode());
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            });
        } else {
          String alreadyExists = String.format("Instance %s already exists on target tenant %s",
            instanceId, sharingInstance.getTargetTenantId());
          LOGGER.error(alreadyExists);
          promise.fail(alreadyExists);
        }
      }, failure -> {
        String errorMessage = format("Error retrieving Instance by id %s from target tenant $s - %s, status code %s",
          instanceId, sharingInstance.getTargetTenantId(), failure.getReason(), failure.getStatusCode());
        LOGGER.error(errorMessage);
        promise.fail(errorMessage);
      });

      return promise.future();
    } catch (Exception e) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", record.topic()), e);
      return Future.failedFuture(e);
    }
  }

  private Future<Instance> getInstanceById(String instanceId, InstanceCollection instanceCollection) {
    LOGGER.info("getInstanceById :: instanceId : {}", instanceId);
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.error("Can't find Instance by id: {} ", instanceId);
          promise.fail(new NotFoundException(format("Can't find Instance by id: %s", instanceId)));
        } else {
          LOGGER.info("getInstanceById :: instanceCollection.findById :: success : {}", success);
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
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

  private String getInstanceId(Record record) {
    String subfield999ffi = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
    return isEmpty(subfield999ffi) ? UUID.randomUUID().toString() : subfield999ffi;
  }

}
