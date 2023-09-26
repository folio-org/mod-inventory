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
import org.folio.Record;
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
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.JobProfileInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.inventory.consortium.entities.SharingInstanceEventType.CONSORTIUM_INSTANCE_SHARING_COMPLETE;
import static org.folio.inventory.consortium.entities.SharingStatus.COMPLETE;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.folio.inventory.dataimport.util.DataImportConstants.UNIQUE_ID_ERROR_MESSAGE;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.folio.inventory.domain.items.Item.HRID_KEY;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingHandler.class);
  public static final String COMMITTED = "COMMITTED";
  public static final String ERROR = "ERROR";
  public static final String STATUS = "status";
  public static final String SOURCE = "source";

  public static final String ID = "id";
  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;
  private final RestDataImportHelper dataImportService;

  public static final JobProfileInfo JOB_PROFILE_INFO = new JobProfileInfo()
    .withId("e34d7b92-9b83-11eb-a8b3-0242ac130003") //default stub id
    .withName("Default - Create instance and SRS MARC Bib")
    .withDataType(JobProfileInfo.DataType.MARC);

  public ConsortiumInstanceSharingHandler(Vertx vertx, Storage storage, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.storage = storage;
    this.kafkaConfig = kafkaConfig;
    this.dataImportService = new RestDataImportHelper(vertx);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> event) {
    try {
      SharingInstance sharingInstanceMetadata = Json.decodeValue(event.value(), SharingInstance.class);

      Map<String, String> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersToMap(event.headers());
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();

      LOGGER.info("Event CONSORTIUM_INSTANCE_SHARING_INIT has been received for InstanceId={}, sourceTenant={}, targetTenant={}",
        instanceId, sharingInstanceMetadata.getSourceTenantId(), sharingInstanceMetadata.getTargetTenantId());

      InstanceCollection targetInstanceCollection =
        getTenantSpecificSourceCollection(sharingInstanceMetadata.getTargetTenantId(), kafkaHeaders);

      InstanceCollection sourceInstanceCollection =
        getTenantSpecificSourceCollection(sharingInstanceMetadata.getSourceTenantId(), kafkaHeaders);

      return checkIsInstanceExistsOnTargetTenant(sharingInstanceMetadata, targetInstanceCollection, sourceInstanceCollection, kafkaHeaders);
    } catch (Exception ex) {
      LOGGER.error(format("Failed to process data import kafka record from topic %s", event.topic()), ex);
      return Future.failedFuture(ex);
    }
  }

  private Future<String> checkIsInstanceExistsOnTargetTenant(SharingInstance sharingInstanceMetadata,
                                                           InstanceCollection targetInstanceCollection,
                                                           InstanceCollection sourceInstanceCollection,
                                                           Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String targetTenant = sharingInstanceMetadata.getTargetTenantId();
    LOGGER.info("Checking if instance with InstanceId={} exists on target tenant={}", instanceId, targetTenant);

    return getInstanceById(instanceId, targetTenant, targetInstanceCollection)
      .compose(instance -> {
        String warningMessage = String.format("Instance with InstanceId=%s is present on target tenant: %s", instanceId, targetTenant);
        sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, warningMessage, kafkaHeaders);
        return Future.succeededFuture(warningMessage);
      })
      .onFailure(throwable -> {
        String warningMessage = String.format("Instance with InstanceId=%s is not exists on target tenant: %s.", instanceId, targetTenant);
        LOGGER.info(warningMessage);
      })
      .recover(throwable -> publishInstance(sharingInstanceMetadata, sourceInstanceCollection, targetInstanceCollection, kafkaHeaders));
  }


  private Future<String> publishInstance(SharingInstance sharingInstanceMetadata, InstanceCollection sourceInstanceCollection,
                                         InstanceCollection targetInstanceCollection, Map<String, String> kafkaHeaders) {

    LOGGER.info("publishInstance :: Publishing instance with InstanceId={} from tenant={} to tenant={}.",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getSourceTenantId(),
      sharingInstanceMetadata.getTargetTenantId());

    Promise<String> promise = Promise.promise();

    try {
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
      String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
      String targetTenant = sharingInstanceMetadata.getTargetTenantId();

      getInstanceById(instanceId, sourceTenant, sourceInstanceCollection).onSuccess(srcInstance -> {

          if (FOLIO.getValue().equals(srcInstance.getSource())) {

            publishInstanceWithFolioSource(srcInstance, sharingInstanceMetadata, targetInstanceCollection, sourceInstanceCollection, kafkaHeaders)
              .onComplete(result -> promise.complete()).onFailure(promise::fail);

          } else if (MARC.getValue().equals(srcInstance.getSource())) {

            SourceStorageRecordsClient sourceTenantStorageClient = getSourceStorageRecordsClient(sourceTenant, kafkaHeaders);
            getSourceMARCByInstanceId(instanceId, sourceTenant, sourceTenantStorageClient)
              .compose(marcRecord -> dataImportService.publishInstanceWithMarcSource(marcRecord, sharingInstanceMetadata, kafkaHeaders)
                .compose(dataImportResult -> {
                    LOGGER.info("publishInstance :: Import MARC file result. {}", dataImportResult);
                    if (dataImportResult.equals(COMMITTED)) {
                      deleteSourceRecordByInstanceId(instanceId, sourceTenant, sourceTenantStorageClient)
                        .compose(deletionResult -> {
                          LOGGER.info("publishInstance :: Delete MARC file result. {}", deletionResult);
                          JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
                          jsonInstanceToPublish.put(SOURCE, CONSORTIUM_MARC.getValue());
                          updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceTenant, sourceInstanceCollection)
                            .onSuccess(event -> {
                              String message = format("Instance with InstanceId=%s has been shared to the target tenant %s", instanceId, targetTenant);
                              sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
                              promise.complete(message);
                            }).onFailure(
                              throwable -> promise.fail(throwable.getCause())
                            );
                          return promise.future();
                        });
                    } else {
                      String errorMessage = format("Sharing instance with InstanceId=%s to the target tenant=%s. " +
                        "DI status is %s.", instanceId, targetTenant, dataImportResult);
                      sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                      return Future.failedFuture(errorMessage);
                    }
                    return promise.future();
                  }, throwable -> {
                    String errorMessage = format("Error sharing instance with InstanceId=%s to the target tenant=%s. Error: %s",
                      instanceId, targetTenant, throwable.getMessage());
                    sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                    return Future.failedFuture(errorMessage);
                  }
                ))
              .onFailure(throwable -> {
                String errorMessage = String.format("Error sharing Instance with InstanceId=%s to the target tenant=%s. " +
                  "Error: %s", instanceId, targetTenant, throwable.getMessage());
                sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                promise.fail(throwable);
              });
          } else {
            String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant=%s. Because source is %s",
              instanceId, targetTenant, srcInstance.getSource());
            sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
            promise.fail(errorMessage);
          }
        }).onFailure(throwable -> {
          String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant=%s. " +
            "Because the instance is not found on the source tenant=%s", instanceId, targetTenant, sourceTenant);
          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
          promise.fail(errorMessage);
        });
    } catch (Exception ex) {
      LOGGER.error(format("Failed to import instance with importId to  %s", sharingInstanceMetadata.getInstanceIdentifier()), ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(String tenant, Map<String, String> kafkaHeaders) {
    LOGGER.info("getSourceStorageRecordsClient :: Creating SourceStorageRecordsClient for tenant={}", tenant);
    return new SourceStorageRecordsClient(
      kafkaHeaders.get(URL.toLowerCase()),
      tenant,
      kafkaHeaders.get(TOKEN.toLowerCase()),
      vertx.createHttpClient());
  }

  public ChangeManagerClient getChangeManagerClient(Map<String, String> kafkaHeaders) {
    return new ChangeManagerClient(
      kafkaHeaders.get(URL.toLowerCase()),
      kafkaHeaders.get(TENANT.toLowerCase()),
      kafkaHeaders.get(TOKEN.toLowerCase()),
      vertx.createHttpClient());
  }

  private Future<String> publishInstanceWithFolioSource(Instance instance, SharingInstance sharingInstanceMetadata,
                                                        InstanceCollection targetInstanceCollection,
                                                        InstanceCollection sourceInstanceCollection,
                                                        Map<String, String> kafkaHeaders) {

    LOGGER.info("publishInstanceWithFolioSource :: Publishing instance with InstanceId={} to tenant={}.",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());

    Promise<String> promise = Promise.promise();
    try {
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
      String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
      String targetTenant = sharingInstanceMetadata.getTargetTenantId();

      JsonObject jsonInstance = instance.getJsonForStorage();
      jsonInstance.remove(HRID_KEY);
      addInstance(instance, targetTenant, targetInstanceCollection)
        .compose(addedInstance -> {
          JsonObject jsonInstanceToPublish = instance.getJsonForStorage();
          jsonInstanceToPublish.put(SOURCE, CONSORTIUM_FOLIO.getValue());
          return updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceTenant, sourceInstanceCollection)
            .onComplete(ignored -> {
              String message = format("Instance with InstanceId=%s has been shared to the target tenant=%s", instanceId, targetTenant);
              sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
              promise.complete(message);
            }).onFailure(throwable -> {
              String errorMessage = format("Error updating Instance with InstanceId=%s on source tenant=%s.", instanceId, sourceTenant);
              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
              promise.fail(throwable);
            });
        }, throwable -> {
          String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant=%s.", instanceId, targetTenant);
          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
          promise.fail(throwable);
          return null;
        });
    } catch (Exception ex) {
      LOGGER.error(format("Failed to sharing instance with ImportId=%s to target tenant=%s",
        sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId()), ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  private InstanceCollection getTenantSpecificSourceCollection(String tenantId, Map<String, String> kafkaHeaders) {
    return storage.getInstanceCollection(
      EventHandlingUtil.constructContext(
        tenantId,
        kafkaHeaders.get(TOKEN.toLowerCase()),
        kafkaHeaders.get(URL.toLowerCase()))
    );
  }

  private Future<Record> getSourceMARCByInstanceId(String instanceId, String sourceTenant, SourceStorageRecordsClient client) {

    LOGGER.info("getSourceMARCByInstanceId:: Getting source MARC record for instance with InstanceId={} from tenant={}.",
      instanceId, sourceTenant);
    Promise<Record> promise = Promise.promise();
    client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE)
      .onComplete(resp -> {
        if (resp.succeeded()) {
          int statusCode = resp.result().statusCode();
          if (statusCode == HttpStatus.SC_OK) {
            String bodyAsString = resp.result().bodyAsString();
            LOGGER.debug("MARC source for instance with InstanceId={} from tenant={}. Record={}.", instanceId, sourceTenant, bodyAsString);
            promise.complete(resp.result().bodyAsJson(Record.class));
          } else {
            String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
              "Status message: %s. Status code: %s", instanceId, sourceTenant, resp.result().statusMessage(), statusCode);
            LOGGER.error(errorMessage);
            promise.fail(errorMessage);
          }
        } else {
          String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
            "Error message: %s", instanceId, sourceTenant, resp.cause().getMessage());
          LOGGER.error(errorMessage);
          promise.fail(resp.cause());
        }
      });
    return promise.future();
  }

  private void sendErrorResponseAndPrintLogMessage(String errorMessage, SharingInstance sharingInstance, Map<String, String> kafkaHeaders) {
    LOGGER.error("handle:: {}", errorMessage);
    sendCompleteEventToKafka(sharingInstance, SharingStatus.ERROR, errorMessage, kafkaHeaders);
  }

  private Future<String> deleteSourceRecordByInstanceId(String instanceId, String tenantId, SourceStorageRecordsClient client) {
    LOGGER.info("deleteSourceRecordByInstanceId :: Delete source record for instance by InstanceId={} from tenant={}", instanceId, tenantId);
    Promise<String> promise = Promise.promise();
    client.deleteSourceStorageRecordsById(instanceId).onComplete(response -> {
      if (response.failed()) {
        LOGGER.error("deleteSourceRecordByInstanceId:: Error deleting source record by InstanceId={} from tenant={}",
          instanceId, tenantId, response.cause());
        promise.fail(response.cause());
      } else {
        LOGGER.info("deleteSourceRecordByInstanceId:: Source record for instance with InstanceId={} from tenant={} has been deleted.",
          instanceId, tenantId);
        promise.complete(instanceId);
      }
    });
    return promise.future();
  }

  private Future<Instance> getInstanceById(String instanceId, String tenantId, InstanceCollection instanceCollection) {
    LOGGER.info("getInstanceById :: Get instance by InstanceId={} on tenant={}", instanceId, tenantId);
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          String errorMessage = format("Can't find Instance by InstanceId=%s on tenant=%s.", instanceId, tenantId);
          LOGGER.warn("getInstanceById:: {}", errorMessage);
          promise.fail(new NotFoundException(format(errorMessage)));
        } else {
          LOGGER.debug("getInstanceById :: Instance with InstanceId={} is present on tenant={}.", instanceId, tenantId);
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.error(format("getInstanceById :: Error retrieving Instance by InstanceId=%s from tenant=%s - %s, status code %s",
          instanceId, tenantId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<Instance> addInstance(Instance instance, String tenant, InstanceCollection instanceCollection) {

    LOGGER.info("addInstance :: Publishing instance with InstanceId={} to tenant={}", instance.getId(), tenant);
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

  private Future<Instance> updateInstanceInStorage(Instance instance, String tenant, InstanceCollection instanceCollection) {

    LOGGER.info("updateInstanceInStorage :: Updating instance with InstanceId={} on tenant={}", instance.getId(), tenant);
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
          promise.fail(new OptimisticLockingException(failure.getReason()));
        } else {
          LOGGER.error(format("Error updating instance with InstanceId=%s. Reason: %s. Status code %s",
            instance.getId(), failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  private void sendCompleteEventToKafka(SharingInstance sharingInstance, SharingStatus status, String errorMessage,
                                        Map<String, String> kafkaHeaders) {

    SharingInstanceEventType evenType = CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {
      String tenantId = kafkaHeaders.get(TENANT.toLowerCase());
      List<KafkaHeader> kafkaHeadersList = convertKafkaHeadersMap(kafkaHeaders);

      LOGGER.info("sendEventToKafka :: tenantId={}, instance with InstanceId={}, status={}, message={}",
        tenantId, sharingInstance.getInstanceIdentifier(), status.getValue(), errorMessage);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createProducerRecord(topicName, sharingInstance, status, errorMessage, kafkaHeadersList);
      createProducer(tenantId, topicName).write(kafkaRecord, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("Event with type {}, was sent to kafka", evenType.value());
        } else {
          var cause = ar.cause();
          LOGGER.error("Failed to sent event {}, cause: {}", evenType.value(), cause);
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
      status.equals(SharingStatus.ERROR) ? " message: " + errorMessage + ", " : EMPTY, topicName);

    sharingInstance.setStatus(status);
    if (sharingInstance.getStatus().equals(SharingStatus.ERROR)) {
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

  private List<KafkaHeader> convertKafkaHeadersMap(Map<String, String> kafkaHeaders) {
    return new ArrayList<>(List.of(
      KafkaHeader.header(URL.toLowerCase(), kafkaHeaders.get(URL.toLowerCase())),
      KafkaHeader.header(TENANT.toLowerCase(), kafkaHeaders.get(TENANT.toLowerCase())),
      KafkaHeader.header(TOKEN.toLowerCase(), kafkaHeaders.get(TOKEN.toLowerCase())))
    );
  }

}
