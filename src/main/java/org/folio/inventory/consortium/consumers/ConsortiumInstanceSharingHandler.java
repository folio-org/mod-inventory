package org.folio.inventory.consortium.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.http.HttpException;
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
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
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
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;

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

  private final JobProfileInfo jobProfileInfo = new JobProfileInfo()
    .withId("e34d7b92-9b83-11eb-a8b3-0242ac130003")
    .withName("Default - Create instance and SRS MARC Bib")
    .withDataType(JobProfileInfo.DataType.MARC);

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

    LOGGER.info("checkIsInstanceExistsOnTargetTenant :: Checking is instance with InstanceId={} exists on tenant={}", instanceId, targetTenant);
    return getInstanceById(instanceId, targetTenant, targetInstanceCollection)
      .compose(instance -> {
        String warningMessage = format("Instance with InstanceId=%s is present on target tenant: %s", instanceId, targetTenant);
        sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, warningMessage, kafkaHeaders);
        return Future.succeededFuture(warningMessage);
      }, throwable -> publishInstance(sharingInstanceMetadata, sourceInstanceCollection, targetInstanceCollection, kafkaHeaders));
  }

  private Future<String> publishInstance(SharingInstance sharingInstanceMetadata, InstanceCollection sourceInstanceCollection,
                                         InstanceCollection targetInstanceCollection, Map<String, String> kafkaHeaders) {

    LOGGER.info("publishInstance :: Publishing instance with InstanceId={} to tenant={}.",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());

    Promise<String> promise = Promise.promise();

    try {
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
      String sourceTenant = sharingInstanceMetadata.getSourceTenantId();
      String targetTenant = sharingInstanceMetadata.getTargetTenantId();

      getInstanceById(instanceId, sourceTenant, sourceInstanceCollection)
        .onSuccess(srcInstance -> {

          if (FOLIO.getValue().equals(srcInstance.getSource())) {

            publishInstanceWithFolioSource(srcInstance, sharingInstanceMetadata, targetInstanceCollection, sourceInstanceCollection, kafkaHeaders)
              .onComplete(result -> promise.complete()).onFailure(promise::fail);

          } else if (MARC.getValue().equals(srcInstance.getSource())) {

            SourceStorageRecordsClient sourceTenantStorageClient = new SourceStorageRecordsClient(
              kafkaHeaders.get(URL.toLowerCase()),
              sourceTenant,
              kafkaHeaders.get(TOKEN.toLowerCase()),
              vertx.createHttpClient());

            getSourceMARCByInstanceId(instanceId, sourceTenant, sourceTenantStorageClient)
              .compose(marcRecord -> publishInstanceWithMarcSource(marcRecord, sharingInstanceMetadata, kafkaHeaders))
              .compose(dataImportResult -> {
                  LOGGER.info("publishInstance :: Import MARC file result. {}", dataImportResult);
                  if (dataImportResult.equals(COMMITTED)) {
                    deleteSourceRecordByInstanceId(instanceId, sourceTenant, sourceTenantStorageClient)
                      .compose(deletionResult -> {
                        JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
                        jsonInstanceToPublish.put(SOURCE, CONSORTIUM_MARC.getValue());
                        updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceTenant, sourceInstanceCollection)
                          .onSuccess(event -> {
                            String message = format("Instance with InstanceId=%s has been shared to the target tenant %s", instanceId, targetTenant);
                            sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
                            promise.complete(message);
                          }).onFailure(throwable -> promise.fail(throwable.getCause()));
                        return Future.failedFuture(deletionResult);
                      });
                  }
                  String errorMessage = format("Error sharing instance with InstanceId=%s to the target tenant=%s. Error: %s",
                    instanceId, targetTenant, dataImportResult);
                  sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                  return Future.failedFuture(errorMessage);
                }
              );
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

  private Future<String> publishInstanceWithFolioSource(Instance instance, SharingInstance sharingInstanceMetadata,
                                                        InstanceCollection targetInstanceCollection,
                                                        InstanceCollection sourceInstanceCollection,
                                                        Map<String, String> kafkaHeaders) {
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

  public Future<String> publishInstanceWithMarcSource(Record marcRecord, SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders) {

    LOGGER.info("publishInstanceWithMarcSource:: Importing MARC record for instance with InstanceId={} to target tenant={}.",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());

    Promise<String> promise = Promise.promise();

    try {
      String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
      ChangeManagerClient targetManagerClient = new ChangeManagerClient(
        kafkaHeaders.get(URL.toLowerCase()),
        kafkaHeaders.get(TENANT.toLowerCase()),
        kafkaHeaders.get(TOKEN.toLowerCase()),
        vertx.createHttpClient());

      initJobExecution(targetManagerClient, kafkaHeaders)
        .map(jobExecutionResponse -> jobExecutionResponse.getString(ID))
        .map(jobExecutionId -> setDefaultJobProfileToJobExecution(jobExecutionId, targetManagerClient)
          .compose(ignore -> {
            Object parsedRecord = JsonObject.mapFrom(marcRecord.getParsedRecord().getContent());
            postRecordToParsing(jobExecutionId, true, buildDataChunk(false, singletonList(new InitialRecord().withRecord(parsedRecord.toString()))), targetManagerClient)
              .compose(publishFistChunkResult -> postRecordToParsing(jobExecutionId, false, buildDataChunk(true, new ArrayList<>()), targetManagerClient)
                .compose(publishLastChunkResult -> checkDataImportStatus(jobExecutionId, sharingInstanceMetadata, 20L, 3, targetManagerClient))
                .compose(dataImportResult -> {
                  LOGGER.info("publishInstanceWithMarcSource:: Import MARC file for instance with InstanceId={} has been finished to the target tenant={}. " +
                    "Data import result: {}", instanceId, sharingInstanceMetadata.getTargetTenantId(), dataImportResult);
                  return Future.succeededFuture(dataImportResult);
                }));
            String errorMessage = format("Failed to start import of MARC file for instance with InstanceId=%s to the target tenant=%s.",
              instanceId, sharingInstanceMetadata.getTargetTenantId());
            LOGGER.error("publishInstanceWithMarcSource :: {}", errorMessage);
            return Future.failedFuture(errorMessage);
          }));
    } catch (Exception ex) {
      LOGGER.error("sharingInstanceWithMarcSource:: Starting DI for Instance with InstanceId={} and with MARC source failed.",
        sharingInstanceMetadata.getInstanceIdentifier(), ex);
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

  private RawRecordsDto buildDataChunk(boolean isLast, List<InitialRecord> data) {
    return new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(isLast)
        .withCounter(1)
        .withTotal(1)
        .withContentType(RecordsMetadata.ContentType.MARC_JSON))
      .withInitialRecords(data);
  }

  private Future<String> checkDataImportStatus(String jobExecutionId, SharingInstance sharingInstanceMetadata,
                                               Long durationInSec, Integer attemptsNumber, ChangeManagerClient client) {
    LOGGER.info("checkDataImportStatus:: InstanceId={}, jobExecutionId={}. Start.",
      sharingInstanceMetadata.getInstanceIdentifier(), jobExecutionId);

    Promise<String> promise = Promise.promise();
    AtomicInteger counter = new AtomicInteger(0);

    try {
      vertx.setPeriodic(TimeUnit.SECONDS.toMillis(durationInSec), timerId -> {
        LOGGER.info("checkDataImportStatus:: Checking import status by jobExecutionId={}. InstanceId={}.",
          sharingInstanceMetadata.getInstanceIdentifier(), jobExecutionId);

        getJobExecutionById(jobExecutionId, client)
          .onComplete(jobExecution -> {
            if (jobExecution.succeeded()) {
              JsonObject jobExecutionJson = new JsonObject(jobExecution.result());
              String jobExecutionStatus = jobExecutionJson.getString(STATUS);
              LOGGER.trace("checkDataImportStatus:: Check import status for DI with jobExecutionId={}. " +
                  "InstanceId={}. JobExecution={}", sharingInstanceMetadata.getInstanceIdentifier(),
                jobExecutionId, jobExecutionStatus);
              if (jobExecutionStatus.equals(COMMITTED) || jobExecutionStatus.equals(ERROR)) {
                vertx.cancelTimer(timerId);
                promise.complete(jobExecutionJson.getString(STATUS));
              }
            } else {
              vertx.cancelTimer(timerId);
              String errorMessage = String.format("Failed get jobExecutionId=%s for DI with InstanceId=%s. " +
                "Error: %s", jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier(), jobExecution.cause().getMessage());
              LOGGER.error("checkDataImportStatus:: {}", errorMessage);
              promise.fail(errorMessage);
            }

            if (counter.getAndIncrement() > attemptsNumber) {
              vertx.cancelTimer(timerId);
              String errorMessage = String.format("Number of attempts=%s to check DI status has ended for jobExecutionId=%s",
                counter.get(), jobExecutionId);
              LOGGER.info(errorMessage);
              promise.fail(errorMessage);
            }
          });
      });
    } catch (Exception ex) {
      LOGGER.error("checkDataImportStatus:: Checking DI status failed", ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  private Future<String> getJobExecutionById(String jobExecutionId, ChangeManagerClient client) {
    LOGGER.info("getJobExecutionById:: Getting jobExecution by jobExecutionId={}.", jobExecutionId);
    Promise<String> promise = Promise.promise();
    try {
      client.getChangeManagerJobExecutionsById(jobExecutionId, response -> {
        if (response.result().statusCode() != HttpStatus.SC_OK) {
          String errorMessage = format("Error getting jobExecution by jobExecutionId=%s. " +
              "Status message: %s. Status code: %s", jobExecutionId, response.result().statusMessage(),
            response.result().statusCode());
          LOGGER.error(errorMessage);
          promise.fail(errorMessage);
        } else {
          promise.complete(response.result().bodyAsJsonObject().toString());
        }
      });
    } catch (Exception ex) {
      LOGGER.error("getJobExecutionById:: Error getting jobExecution by jobExecutionId={}.", jobExecutionId, ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  private Future<Record> getSourceMARCByInstanceId(String instanceId, String sourceTenant, SourceStorageRecordsClient client) {
    LOGGER.info("getSourceMARCByInstanceId:: Getting source MARC record for instance with InstanceId={} from tenant={}.",
      instanceId, sourceTenant);
    return client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE)
      .compose(resp -> {
        if (resp.statusCode() != HttpStatus.SC_OK) {
          String errorMessage = format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
            "Status message: %s. Status code: %s", instanceId, sourceTenant, resp.statusMessage(), resp.statusCode());
          LOGGER.error(errorMessage);
          return Future.failedFuture(errorMessage);
        }
        LOGGER.trace("getSourceMARCByInstanceId:: MARC source for instance with InstanceId={} from tenant={}. Record={}. Finish.",
          instanceId, sourceTenant, resp.bodyAsString());
        return Future.succeededFuture(resp.bodyAsJson(Record.class));
      });
  }

  private Future<JsonObject> initJobExecution(ChangeManagerClient client, Map<String, String> kafkaHeaders) {
    LOGGER.info("initJobExecution:: Receiving new JobExecution ...");
    Promise<JsonObject> promise = Promise.promise();
    try {

      InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
        .withSourceType(InitJobExecutionsRqDto.SourceType.ONLINE)
        .withUserId(kafkaHeaders.get(USER_ID.toLowerCase()));

      client.postChangeManagerJobExecutions(initJobExecutionsRqDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_CREATED) {
          LOGGER.error("initJobExecution:: Error receiving new JobExecution. " +
            "Status message: {}. Status code: {}", response.result().statusMessage(), response.result().statusCode());
          promise.fail(new HttpException("Error receiving JobExecutionId.", response.cause()));
        } else {
          JsonObject responseBody = response.result().bodyAsJsonObject();
          LOGGER.trace("initJobExecution:: ResponseBody: {}", responseBody);
          JsonArray jobExecutions = responseBody.getJsonArray("jobExecutions");
          LOGGER.trace("initJobExecution:: ResponseBody.JobExecutions: {}", jobExecutions);

          if (jobExecutions.isEmpty()) {
            promise.fail("Response body doesn't contains JobExecution object");
          } else {
            promise.complete(jobExecutions.getJsonObject(0));
          }
        }
      });
    } catch (Exception ex) {
      LOGGER.error(format("initJobExecution :: Error init JobExecution. Error: %s", ex.getMessage()));
      promise.fail(ex);
    }
    return promise.future();
  }

  private Future<JsonObject> setDefaultJobProfileToJobExecution(String jobExecutionId, ChangeManagerClient client) {
    LOGGER.info("setJobProfileToJobExecution:: Linking JobProfile to JobExecution with jobExecutionId={}", jobExecutionId);
    Promise<JsonObject> promise = Promise.promise();
    try {
      client.putChangeManagerJobExecutionsJobProfileById(jobExecutionId, jobProfileInfo, response -> {
        if (response.result().statusCode() != HttpStatus.SC_OK) {
          LOGGER.warn("setJobProfileToJobExecution:: Failed to set JobProfile for JobExecution. " +
            "Status message: {}. Status code: {}", response.result().statusMessage(), response.result().statusCode());
          promise.fail(new HttpException(format("Failed to set JobProfile for JobExecution with jobExecutionId=%s.",
            jobExecutionId), response.cause()));
        } else {
          LOGGER.trace("setJobProfileToJobExecution:: Response: {}", response.result().bodyAsJsonObject());
          promise.complete(response.result().bodyAsJsonObject());
        }
      });
    } catch (Exception ex) {
      LOGGER.error(format("setJobProfileToJobExecution:: Failed to link JobProfile to JobExecution " +
        "with jobExecutionId=%s. Error: %s", jobExecutionId, ex.getCause()));
      promise.fail(ex);
    }
    return promise.future();
  }

  private Future<JsonObject> postRecordToParsing(String jobExecutionId, Boolean acceptInstanceId,
                                                 RawRecordsDto rawRecordsDto, ChangeManagerClient client) {
    LOGGER.info("postRecordToParsing :: Sending data for jobExecutionId={}, acceptInstanceId={}.",
      jobExecutionId, acceptInstanceId);
    Promise<JsonObject> promise = Promise.promise();
    try {
      client.postChangeManagerJobExecutionsRecordsById(jobExecutionId, acceptInstanceId, rawRecordsDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_NO_CONTENT) {
          LOGGER.error("postRecordToParsing:: Failed sending data. Status message: {}. Status code: {}.",
            response.result().statusMessage(), response.result().statusCode());
          promise.fail(new HttpException("Failed sending record data.", response.cause()));
        } else {
          LOGGER.trace("postRecordToParsing:: Sending data result: {}", response.result());
          promise.complete(response.result().bodyAsJsonObject());
        }
      });
    } catch (Exception e) {
      LOGGER.error("postRecordToParsing:: Error sending data for jobExecutionId={}. Error: {}",
        jobExecutionId, e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }

  private void sendErrorResponseAndPrintLogMessage(String errorMessage, SharingInstance sharingInstance, Map<String, String> kafkaHeaders) {
    LOGGER.error("handle:: {}", errorMessage);
    sendCompleteEventToKafka(sharingInstance, SharingStatus.ERROR, errorMessage, kafkaHeaders);
  }

  private Future<String> deleteSourceRecordByInstanceId(String instanceId, String tenantId, SourceStorageRecordsClient client) {
    LOGGER.info("deleteSourceRecordByInstanceId :: Delete instance by InstanceId={} from tenant={}", instanceId, tenantId);
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
      sharingInstance.getInstanceIdentifier(),
      status,
      status.equals(SharingStatus.ERROR) ? " message: " + errorMessage + ", " : EMPTY,
      topicName);

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
