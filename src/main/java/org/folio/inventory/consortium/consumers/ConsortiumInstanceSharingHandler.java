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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.isEmpty;
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
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;

public class ConsortiumInstanceSharingHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger(ConsortiumInstanceSharingHandler.class);

  private final Vertx vertx;
  private final Storage storage;
  private final KafkaConfig kafkaConfig;

  private static final String INSTANCE_ID_TYPE = "INSTANCE";
  private static final String DEFAULT_INSTANCE_JOB_PROFILE_ID = "e34d7b92-9b83-11eb-a8b3-0242ac130003";

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
    Promise<String> promise = Promise.promise();
    try {
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
                return updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish),
                  sharingInstanceMetadata.getSourceTenantId(), sourceInstanceCollection)
                  .compose(ignored -> {
                    String message = format("Instance with InstanceId=%s has been shared to the target tenant %s",
                      instanceId, targetTenant);
                    sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
                    return Future.succeededFuture(message);
                  }, throwable -> {
                    String errorMessage = format("Error updating Instance with InstanceId=%s on source tenant %s.",
                      instanceId, sourceTenant);
                    sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                    return Future.failedFuture(throwable);
                  });
              }, throwable -> {
                String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s.",
                  instanceId, targetTenant);
                sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                return Future.failedFuture(errorMessage);
              });
          } else if ("MARC".equals(srcInstance.getSource())) {
            SourceStorageRecordsClient sourceTenantStorageClient = new SourceStorageRecordsClient(kafkaHeaders.get(URL.toLowerCase()),
              sourceTenant, kafkaHeaders.get(TOKEN.toLowerCase()), vertx.createHttpClient());
            return getParsedSourceMARCByInstanceId(instanceId, sourceTenant, sourceTenantStorageClient)
              .compose(record -> {
                  return sharingInstanceWithMarcSource(record, sharingInstanceMetadata, kafkaHeaders)
                    .compose(ignore -> {
                      return sourceTenantStorageClient.deleteSourceStorageRecordsById(instanceId)
                        .compose(response -> {
                          if (response.statusCode() != HttpStatus.SC_NO_CONTENT) {
                            String errorMessage = String.format("Failed to delete MARC source for Instance=%s. Error: %s",
                              instanceId, response.statusMessage());
                            sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                            return Future.failedFuture(errorMessage);
                          } else {
                            String message = format("Instance with InstanceId=%s has been shared to the target tenant %s",
                              instanceId, targetTenant);
                            sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
                            return Future.succeededFuture(message);
                          }
                        }, throwable -> {
                          String errorMessage = String.format("Failed to delete MARC source for Instance=%s. Error: %s",
                            instanceId, throwable.getCause().getMessage());
                          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                          return Future.failedFuture(throwable);
                        });
                    }, throwable -> {
                      String errorMessage = String.format("Failed to get MARC record from source tenant %s " +
                          "for Instance with InstanceId=%s. Error: %s", sharingInstanceMetadata.getSourceTenantId(),
                        sharingInstanceMetadata.getInstanceIdentifier(), throwable.getCause().getMessage());
                      sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                      return Future.failedFuture(throwable);
                    });
                },
                throwable -> {
                  String errorMessage = String.format("Failed to get MARC source for Instance=%s. Error: %s",
                    instanceId, throwable.getCause());
                  sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                  return Future.failedFuture(throwable);
                });
          } else {
            String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s. Because source is %s",
              instanceId, targetTenant, srcInstance.getSource());
            sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
            return Future.failedFuture(errorMessage);
          }
        });
    } catch (Exception ex) {
      LOGGER.error(format("Failed to import instance with importId to  %s",
        sharingInstanceMetadata.getInstanceIdentifier()), ex);
      return Future.failedFuture(ex);
    }

//    return getInstanceById(instanceId, sourceTenant, sourceInstanceCollection)
//      .compose(srcInstance -> {
//        if ("FOLIO".equals(srcInstance.getSource())) {
//          JsonObject jsonInstance = srcInstance.getJsonForStorage();
//          jsonInstance.remove(HRID_KEY);
//          return addInstance(srcInstance, targetTenant, targetInstanceCollection)
//            .compose(addedInstance -> {
//              JsonObject jsonInstanceToPublish = srcInstance.getJsonForStorage();
//              jsonInstanceToPublish.put("source", CONSORTIUM_FOLIO.getValue());
//              return updateInstanceInStorage(Instance.fromJson(jsonInstanceToPublish), sourceInstanceCollection)
//                .map(ignored -> "Instance has been updated successfully")
//                .onSuccess(ignored -> {
//                  String message = format("Instance with InstanceId=%s has been shared to the target tenant %s",
//                    instanceId, targetTenant);
//                  sendCompleteEventToKafka(sharingInstanceMetadata, COMPLETE, message, kafkaHeaders);
//                }).onFailure(throwable -> {
//                  String errorMessage = format("Error updating Instance with InstanceId=%s on source tenant %s.",
//                    instanceId, sourceTenant);
//                  sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//                });
//            }).onFailure(throwable -> {
//              String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s.",
//                instanceId, targetTenant);
//              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//            });
//        } else if ("MARC".equals(srcInstance.getSource())) {
//          sharingInstanceWithMarcSource(sharingInstanceMetadata, kafkaHeaders)
//            .onComplete(result -> {
//              return;
//            }).onFailure(throwable -> {
//              return;
//            });
//          return null;
//        } else {
//          String errorMessage = format("Error sharing Instance with InstanceId=%s to the target tenant %s. Because source is %s",
//            instanceId, targetTenant, srcInstance.getSource());
//          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//          return Future.failedFuture("errorMessage");
//        }
//      }, throwable -> {
//        String errorMessage = format("Error retrieving Instance by InstanceId=%s from source tenant %s.", instanceId, sourceTenant);
//        if (throwable != null && throwable.getCause() != null)
//          errorMessage += " Error: " + throwable.getCause();
//        sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//        return Future.failedFuture(errorMessage);
//      }).otherwiseEmpty();
  }

  public Future<String> sharingInstanceWithMarcSource(Record marcRecord, SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders) {
    LOGGER.info("sharingInstanceWithMarcSource:: InstanceId={}. Start.", sharingInstanceMetadata.getInstanceIdentifier());

    Promise<String> promise = Promise.promise();
    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();

    ChangeManagerClient targetManagerClient = new ChangeManagerClient(kafkaHeaders.get(URL.toLowerCase()),
      kafkaHeaders.get(TENANT.toLowerCase()), kafkaHeaders.get(TOKEN.toLowerCase()), vertx.createHttpClient());

    getNewJobExecutionByChangeManager(targetManagerClient, kafkaHeaders).onComplete(jobExecutionRes -> {
      if (jobExecutionRes.failed()) {
        String errorMessage = String.format("Failed to handle job execution: %s", jobExecutionRes.cause().getMessage());
        promise.fail(new CompletionException(errorMessage, jobExecutionRes.cause()));
        sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
      } else {
        String jobExecutionId = jobExecutionRes.result().getString("id");
        setDefaultJobProfileToJobExecution(jobExecutionId, targetManagerClient)
          .onComplete(jobProfileSet -> {
            if (jobProfileSet.failed()) {
              String errorMessage = String.format("Failed to link jobProfile to jobExecution with jibExecutionId=%s: Error: %s",
                jobExecutionId, jobProfileSet.cause().getMessage());
              promise.fail(new CompletionException(errorMessage, jobProfileSet.cause()));
              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
            } else {
              // Post record to parsing
              String jsonRecord = JsonObject.mapFrom(marcRecord).toString();
              RawRecordsDto sendRecord = new RawRecordsDto()
                .withId(UUID.randomUUID().toString())
                .withRecordsMetadata(new RecordsMetadata()
                  .withLast(false)
                  .withCounter(1)
                  .withTotal(1)
                  .withContentType(RecordsMetadata.ContentType.MARC_JSON))
                .withInitialRecords(singletonList(new InitialRecord().withRecord(jsonRecord)));

              LOGGER.info("sharingInstanceWithMarcSource:: InstanceId={}. Send record to parsing.", sharingInstanceMetadata.getInstanceIdentifier());
              postRecordToParsing(jobExecutionId, true, sendRecord, targetManagerClient).onComplete(postRecords -> {
                if (postRecords.failed()) {
                  String errorMessage = String.format("Failed start DI with jobExecutionId=%s for " +
                    "sharing instance with InstanceId=%s. Error: %s", jobExecutionId, instanceId, postRecords.cause().getMessage());
                  promise.fail(new CompletionException(errorMessage, postRecords.cause()));
                  sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                } else {
                  // Check record
                  RawRecordsDto checkRecord = new RawRecordsDto()
                    .withId(UUID.randomUUID().toString())
                    .withRecordsMetadata(new RecordsMetadata()
                      .withLast(true)
                      .withCounter(1)
                      .withTotal(1)
                      .withContentType(RecordsMetadata.ContentType.MARC_JSON));

                  vertx.setPeriodic(TimeUnit.MINUTES.toMillis(1), timerId -> {
                    LOGGER.info("sharingInstanceWithMarcSource:: InstanceId={}. Check import status for DI with jobExecutionId={}.", sharingInstanceMetadata.getInstanceIdentifier(), jobExecutionId);
                    postRecordToParsing(jobExecutionId, false, checkRecord, targetManagerClient)
                      .onComplete(importResult -> {
                        if (importResult.failed()) {
                          String errorMessage = String.format("DI failed with jobExecutionId=%s for " +
                            "Instance with InstanceId=%s. Error: %s", jobExecutionId, instanceId, importResult.cause().getMessage());
                          promise.fail(new CompletionException(errorMessage, importResult.cause()));
                          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
                        } else {
                          JsonObject checkResult = importResult.result();
                          LOGGER.info("sharingInstanceWithMarcSource:: Status checking DI with jobExecutionId={} result: {}",
                            jobExecutionId, checkResult.getString("status"));
                          if (checkResult.getBoolean("COMPLETED")) {
                            promise.complete(marcRecord.getMatchedId());
                          } else if (checkResult.getBoolean("ERROR")) {
                            promise.fail(new CompletionException(new Exception("ERROR")));
                          }
                          vertx.cancelTimer(timerId);
                        }
                      });
                  });
                }
              });
            }
          });
      }
    });
    return promise.future();
  }

//  public Future<String> sharingInstanceWithMarcSource1(SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders) {
//
//    LOGGER.info("sharingInstanceWithMarcSource:: InstanceId={}. Start.", sharingInstanceMetadata.getInstanceIdentifier());
//
//    Promise<String> promise = Promise.promise();
//    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
//
//    ChangeManagerClient targetManagerClient = new ChangeManagerClient(kafkaHeaders.get(URL.toLowerCase()),
//      kafkaHeaders.get(TENANT.toLowerCase()), kafkaHeaders.get(TOKEN.toLowerCase()), vertx.createHttpClient());
//
//    getParsedSourceMARCByInstanceId(instanceId, sharingInstanceMetadata.getSourceTenantId(), kafkaHeaders)
//      .onComplete(marcRecord -> {
//
//        if (marcRecord.failed()) {
//          String errorMessage = String.format("Failed to get MARC source for Instance=%s. Error: %s",
//            instanceId, marcRecord.cause().getMessage());
//          promise.fail(new CompletionException(errorMessage, marcRecord.cause()));
//          sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//        } else {
//          getJobExecutionByChangeManager(targetManagerClient, kafkaHeaders).onComplete(jobExecutionRes -> {
//            if (jobExecutionRes.failed()) {
//              String errorMessage = String.format("Failed to handle job execution: %s",
//                jobExecutionRes.cause().getMessage());
//              promise.fail(new CompletionException(errorMessage, jobExecutionRes.cause()));
//              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//            } else {
//              String jobExecutionId = jobExecutionRes.result().getString("id");
//              setDefaultJobProfileToJobExecution(jobExecutionId, targetManagerClient)
//                .onComplete(jobProfileSet -> {
//                  if (jobProfileSet.failed()) {
//                    String errorMessage = String.format("Failed to link jobProfile to jobExecution with jibExecutionId=%s: Error: %s",
//                      jobExecutionId, jobProfileSet.cause().getMessage());
//                    promise.fail(new CompletionException(errorMessage, jobProfileSet.cause()));
//                    sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//                  } else {
//                    // Post record to parsing
//                    String jsonRecord = JsonObject.mapFrom(marcRecord.result()).toString();
//                    RawRecordsDto sendRecord = new RawRecordsDto()
//                      .withId(UUID.randomUUID().toString())
//                      .withRecordsMetadata(new RecordsMetadata()
//                        .withLast(false)
//                        .withCounter(1)
//                        .withTotal(1)
//                        .withContentType(RecordsMetadata.ContentType.MARC_JSON))
//                      .withInitialRecords(singletonList(new InitialRecord().withRecord(jsonRecord)));
//
//                    postRecordToParsing(jobExecutionId, sendRecord, targetManagerClient).onComplete(postRecords -> {
//                      if (postRecords.failed()) {
//                        String errorMessage = String.format("Failed start DI with jobExecutionId=%s for " +
//                            "sharing instance with InstanceId=%s. Error: %s", jobExecutionId, instanceId,
//                          marcRecord.cause().getMessage());
//                        promise.fail(new CompletionException(errorMessage, marcRecord.cause()));
//                        sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//                      } else {
//                        // Check record
//                        RawRecordsDto checkRecord = new RawRecordsDto()
//                          .withId(UUID.randomUUID().toString())
//                          .withRecordsMetadata(new RecordsMetadata()
//                            .withLast(true)
//                            .withCounter(1)
//                            .withTotal(1)
//                            .withContentType(RecordsMetadata.ContentType.MARC_JSON));
//
//
//                        vertx.setPeriodic(TimeUnit.MINUTES.toMillis(1), timerId -> {
//                          postRecordToParsing(jobExecutionId, checkRecord, targetManagerClient).onComplete(checkRecords -> {
//                            if (checkRecords.failed()) {
//                              String errorMessage = String.format("DI failed with jobExecutionId=%s for " +
//                                "Instance with InstanceId=%s. Error: %s", jobExecutionId, instanceId, marcRecord.cause().getMessage());
//                              promise.fail(new CompletionException(errorMessage, marcRecord.cause()));
//                              sendErrorResponseAndPrintLogMessage(errorMessage, sharingInstanceMetadata, kafkaHeaders);
//                            } else {
//                              JsonObject checkResult = checkRecords.result();
//                              LOGGER.info("sharingInstanceWithMarcSource:: Check import with jobExecutionId={} result: {}",
//                                jobExecutionId, checkResult.getString("status"));
//                              if (checkResult.getBoolean("COMPLETED")) {
//                                vertx.cancelTimer(timerId);
//                                promise.complete(marcRecord.result().getMatchedId());
//                              } else if (checkResult.getBoolean("ERROR")) {
//                                vertx.cancelTimer(timerId);
//                                promise.fail(new CompletionException(new Exception("ERROR")));
//                              }
//                            }
//                          });
//                        });
//                      }
//                    });
//                  }
//                });
//            }
//          });
//        }
//
//      });
//    return promise.future();
//  }

  private Future<Record> getParsedSourceMARCByInstanceId(String instanceId, String sourceTenant, SourceStorageRecordsClient client) {
    LOGGER.info("getParsedMARCByInstanceId:: For Instance with InstanceId={} from tenant {}. Start.", instanceId, sourceTenant);
    return client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE).compose(resp -> {
      if (resp.statusCode() != 200) {
        return Future.failedFuture(format("Failed to retrieve MARC record by instance id: '%s', status code: %s",
          instanceId, resp.statusCode()));
      }
      LOGGER.info("getParsedMARCByInstanceId:: InstanceId={}. Record:{}. Finish.", instanceId, resp.bodyAsString());
      return Future.succeededFuture(resp.bodyAsJson(Record.class));
    });
  }

  private Future<JsonObject> getNewJobExecutionByChangeManager(ChangeManagerClient client, Map<String, String> kafkaHeaders) {

    Promise<JsonObject> promise = Promise.promise();
    try {

      LOGGER.info("getJobExecutionByChangeManager:: Start.");

      InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
        .withSourceType(InitJobExecutionsRqDto.SourceType.ONLINE)
        .withUserId(kafkaHeaders.get(USER_ID.toLowerCase()));

      client.postChangeManagerJobExecutions(initJobExecutionsRqDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_CREATED) {
          LOGGER.info("getJobExecutionByChangeManager:: Error creating new JobExecution. Status message: {}",
            response.result().statusMessage());
          promise.fail(new HttpException("Error creating new JobExecution", response.cause()));
        } else {
          LOGGER.info("getJobExecutionByChangeManager:: Response: {}", response.result());
          JsonObject responseBody = response.result().bodyAsJsonObject();
          LOGGER.info("getJobExecutionByChangeManager:: ResponseBody: {}", responseBody);
          JsonArray jobExecutions = responseBody.getJsonArray("jobExecutions");
          LOGGER.info("getJobExecutionByChangeManager:: JobExecutions: {}", jobExecutions);
          promise.complete(jobExecutions.getJsonObject(0));
        }
      });
    } catch (Exception e) {
      LOGGER.error("getJobExecutionByChangeManager:: Error: {}", e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<JsonObject> setDefaultJobProfileToJobExecution(String jobExecutionId, ChangeManagerClient client) {
    LOGGER.info("setJobProfileToJobExecution:: jobExecutionId={} Start.", jobExecutionId);
    Promise<JsonObject> promise = Promise.promise();
    try {

      // Set job profile
      JobProfileInfo jobProfileInfo = new JobProfileInfo()
        .withId(DEFAULT_INSTANCE_JOB_PROFILE_ID)
        .withName("Default - Create instance and SRS MARC Bib")
        .withDataType(JobProfileInfo.DataType.MARC);

      client.putChangeManagerJobExecutionsJobProfileById(jobExecutionId, jobProfileInfo, response -> {
        if (response.result().statusCode() != HttpStatus.SC_OK) {
          LOGGER.warn("setJobProfileToJobExecution:: Failed to set JobProfile for JobExecution. Status message: {}",
            response.result().statusMessage());
          promise.fail(new HttpException("Failed to set JobProfile for JobExecution.", response.cause()));
        } else {
          LOGGER.info("setJobProfileToJobExecution:: Response: {}", response.result());
          promise.complete(response.result().bodyAsJsonObject());
        }
      });
    } catch (Exception e) {
      LOGGER.error("setJobProfileToJobExecution:: Error: {}", e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<JsonObject> postRecordToParsing(String jobExecutionId, Boolean acceptInstanceId, RawRecordsDto rawRecordsDto,
                                                 ChangeManagerClient client) {
    LOGGER.info("postRecordToParsing :: jobExecutionId={}, acceptInstanceId={}. Start.", jobExecutionId, acceptInstanceId);
    Promise<JsonObject> promise = Promise.promise();
    try {
      client.postChangeManagerJobExecutionsRecordsById(jobExecutionId, acceptInstanceId, rawRecordsDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_NO_CONTENT) {
          LOGGER.warn("postRecordToParsing:: Failed sending record to parsing. Status message: {}",
            response.result().statusMessage());
          promise.fail(new HttpException("Failed sending record to parsing.", response.cause()));
        } else {
          LOGGER.info("postRecordToParsing:: Response: {}", response.result());
          promise.complete(response.result().bodyAsJsonObject());
        }
      });
    } catch (Exception e) {
      LOGGER.error("postRecordToParsing:: Error: {}", e.getMessage());
      promise.fail(e);
    }
    return promise.future();
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
          LOGGER.warn("getInstanceById :: Can't find Instance by InstanceId={} on tenant {}.", instanceId, tenantId);
          promise.fail(new NotFoundException(format("Can't find Instance by InstanceId=%s on tenant %s.", instanceId, tenantId)));
        } else {
          LOGGER.debug("getInstanceById :: Instance with InstanceId={} is present on tenant {}.", instanceId, tenantId);
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
    LOGGER.info("addInstance :: Publishing instance with InstanceId={} to tenant {}", instance.getId(), tenant);
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
    LOGGER.info("updateInstanceInStorage :: Updating instance with InstanceId={} on tenant {}",
      instance.getId(), tenant);
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

  private void sendCompleteEventToKafka(SharingInstance sharingInstance, SharingStatus status, String errorMessage,
                                        Map<String, String> kafkaHeaders) {

    SharingInstanceEventType evenType = CONSORTIUM_INSTANCE_SHARING_COMPLETE;

    try {

      String tenantId = kafkaHeaders.get(TENANT.toLowerCase());
      List<KafkaHeader> kafkaHeadersList = convertKafkaHeadersMap(kafkaHeaders);

      LOGGER.info("sendEventToKafka :: tenantId: {}, instance with InstanceId={}, status: {}, message: {}",
        tenantId, sharingInstance.getInstanceIdentifier(), status.getValue(), errorMessage);

      String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(),
        KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, evenType.value());

      KafkaProducerRecord<String, String> kafkaRecord = createProducerRecord(topicName, sharingInstance, status, errorMessage, kafkaHeadersList);
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

  private List<KafkaHeader> convertKafkaHeadersMap(Map<String, String> kafkaHeaders) {
    return new ArrayList<>(List.of(
      KafkaHeader.header(URL, kafkaHeaders.get(URL.toLowerCase())),
      KafkaHeader.header(TENANT, kafkaHeaders.get(TENANT.toLowerCase())),
      KafkaHeader.header(TOKEN, kafkaHeaders.get(TOKEN.toLowerCase())))
    );
  }

}
