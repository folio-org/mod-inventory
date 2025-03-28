package org.folio.inventory.consortium.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpException;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Record;
import org.folio.inventory.client.wrappers.ChangeManagerClientWrapper;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.kafka.SimpleConfigurationReader;
import org.folio.rest.client.ChangeManagerClient;
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
import static org.folio.okapi.common.XOkapiHeaders.TENANT;
import static org.folio.okapi.common.XOkapiHeaders.TOKEN;
import static org.folio.okapi.common.XOkapiHeaders.URL;
import static org.folio.okapi.common.XOkapiHeaders.USER_ID;

/**
 * Helper class for importing MARC records with endpoints.
 */
public class RestDataImportHelper {

  private static final Logger LOGGER = LogManager.getLogger(RestDataImportHelper.class);

  public static final String FIELD_ID = "id";
  public static final String FIELD_STATUS = "status";
  public static final String FIELD_JOB_EXECUTIONS = "jobExecutions";

  public static final String STATUS_COMMITTED = "COMMITTED";
  public static final String STATUS_ERROR = "ERROR";

  private static final String IMPORT_STATUS_POLL_INTERVAL_SEC_PARAM =
    "inventory.sharing.di.status.poll.interval.seconds";
  private static final String IMPORT_STATUS_POLL_NUMBER_PARAM =
    "inventory.sharing.di.status.poll.number";
  private static final String DEFAULT_IMPORT_STATUS_POLL_INTERVAL_SEC = "5";
  private static final String DEFAULT_IMPORT_STATUS_POLL_NUMBER = "5";

  private final Vertx vertx;
  private final long durationInSec;
  private final int attemptsNumber;

  public RestDataImportHelper(Vertx vertx) {
    this.vertx = vertx;
    this.durationInSec = Integer.parseInt(SimpleConfigurationReader.getValue(
      IMPORT_STATUS_POLL_INTERVAL_SEC_PARAM, DEFAULT_IMPORT_STATUS_POLL_INTERVAL_SEC));
    this.attemptsNumber = Integer.parseInt(SimpleConfigurationReader.getValue(
      IMPORT_STATUS_POLL_NUMBER_PARAM, DEFAULT_IMPORT_STATUS_POLL_NUMBER));
  }

  public static final JobProfileInfo JOB_PROFILE_INFO = new JobProfileInfo()
    .withId("90fd4389-e5a9-4cc5-88cf-1568c0ff7e8b") //default stub id
    .withName("ECS - Create instance and SRS MARC Bib")
    .withDataType(JobProfileInfo.DataType.MARC);

  /**
   * Import MARC record to tenant.
   *
   * @param marcRecord              - MARC record
   * @param sharingInstanceMetadata - sharing instance metadata
   * @param kafkaHeaders            - kafka headers
   * @return - future with "COMMITTED" | "ERROR" or failed status
   */
  public Future<String> importMarcRecord(Record marcRecord, SharingInstance sharingInstanceMetadata, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    LOGGER.info("publishInstanceWithMarcSource:: Importing MARC record for instance with InstanceId={} to target tenant={}.",
      sharingInstanceMetadata.getInstanceIdentifier(), sharingInstanceMetadata.getTargetTenantId());

    ChangeManagerClient changeManagerClient = getChangeManagerClient(kafkaHeaders);

    return initJobExecution(instanceId, changeManagerClient, kafkaHeaders)
      .compose(jobExecutionId -> setDefaultJobProfileToJobExecution(jobExecutionId, changeManagerClient))
      .compose(jobExecutionId -> {
        RawRecordsDto dataChunk = buildDataChunk(false, singletonList(new InitialRecord().withRecord(
          getParsedRecordContentAsString(marcRecord))));
        return postChunk(jobExecutionId, true, dataChunk, changeManagerClient)
          .compose(response -> {
            //Sending empty chunk to finish import
            RawRecordsDto finishChunk = buildDataChunk(true, new ArrayList<>());
            return postChunk(jobExecutionId, false, finishChunk, changeManagerClient);
          });
      })
      .compose(jobExecutionId -> checkDataImportStatus(jobExecutionId, sharingInstanceMetadata, durationInSec, attemptsNumber, changeManagerClient));
  }

  private static String getParsedRecordContentAsString(Record marcRecord) {
    if (marcRecord.getParsedRecord().getContent() instanceof String parsedRecordContent) {
      return parsedRecordContent;
    }
    return JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).toString();
  }

  protected Future<String> initJobExecution(String instanceId, ChangeManagerClient changeManagerClient, Map<String, String> kafkaHeaders) {

    LOGGER.info("initJobExecution:: Receiving new JobExecution for sharing instance with InstanceId={} ...", instanceId);
    Promise<String> promise = Promise.promise();
    try {

      InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
        .withSourceType(InitJobExecutionsRqDto.SourceType.ONLINE)
        .withUserId(kafkaHeaders.get(USER_ID.toLowerCase()));

      changeManagerClient.postChangeManagerJobExecutions(initJobExecutionsRqDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_CREATED) {
          String errorMessage = format("Error receiving new JobExecution for sharing instance with InstanceId=%s. " +
            "Status message: %s. Status code: %s", instanceId, response.result().statusMessage(), response.result().statusCode());
          LOGGER.error("initJobExecution:: {}", errorMessage);
          promise.fail(new HttpException(errorMessage, response.cause()));
        } else {
          JsonObject responseBody = response.result().bodyAsJsonObject();
          LOGGER.trace("initJobExecution:: ResponseBody: {} for sharing instance with InstanceId={}.", responseBody, instanceId);
          JsonArray jobExecutions = responseBody.getJsonArray(FIELD_JOB_EXECUTIONS);
          LOGGER.trace("initJobExecution:: ResponseBody.JobExecutions: {} for sharing instance with InstanceId={}.", jobExecutions, instanceId);

          if (jobExecutions == null || jobExecutions.isEmpty()) {
            String errorMessage = format("Response body doesn't contains JobExecution object for sharing instance with InstanceId=%s.", instanceId);
            LOGGER.error("initJobExecution:: {}", errorMessage);
            promise.fail(errorMessage);
          } else {
            promise.complete(jobExecutions.getJsonObject(0).getString(FIELD_ID));
          }
        }
      });
    } catch (Exception ex) {
      LOGGER.error("initJobExecution:: Error init JobExecution for sharing instance with InstanceId={}. Error: {}.", instanceId, ex.getMessage());
      promise.fail(ex);
    }
    return promise.future();
  }

  protected Future<String> setDefaultJobProfileToJobExecution(String jobExecutionId, ChangeManagerClient changeManagerClient) {
    LOGGER.info("setDefaultJobProfileToJobExecution:: Linking JobProfile to JobExecution with jobExecutionId={}", jobExecutionId);
    Promise<String> promise = Promise.promise();
    try {
      changeManagerClient.putChangeManagerJobExecutionsJobProfileById(jobExecutionId, JOB_PROFILE_INFO, response -> {
        if (response.result().statusCode() != HttpStatus.SC_OK) {
          String errorMessage = format("Failed to set JobProfile for JobExecution with jobExecutionId=%s. " +
            "Status message: %s. Status code: %s", jobExecutionId, response.result().statusMessage(), response.result().statusCode());
          LOGGER.warn("setDefaultJobProfileToJobExecution:: {}", errorMessage);
          promise.fail(new HttpException(format(errorMessage, jobExecutionId), response.cause()));
        } else {
          LOGGER.trace("setDefaultJobProfileToJobExecution:: Response: {} set JobProfile for JobExecution with jobExecutionId={}.",
            response.result().bodyAsJsonObject(), jobExecutionId);
          promise.complete(jobExecutionId);
        }
      });
    } catch (Exception ex) {
      LOGGER.error(format("setDefaultJobProfileToJobExecution:: Failed to link JobProfile to JobExecution " +
        "with jobExecutionId=%s. Error: %s", jobExecutionId, ex.getCause()));
      promise.fail(ex);
    }
    return promise.future();
  }

  protected Future<String> postChunk(String jobExecutionId, Boolean shouldAcceptInstanceId,
                                   RawRecordsDto rawRecordsDto, ChangeManagerClient changeManagerClient) {
    LOGGER.info("postChunk:: Sending data for jobExecutionId={}, shouldAcceptInstanceId={}.", jobExecutionId, shouldAcceptInstanceId);
    Promise<String> promise = Promise.promise();
    try {
      changeManagerClient.postChangeManagerJobExecutionsRecordsById(jobExecutionId, shouldAcceptInstanceId, rawRecordsDto, response -> {
        if (response.result().statusCode() != HttpStatus.SC_NO_CONTENT) {
          LOGGER.error("postChunk:: Failed sending data. Status message: {}. Status code: {}.",
            response.result().statusMessage(), response.result().statusCode());
          promise.fail(new HttpException("Failed sending record data.", response.cause()));
        } else {
          LOGGER.info("postChunk:: Sending data result: {}", response.result());
          promise.complete(jobExecutionId);
        }
      });
    } catch (Exception e) {
      LOGGER.error("postChunk:: Error sending data for jobExecutionId={}. Error: {}",
        jobExecutionId, e.getMessage());
      promise.fail(e);
    }
    return promise.future();
  }

  protected Future<String> checkDataImportStatus(String jobExecutionId, SharingInstance sharingInstanceMetadata,
                                                 long durationInSec, int attemptsNumber, ChangeManagerClient changeManagerClient) {

    LOGGER.info("checkDataImportStatus:: Check import of MARC with jobExecutionId={} for sharing instance with InstanceId={}.",
      jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier());

    Promise<String> promise = Promise.promise();

    AtomicInteger counter = new AtomicInteger(0);
    try {
      checkDataImportStatusHelper(jobExecutionId, sharingInstanceMetadata, durationInSec, attemptsNumber, counter, promise, changeManagerClient);
    } catch (Exception ex) {
      String errorMessage = String.format("Error check status of MARC import with jobExecutionId=%s for sharing instance with InstanceId=%s.",
        jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier());
      LOGGER.error("checkDataImportStatus:: {}", errorMessage, ex);
      promise.fail(ex);
    }
    return promise.future();
  }

  private void checkDataImportStatusHelper(String jobExecutionId, SharingInstance sharingInstanceMetadata,
                                           long durationInSec, int attemptsNumber, AtomicInteger counter,
                                           Promise<String> promise, ChangeManagerClient changeManagerClient) {

    if (counter.getAndIncrement() > attemptsNumber) {
      String errorMessage = String.format("Number of attempts=%s to check DI status has ended for jobExecutionId=%s", counter.get(), jobExecutionId);
      LOGGER.info(errorMessage);
      promise.fail(errorMessage);
      return;
    }

    LOGGER.info("checkDataImportStatus:: Checking import status by jobExecutionId={}. InstanceId={}.",
      jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier());

    getJobExecutionStatusByJobExecutionId(jobExecutionId, changeManagerClient).onComplete(jobExecution -> {
      if (jobExecution.succeeded()) {
        String jobExecutionStatus = jobExecution.result();
        LOGGER.info("checkDataImportStatus:: Check import status for DI with jobExecutionId={}, InstanceId={}, JobExecutionStatus={}",
          jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier(), jobExecutionStatus);
        if (jobExecutionStatus.equals(STATUS_COMMITTED) || jobExecutionStatus.equals(STATUS_ERROR)) {
          promise.complete(jobExecutionStatus);
        } else {
          vertx.setTimer(TimeUnit.SECONDS.toMillis(durationInSec), timerId ->
            checkDataImportStatusHelper(jobExecutionId, sharingInstanceMetadata, durationInSec, attemptsNumber, counter, promise, changeManagerClient));
        }
      } else {
        String errorMessage = String.format("Failed get jobExecutionId=%s for DI with InstanceId=%s. Error: %s",
          jobExecutionId, sharingInstanceMetadata.getInstanceIdentifier(), jobExecution.cause().getMessage());
        LOGGER.error("checkDataImportStatus:: {}", errorMessage);
        promise.fail(errorMessage);
      }
    });
  }

  protected Future<String> getJobExecutionStatusByJobExecutionId(String jobExecutionId, ChangeManagerClient changeManagerClient) {

    LOGGER.info("getJobExecutionStatusByJobExecutionId:: Getting jobExecution by jobExecutionId={}.", jobExecutionId);
    Promise<String> promise = Promise.promise();
    try {
      changeManagerClient.getChangeManagerJobExecutionsById(jobExecutionId, response -> {
        if (response.result().statusCode() != HttpStatus.SC_OK) {
          String errorMessage = format("Error getting jobExecution by jobExecutionId=%s. " +
              "Status message: %s. Status code: %s", jobExecutionId, response.result().statusMessage(),
            response.result().statusCode());
          LOGGER.error("getJobExecutionStatusByJobExecutionId:: {}", errorMessage);
          promise.fail(errorMessage);
        } else {
          LOGGER.trace("getJobExecutionStatusByJobExecutionId:: Response: {}", response.result().bodyAsJsonObject());
          if (response.result().bodyAsJsonObject() == null || response.result().bodyAsJsonObject().isEmpty()) {
            String errorMessage = format("Response body doesn't contains data for jobExecutionId=%s.", jobExecutionId);
            LOGGER.error("getJobExecutionStatusByJobExecutionId:: {}", errorMessage);
            promise.fail(errorMessage);
          } else {
            promise.complete(response.result().bodyAsJsonObject().getString(FIELD_STATUS));
          }
        }
      });
    } catch (Exception ex) {
      LOGGER.error("getJobExecutionStatusByJobExecutionId:: Error getting jobExecution by jobExecutionId={}.", jobExecutionId, ex);
      promise.fail(ex);
    }
    return promise.future();
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

  public ChangeManagerClient getChangeManagerClient(Map<String, String> kafkaHeaders) {
    return new ChangeManagerClientWrapper(
      kafkaHeaders.get(URL.toLowerCase()),
      kafkaHeaders.get(TENANT.toLowerCase()),
      kafkaHeaders.get(TOKEN.toLowerCase()),
      kafkaHeaders.get(USER_ID.toLowerCase()),
      vertx.createHttpClient());
  }

}
