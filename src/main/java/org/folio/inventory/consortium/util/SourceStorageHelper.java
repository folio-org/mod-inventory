package org.folio.inventory.consortium.util;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.Record;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.inventory.consortium.exceptions.StorageOperationException;
import org.folio.rest.client.SourceStorageRecordsClient;

public class SourceStorageHelper {

  /**
   * Value of the {@code idType} query parameter accepted by the SRS records endpoints: which identifier the
   * given id refers to.
   */
  public enum IdType {
    INSTANCE,
    SRS_RECORD
  }

  private static final Logger LOGGER = LogManager.getLogger(SourceStorageHelper.class);

  private static final String GETTING_SOURCE_MARC_RECORD_MSG =
    "getSourceMarcByInstanceId:: Getting source MARC record for instance with InstanceId={} from tenant={}.";
  private static final String MARC_SOURCE_RETRIEVED_MSG =
    "MARC source for instance with InstanceId={} from tenant={}.";
  private static final String FAILED_TO_RETRIEVE_MARC_RECORD_STATUS_MSG =
    "Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. Status code: %s";
  private static final String FAILED_TO_RETRIEVE_MARC_RECORD_ERROR_MSG =
    "Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s";
  private static final String DELETING_SOURCE_RECORD_MSG =
    "deleteSourceRecord:: Delete source record by id={} with idType={} from tenant {}";
  private static final String ERROR_DELETING_SOURCE_RECORD_MSG =
    "deleteSourceRecord:: Error deleting source record by id={} with idType={} from tenant {}";
  private static final String SOURCE_RECORD_DELETED_MSG =
    "deleteSourceRecord:: Source record by id={} with idType={} from tenant {} has been deleted.";
  private static final String ERROR_DELETING_SOURCE_RECORD_DETAILS_MSG =
    "Error deleting source record by id=%s with idType=%s from tenant %s, responseStatus=%s";
  private static final String DELETE_SOURCE_RECORD_ERROR_PREFIX_MSG = "deleteSourceRecord:: {}";
  private static final String UNDELETING_SOURCE_RECORD_MSG =
    "unDeleteSourceRecord:: Un-delete source record by id={} with idType={} from tenant {}";
  private static final String ERROR_UNDELETING_SOURCE_RECORD_MSG =
    "unDeleteSourceRecord:: Error un-deleting source record by id={} with idType={} from tenant {}";
  private static final String SOURCE_RECORD_UNDELETED_MSG =
    "unDeleteSourceRecord:: Source record by id={} with idType={} from tenant {} has been un-deleted.";
  private static final String ERROR_UNDELETING_SOURCE_RECORD_DETAILS_MSG =
    "Error un-deleting source record by id=%s with idType=%s from tenant %s, responseStatus=%s";
  private static final String UNDELETE_SOURCE_RECORD_ERROR_PREFIX_MSG = "unDeleteSourceRecord:: {}";
  private static final String UPDATING_SUPPRESS_FROM_DISCOVERY_MSG =
    "updateSourceRecordSuppressFromDiscovery:: Updating suppress from discovery flag for record in SRS, "
    + "instanceId: {}, suppressFromDiscovery: {}";
  private static final String SUPPRESS_FROM_DISCOVERY_UPDATED_MSG =
    "updateSourceRecordSuppressFromDiscovery:: Suppress from discovery flag was successfully updated for record "
    + "in SRS, instanceId: {}, suppressFromDiscovery: {}";
  private static final String CANNOT_UPDATE_SUPPRESS_FROM_DISCOVERY_MSG =
    "Cannot update suppress from discovery flag for SRS record, instanceId: %s, statusCode: %s, "
    + "suppressFromDiscovery: %s";
  private static final String SUPPRESS_FROM_DISCOVERY_ERROR_PREFIX_MSG = "updateSourceRecordSuppressFromDiscovery:: {}";
  private static final String CREATING_SOURCE_STORAGE_RECORDS_CLIENT_MSG =
    "getSourceStorageRecordsClient:: Creating SourceStorageRecordsClient for tenant={}";
  private final HttpClient httpClient;

  public SourceStorageHelper(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public Future<Record> getSourceRecordByInstanceId(String instanceId, String tenantId, Map<String, String> headers) {

    LOGGER.info(GETTING_SOURCE_MARC_RECORD_MSG, instanceId, tenantId);

    return prepareClient(tenantId, headers)
      .getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE)
      .compose(response -> {
        int statusCode = response.statusCode();
        if (statusCode == HttpStatus.SC_OK) {
          LOGGER.debug(MARC_SOURCE_RETRIEVED_MSG, instanceId, tenantId);
          return Future.succeededFuture(response.bodyAsJson(Record.class));
        } else {
          String errorMessage = format(FAILED_TO_RETRIEVE_MARC_RECORD_STATUS_MSG, instanceId, tenantId, statusCode);
          LOGGER.error(errorMessage);
          return Future.failedFuture(new StorageOperationException(errorMessage, statusCode));
        }
      }, throwable -> {
        String errorMessage = format(FAILED_TO_RETRIEVE_MARC_RECORD_ERROR_MSG, instanceId, tenantId);
        LOGGER.error(errorMessage, throwable);
        return Future.failedFuture(throwable);
      });
  }

  /**
   * Soft-deletes the source record resolved by the given id.
   *
   * @return future completed with the id that was passed in
   */
  public Future<String> deleteSourceRecord(String id, IdType idType, String tenantId, Map<String, String> headers) {
    LOGGER.info(DELETING_SOURCE_RECORD_MSG, id, idType, tenantId);

    return prepareClient(tenantId, headers)
      .deleteSourceStorageRecordsById(id, idType.name())
      .onFailure(e -> LOGGER.error(ERROR_DELETING_SOURCE_RECORD_MSG, id, idType, tenantId, e))
      .compose(response -> {
        var statusCode = response.statusCode();
        if (statusCode == HttpStatus.SC_NO_CONTENT) {
          LOGGER.info(SOURCE_RECORD_DELETED_MSG, id, idType, tenantId);
          return Future.succeededFuture(id);
        } else {
          String msg = format(ERROR_DELETING_SOURCE_RECORD_DETAILS_MSG, id, idType, tenantId, statusCode);
          LOGGER.error(DELETE_SOURCE_RECORD_ERROR_PREFIX_MSG, msg);
          return Future.failedFuture(new StorageOperationException(msg, statusCode));
        }
      });
  }

  /**
   * Reverts {@link #deleteSourceRecord(String, IdType, String, Map)}. Safe to call on a record that is not deleted.
   *
   * @return future completed with the id that was passed in
   */
  public Future<String> unDeleteSourceRecord(String id, IdType idType, String tenantId, Map<String, String> headers) {
    LOGGER.info(UNDELETING_SOURCE_RECORD_MSG, id, idType, tenantId);

    return prepareClient(tenantId, headers)
      .postSourceStorageRecordsUnDeleteById(id, idType.name())
      .onFailure(e -> LOGGER.error(ERROR_UNDELETING_SOURCE_RECORD_MSG, id, idType, tenantId, e))
      .compose(response -> {
        var statusCode = response.statusCode();
        if (statusCode == HttpStatus.SC_NO_CONTENT) {
          LOGGER.info(SOURCE_RECORD_UNDELETED_MSG, id, idType, tenantId);
          return Future.succeededFuture(id);
        } else {
          String msg = format(ERROR_UNDELETING_SOURCE_RECORD_DETAILS_MSG, id, idType, tenantId, statusCode);
          LOGGER.error(UNDELETE_SOURCE_RECORD_ERROR_PREFIX_MSG, msg);
          return Future.failedFuture(new StorageOperationException(msg, statusCode));
        }
      });
  }

  public Future<String> updateSourceRecordSuppressFromDiscovery(String instanceId, boolean suppress, String tenantId,
                                                                Map<String, String> headers) {
    LOGGER.info(UPDATING_SUPPRESS_FROM_DISCOVERY_MSG, instanceId, suppress);

    return prepareClient(tenantId, headers)
      .putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, suppress)
      .compose(response -> {
        var statusCode = response.statusCode();
        if (statusCode == HttpStatus.SC_OK) {
          LOGGER.info(SUPPRESS_FROM_DISCOVERY_UPDATED_MSG, instanceId, suppress);
          return Future.succeededFuture(instanceId);
        } else {
          String errorMessage = format(CANNOT_UPDATE_SUPPRESS_FROM_DISCOVERY_MSG, instanceId, statusCode, suppress);
          LOGGER.error(SUPPRESS_FROM_DISCOVERY_ERROR_PREFIX_MSG, errorMessage);
          return Future.failedFuture(new StorageOperationException(errorMessage, statusCode));
        }
      });
  }

  protected SourceStorageRecordsClient prepareClient(String tenant, Map<String, String> headers) {
    LOGGER.debug(CREATING_SOURCE_STORAGE_RECORDS_CLIENT_MSG, tenant);
    var folioHeaders = FolioHeaders.from(headers).tenant(tenant);
    return new SourceStorageRecordsClientWrapper(folioHeaders, httpClient);
  }
}
