package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Record;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.rest.client.SourceStorageRecordsClient;

import java.util.Map;

import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.SOURCE;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;

public class MarcInstanceSharingHandlerImpl implements InstanceSharingHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcInstanceSharingHandlerImpl.class);

  private final RestDataImportHelper restDataImportHelper;

  private final InstanceOperationsHelper instanceOperations;

  private final Vertx vertx;

  public MarcInstanceSharingHandlerImpl(InstanceOperationsHelper instanceOperations, Vertx vertx) {
    this.vertx = vertx;
    this.instanceOperations = instanceOperations;
    this.restDataImportHelper = new RestDataImportHelper(vertx);
  }

  public Future<String>  publishInstance(Instance instance, SharingInstance sharingInstanceMetadata,
                                        Source source, Target target, Map<String, String> kafkaHeaders) {

    String instanceId = sharingInstanceMetadata.getInstanceIdentifier().toString();
    String sourceTenant = sharingInstanceMetadata.getSourceTenantId();

    SourceStorageRecordsClient sourceStorageClient = getSourceStorageRecordsClient(sourceTenant, kafkaHeaders);

    // Get source MARC by instance ID
    return getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient)
      .compose(marcRecord -> {
        // Publish instance with MARC source
        return restDataImportHelper.importMarcRecord(marcRecord, sharingInstanceMetadata, kafkaHeaders)
          .compose(result -> {
            if ("COMMITTED".equals(result)) {
              // Delete source record by instance ID if the result is "COMMITTED"
              return deleteSourceRecordByInstanceId(marcRecord.getId(), instanceId, sourceTenant, sourceStorageClient)
                .compose(deletedInstanceId -> {
                  // Update JSON instance to include SOURCE=CONSORTIUM-MARC
                  JsonObject jsonInstanceToPublish = instance.getJsonForStorage();
                  jsonInstanceToPublish.put(SOURCE, CONSORTIUM_MARC.getValue());
                  // Update instance in sourceInstanceCollection
                  return instanceOperations.updateInstance(Instance.fromJson(jsonInstanceToPublish), source);
                });
            } else {
              // If the result is not "COMMITTED", skip the deletion and update steps and return the result directly
              return Future.failedFuture(String.format("DI status is %s", result));
            }
          });
      });
  }

  Future<Record> getSourceMARCByInstanceId(String instanceId, String sourceTenant, SourceStorageRecordsClient client) {

    LOGGER.info("getSourceMARCByInstanceId:: Getting source MARC record for instance with InstanceId={} from tenant={}.",
      instanceId, sourceTenant);

    Promise<Record> promise = Promise.promise();
    client.getSourceStorageRecordsFormattedById(instanceId, INSTANCE_ID_TYPE).onComplete(responseResult -> {
        try {
          if (responseResult.succeeded()) {
            int statusCode = responseResult.result().statusCode();
            if (statusCode == HttpStatus.SC_OK) {
              String bodyAsString = responseResult.result().bodyAsString();
              LOGGER.debug("MARC source for instance with InstanceId={} from tenant={}. Record={}.", instanceId, sourceTenant, bodyAsString);
              promise.complete(responseResult.result().bodyAsJson(Record.class));
            } else {
              String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
                "Status message: %s. Status code: %s", instanceId, sourceTenant, responseResult.result().statusMessage(), statusCode);
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            }
          } else {
            String errorMessage = String.format("Failed to retrieve MARC record for instance with InstanceId=%s from tenant=%s. " +
              "Error message: %s", instanceId, sourceTenant, responseResult.cause().getMessage());
            LOGGER.error(errorMessage);
            promise.fail(responseResult.cause());
          }
        } catch (Exception ex) {
          LOGGER.error("Error processing MARC record retrieval.", ex);
          promise.fail("Error processing MARC record retrieval.");
        }
      });
    return promise.future();
  }

  Future<String> deleteSourceRecordByInstanceId(String recordId, String instanceId, String tenantId, SourceStorageRecordsClient client) {
    LOGGER.info("deleteSourceRecordByInstanceId :: Delete source record with recordId={} for instance by InstanceId={} from tenant {}",
      recordId, instanceId, tenantId);
    Promise<String> promise = Promise.promise();
    client.deleteSourceStorageRecordsById(recordId).onComplete(responseResult -> {
      try {
        if (responseResult.failed()) {
          LOGGER.error("deleteSourceRecordByInstanceId:: Error deleting source record with recordId={} by InstanceId={} from tenant {}",
            recordId, instanceId, tenantId, responseResult.cause());
          promise.fail(responseResult.cause());
        } else {
          LOGGER.info("deleteSourceRecordByInstanceId:: Source record with recordId={} for instance with InstanceId={} from tenant {} has been deleted.",
            recordId, instanceId, tenantId);
          promise.complete(instanceId);
        }
      } catch (Exception ex) {
        String errorMessage = String.format("Error processing source record with recordId={} deletion for instance with InstanceId=%s from tenant=%s. Error message: %s",
          recordId, instanceId, tenantId, responseResult.cause());
        LOGGER.error("deleteSourceRecordByInstanceId:: {}", errorMessage, ex);
        promise.fail(errorMessage);
      }
    });
    return promise.future();
  }

  public SourceStorageRecordsClient getSourceStorageRecordsClient(String tenant, Map<String, String> kafkaHeaders) {
    LOGGER.info("getSourceStorageRecordsClient :: Creating SourceStorageRecordsClient for tenant={}", tenant);
    return new SourceStorageRecordsClient(
      kafkaHeaders.get(OKAPI_URL_HEADER),
      tenant,
      kafkaHeaders.get(OKAPI_TOKEN_HEADER),
      vertx.createHttpClient());
  }

}
