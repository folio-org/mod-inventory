package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersUpdateDelegate;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;

public class InstanceUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(InstanceUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String QM_RELATED_RECORD_VERSION_KEY = "RELATED_RECORD_VERSION";

  private final Storage storage;

  public InstanceUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<Instance> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      LOGGER.info("Instance update with instanceId: {}", instanceId);
      RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
      var mappedInstance = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      return InstanceUtil.findInstanceById(instanceId, instanceCollection)
        .onSuccess(existingInstance -> {
          LOGGER.info("handleInstanceUpdate:: current version: {}", existingInstance.getVersion());
          fillVersion(existingInstance, eventPayload);
        })
        .compose(existingInstance -> {
          LOGGER.info("handleInstanceUpdate:: version before mapping: {}", existingInstance.getVersion());
          instanceCollection.findByIdAndUpdate(instanceId, existingInstance, context);
          LOGGER.info("EXISTING: {}", existingInstance);
          return Future.fromCompletionStage(updateInstance(existingInstance, mappedInstance));
        })
        .compose(updatedInstance -> {
          LOGGER.info("handleInstanceUpdate:: version before update: {}", updatedInstance.getVersion());
          return updateInstanceInStorage(updatedInstance, instanceCollection);
        });
    } catch (Exception e) {
      LOGGER.error("Error updating inventory instance", e);
      return Future.failedFuture(e);
    }
  }

  public Future<Instance> handleBlocking(Map<String, String> eventPayload, Record marcRecord, Context context) {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
//    Promise<Instance> promise = Promise.promise();
//    io.vertx.core.Context vertxContext = Vertx.currentContext();
//
//    if(vertxContext == null) {
//      return Future.failedFuture("handle:: operation must be executed by a Vertx thread");
//    }
//
//    vertxContext.owner().executeBlocking(() -> {
        try {
          JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
          MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
          JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
          String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
          LOGGER.info("Instance update with instanceId: {}", instanceId);
          RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
          var mappedInstance = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
          InstanceCollection instanceCollection = storage.getInstanceCollection(context);

          var existing = instanceCollection.findById(instanceId).get(5, TimeUnit.SECONDS);
          if (existing == null) {
            return Future.failedFuture(new NotFoundException(format("Can't find Instance by id: %s", instanceId)));
          }
          var modified = updateInstance(existing, mappedInstance).get(5, TimeUnit.SECONDS);
          var updated = instanceCollection.update(modified).get(5, TimeUnit.SECONDS);
          return Future.succeededFuture(updated);

//          CompletableFuture<Instance> getFuture = new CompletableFuture<>();
//          instanceCollection.findById(instanceId, success -> {
//              if (success.getResult() == null) {
//                LOGGER.warn("findInstanceById:: Can't find Instance by id: {} ", instanceId);
//                var ex = new NotFoundException(format("Can't find Instance by id: %s", instanceId));
//                getFuture.completeExceptionally(ex);
//              } else {
//                LOGGER.info("handleInstanceUpdate:: current version: {}, jobId: {}", success.getResult().getVersion(), marcRecord.getSnapshotId());
//                getFuture.complete(success.getResult());
//              }
//            },
//            failure -> {
//              LOGGER.warn(format("findInstanceById:: Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
//              var ex = new ExternalResourceFetchException(format("Instance fetch by id: %s failed", instanceId), failure.getReason(), failure.getStatusCode(), null);
//              getFuture.completeExceptionally(ex);
//            });
//
//          return getFuture.thenCompose(existing -> updateInstance(existing, mappedInstance))
//            .thenCompose(modified -> {
//              LOGGER.info("handleInstanceUpdate:: version before update: {}, jobId: {}", modified.getVersion(), marcRecord.getSnapshotId());
//              CompletableFuture<Instance> updateFuture = new CompletableFuture<>();
//              instanceCollection.update(modified,
//                success -> updateFuture.complete(modified),
//                failure -> {
//                  if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
//                    var ex = new OptimisticLockingException(failure.getReason());
//                    updateFuture.completeExceptionally(ex);
//                  } else {
//                    LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
//                    var ex = new ExternalResourceFetchException(format("Error updating Instance - %s", failure.getReason()), failure.getReason(), failure.getStatusCode(), null);
//                    updateFuture.completeExceptionally(ex);
//                  }
//                });
//              return updateFuture;
//            })
//            .get(2, TimeUnit.SECONDS);
          //return Future.succeededFuture(updatedInstance);
        } catch (Exception ex) {
          LOGGER.error("Error updating inventory instance: {}", ex.getMessage());
          return Future.failedFuture(ex);
          //throw ex;
        }
//      },
//      r -> {
//        if (r.failed()) {
//          LOGGER.warn("handle:: Error during instance save", r.cause());
//          promise.fail(r.cause());
//        } else {
//          LOGGER.debug("saveRecords:: Instance save was successful");
//          promise.complete(r.result());
//        }
//      });
//    return promise.future();
  }

  private void fillVersion(Instance existingInstance, Map<String, String> eventPayload) {
    if (eventPayload.containsKey(QM_RELATED_RECORD_VERSION_KEY)) {
      existingInstance.setVersion(eventPayload.get(QM_RELATED_RECORD_VERSION_KEY));
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private CompletableFuture<Instance> updateInstance(Instance existingInstance, org.folio.Instance mappedInstance) {
    try {
      mappedInstance.setId(existingInstance.getId());
      JsonObject existing = JsonObject.mapFrom(existingInstance);
      JsonObject mapped = JsonObject.mapFrom(mappedInstance);
      JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existing, mapped);
      Instance mergedInstance = Instance.fromJson(mergedInstanceAsJson);
      return CompletableFuture.completedFuture(mergedInstance);
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      return CompletableFuture.failedFuture(e);
    }
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
}
