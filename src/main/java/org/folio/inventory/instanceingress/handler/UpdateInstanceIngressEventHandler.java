package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.TAG_999;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.dataimport.util.AdditionalFieldsUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.Record;

public class UpdateInstanceIngressEventHandler extends ReplaceInstanceEventHandler implements InstanceIngressEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateInstanceIngressEventHandler.class);
  private final InstanceCollection instanceCollection;
  private final Context context;
  private final SnapshotService snapshotService;

  public UpdateInstanceIngressEventHandler(PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                           MappingMetadataCache mappingMetadataCache,
                                           HttpClient client,
                                           Context context,
                                           Storage storage,
                                           SnapshotService snapshotService) {
    super(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, client, null, snapshotService, null);
    this.instanceCollection = storage.getInstanceCollection(context);
    this.context = context;
    this.snapshotService = snapshotService;
  }

  @Override
  public CompletableFuture<Instance> handle(InstanceIngressEvent event) {
    try {
      LOGGER.info("Processing InstanceIngressEvent with id '{}' for instance update", event.getId());
      var future = new CompletableFuture<Instance>();
      if (eventContainsNoData(event)) {
        var message = format("InstanceIngressEvent message does not contain required data to update Instance for eventId: '%s'", event.getId());
        LOGGER.error(message);
        return CompletableFuture.failedFuture(new EventProcessingException(message));
      }

      var recordId = UUID.randomUUID().toString();
      var targetRecord = constructMarcBibRecord(event.getEventPayload(), recordId);
      var instanceId = getInstanceId(event).orElseGet(() -> super.getInstanceId(targetRecord));
      getMappingMetadata(context, super::getMappingMetadataCache)
        .compose(metadataOptional -> metadataOptional.map(metadata -> prepareAndExecuteMapping(metadata, targetRecord, event, instanceId, LOGGER))
          .orElseGet(() -> Future.failedFuture("MappingMetadata was not found for marc-bib record type")))
        .compose(newInstance -> fillPreviousInstanceData(newInstance, instanceId))
        .compose(newInstance -> validateInstance(newInstance, event, LOGGER))
        .compose(mappedInstance -> processInstanceUpdate(mappedInstance, event))
        .onFailure(e -> {
          if (!(e instanceof DuplicateEventException)) {
            LOGGER.error(FAILURE, event.getId(), e);
          }
          future.completeExceptionally(e);
        })
        .onComplete(ar -> future.complete(ar.result()));
      return future;
    } catch (Exception e) {
      LOGGER.error(FAILURE, event.getId(), e);
      return CompletableFuture.failedFuture(e);
    }
  }

  private Future<org.folio.Instance> fillPreviousInstanceData(org.folio.Instance instance, String instanceId) {
    return InstanceUtil.findInstanceById(instanceId, instanceCollection)
      .compose(existingInstance -> {
        instance.setHrid(existingInstance.getHrid());
        instance.setVersion(Integer.parseInt(existingInstance.getVersion()));
        return Future.succeededFuture(instance);
      })
      .onFailure(e -> {
        var message = "Error retrieving inventory Instance";
        LOGGER.error(message, e);
      });
  }

  private Future<Instance> processInstanceUpdate(Instance instance, InstanceIngressEvent event) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
      LOGGER.error("Error updating Instance - {}}, status code {}, eventId - {}",
        failure.getReason(), failure.getStatusCode(), event.getId());
      promise.fail(failure.getReason());
      });
    var targetRecord = (Record) event.getEventPayload().getAdditionalProperties().get(MARC_BIBLIOGRAPHIC.value());
    var sourceContent = targetRecord.getParsedRecord().getContent().toString();
    return promise.future()
      .compose(updatedInstance -> getPrecedingSucceedingTitlesHelper().getExistingPrecedingSucceedingTitles(instance, context))
      .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
        .map(titleJson -> titleJson.getString("id"))
        .collect(Collectors.toSet()))
      .compose(titlesIds -> getPrecedingSucceedingTitlesHelper().deletePrecedingSucceedingTitles(titlesIds, context))
      .map(instance)
      .compose(updatedInstance -> executeFieldsManipulation(updatedInstance, targetRecord,
        event.getEventPayload().getAdditionalProperties(), super::executeFieldsManipulation))
      .compose(updatedInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var content = reorderMarcRecordFields(sourceContent, targetContent);
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
        return putRecordInSrsAndHandleResponse(targetRecord, updatedInstance);
      })
      .compose(ar -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(instance, context))
      .map(instance);
  }

  private Future<Instance> putRecordInSrsAndHandleResponse(Record targetRecord, Instance instance) {
    Promise<Instance> promise = Promise.promise();
    var sourceStorageRecordsClient = getSourceStorageRecordsClient(context.getOkapiLocation(), context.getToken(), context.getTenantId(), context.getUserId());
    postSnapshotInSrsAndHandleResponse(targetRecord.getSnapshotId(), context, snapshotService::postSnapshotInSrsAndHandleResponse)
      .onFailure(promise::fail)
      .compose(snapshot -> super.getRecordByInstanceId(sourceStorageRecordsClient, instance.getId()))
      .compose(existingRecord -> {
        targetRecord.setMatchedId(existingRecord.getMatchedId());
        if (nonNull(existingRecord.getGeneration())) {
          int incrementedGeneration = existingRecord.getGeneration();
          targetRecord.setGeneration(++incrementedGeneration);
        }
        AdditionalFieldsUtil.addFieldToMarcRecord(targetRecord, TAG_999, 's', targetRecord.getMatchedId());
        return Future.succeededFuture(targetRecord.getMatchedId());
      })
      .compose(matchedId ->
        sourceStorageRecordsClient.putSourceStorageRecordsGenerationById(matchedId, targetRecord)
          .onComplete(ar -> {
            var result = ar.result();
            if (ar.succeeded() &&
              result.statusCode() == HttpStatus.HTTP_OK.toInt()) {
              LOGGER.info("Update MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}",
                targetRecord.getId(), instance.getId(), context.getTenantId());
              promise.complete(instance);
            } else {
              String msg = format("Failed to update MARC record in SRS, instanceId: '%s', status code: %s, result: %s",
                instance.getId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
              LOGGER.warn(msg);
              promise.fail(msg);
            }
          })
      );
    return promise.future();
  }

}
