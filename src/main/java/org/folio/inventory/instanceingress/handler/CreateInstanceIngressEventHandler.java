package org.folio.inventory.instanceingress.handler;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.apache.logging.log4j.LogManager.getLogger;
import static org.folio.inventory.dataimport.util.AdditionalFieldsUtil.reorderMarcRecordFields;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.Record;

public class CreateInstanceIngressEventHandler extends CreateInstanceEventHandler implements InstanceIngressEventHandler {
  private static final Logger LOGGER = getLogger(CreateInstanceIngressEventHandler.class);
  private final InstanceCollection instanceCollection;
  private final Context context;

  public CreateInstanceIngressEventHandler(PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                           MappingMetadataCache mappingMetadataCache,
                                           IdStorageService idStorageService,
                                           HttpClient httpClient,
                                           Context context,
                                           Storage storage) {
    super(storage, precedingSucceedingTitlesHelper, mappingMetadataCache, idStorageService, null, httpClient);
    this.instanceCollection = storage.getInstanceCollection(context);
    this.context = context;
  }

  @Override
  public CompletableFuture<Instance> handle(InstanceIngressEvent event) {
    try {
      LOGGER.info("Processing InstanceIngressEvent with id '{}' for instance creation", event.getId());
      var future = new CompletableFuture<Instance>();
      if (eventContainsNoData(event)) {
        var message = format("InstanceIngressEvent message does not contain required data to create Instance for eventId: '%s'", event.getId());
        LOGGER.error(message);
        return CompletableFuture.failedFuture(new EventProcessingException(message));
      }

      var recordId = ofNullable(event.getEventPayload().getSourceRecordIdentifier())
        .orElseGet(() -> UUID.randomUUID().toString());
      var targetRecord = constructMarcBibRecord(event.getEventPayload(), recordId);
      var instanceId = getInstanceId(event).orElseGet(() -> super.getInstanceId(targetRecord));
      idStorageService.store(targetRecord.getId(), instanceId, context.getTenantId())
        .compose(res -> getMappingMetadata(context, super::getMappingMetadataCache, LOGGER))
        .compose(metadataOptional -> metadataOptional.map(metadata -> prepareAndExecuteMapping(metadata, targetRecord, event, instanceId, LOGGER))
          .orElseGet(() -> Future.failedFuture("MappingMetadata was not found for marc-bib record type")))
        .compose(instance -> validateInstance(instance, event, LOGGER))
        .compose(instance -> saveInstance(instance, event))
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

  private Future<Instance> saveInstance(Instance instance, InstanceIngressEvent event) {
    LOGGER.info("Saving Instance from InstanceIngressEvent with id '{}': {}", event.getId(), instance);
    var targetRecord = (Record) event.getEventPayload().getAdditionalProperties().get(MARC_BIBLIOGRAPHIC.value());
    var sourceContent = targetRecord.getParsedRecord().getContent().toString();
    return super.addInstance(instance, instanceCollection)
      .compose(createdInstance -> getPrecedingSucceedingTitlesHelper().createPrecedingSucceedingTitles(instance, context).map(createdInstance))
      .compose(createdInstance -> executeFieldsManipulation(createdInstance, targetRecord,
        event.getEventPayload().getAdditionalProperties(), super::executeFieldsManipulation))
      .compose(createdInstance -> {
        var targetContent = targetRecord.getParsedRecord().getContent().toString();
        var content = reorderMarcRecordFields(sourceContent, targetContent);
        targetRecord.setParsedRecord(targetRecord.getParsedRecord().withContent(content));
        return saveRecordInSrsAndHandleResponse(event, targetRecord, createdInstance);
      });
  }

  private Future<Instance> saveRecordInSrsAndHandleResponse(InstanceIngressEvent event, Record srcRecord, Instance instance) {
    LOGGER.info("Saving record in SRS and handling a response for an Instance with id '{}':", instance.getId());
    Promise<Instance> promise = Promise.promise();
    postSnapshotInSrsAndHandleResponse(srcRecord.getSnapshotId(), context, super::postSnapshotInSrsAndHandleResponse)
      .onFailure(promise::fail)
      .compose(snapshot -> {
        getSourceStorageRecordsClient(context.getOkapiLocation(), context.getToken(), context.getTenantId(), context.getUserId())
          .postSourceStorageRecords(srcRecord)
          .onComplete(ar -> {
            var result = ar.result();
            if (ar.succeeded() &&
              result.statusCode() == HttpStatus.HTTP_CREATED.toInt()) {
              LOGGER.info("Created MARC record in SRS with id: '{}', instanceId: '{}', from tenant: {}",
                srcRecord.getId(), instance.getId(), context.getTenantId());
              promise.complete(instance);
            } else {
              String msg = format(
                "Failed to create MARC record in SRS, instanceId: '%s', status code: %s, Record: %s",
                instance.getId(), result != null ? result.statusCode() : "", result != null ? result.bodyAsString() : "");
              LOGGER.warn(msg);
              super.deleteInstance(instance.getId(), event.getId(),
                instanceCollection);
              promise.fail(msg);
            }
          });
        return promise.future();
      });
    return promise.future();
  }

}
