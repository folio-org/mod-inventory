package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class UpdateInstanceEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateInstanceEventHandler.class);
  private static final String MARC_KEY = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  private Storage storage;
  private Context context;

  public UpdateInstanceEventHandler(Storage storage, Context context) {
    this.storage = storage;
    this.context = context;
  }

  public CompletableFuture<Instance> handle(HashMap<String, String> eventPayload) {
    CompletableFuture<Instance> future = new CompletableFuture<>();
    try {
      if (eventPayload == null || isEmpty(eventPayload.get(MARC_KEY)) || isEmpty(eventPayload.get(MAPPING_RULES_KEY)) || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY))) {
        String message = "Event does not contain required data to update Instance";
        LOGGER.error(message);
        future.completeExceptionally(new EventProcessingException(message));
      }
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);

      JsonObject parsedRecord = JsonObject.mapFrom(marcRecord.getParsedRecord().getContent());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      org.folio.Instance mappedInstance = RecordToInstanceMapperBuilder.buildMapper(MARC_KEY).mapRecord(parsedRecord, mappingParameters, mappingRules);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      getInstanceById(instanceId, instanceCollection)
        .compose(existingInstance -> updateInstance(existingInstance, mappedInstance))
        .compose(updatedInstance -> updateInstanceInStorage(updatedInstance, instanceCollection))
      .setHandler(ar -> {
        if (ar.succeeded()) {
          future.complete(ar.result());
        } else {
          future.completeExceptionally(ar.cause());
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to update Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private Future<Instance> getInstanceById(String instanceId, InstanceCollection instanceCollection) {
    Future<Instance> future = Future.future();
    instanceCollection.findById(instanceId, success -> future.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
        future.fail(failure.getReason());
      });
    return future;
  }

  private Future<Instance> updateInstance(Instance existingInstance, org.folio.Instance mappedInstance) {
    Future<Instance> future = Future.future();
    try {
      mappedInstance.setId(existingInstance.getId());
      JsonObject existing = JsonObject.mapFrom(existingInstance);
      JsonObject mapped = JsonObject.mapFrom(mappedInstance);
      Instance mergedInstance = InstanceUtil.jsonToInstance(existing.mergeIn(mapped));
      future.complete(mergedInstance);
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      future.fail(e);
    }
    return future;
  }

  private Future<Instance> updateInstanceInStorage(Instance instance, InstanceCollection instanceCollection) {
    Future<Instance> future = Future.future();
    instanceCollection.update(instance, success -> future.complete(instance),
      failure -> {
        LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
        future.fail(failure.getReason());
      });
    return future;
  }
}
