package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.Map;

import static java.lang.String.format;

public class InstanceUpdateDelegate {

  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String MARC_FORMAT = "MARC";
  private static final String STATISTICAL_CODE_IDS_PROPERTY = "statisticalCodeIds";
  private static final String NATURE_OF_CONTENT_TERM_IDS_PROPERTY = "natureOfContentTermIds";

  private Storage storage;

  public InstanceUpdateDelegate(Storage storage) {
    this.storage = storage;
  }

  public Future<Instance> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    Future<Instance> future = Future.future();
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      org.folio.Instance mappedInstance = RecordToInstanceMapperBuilder.buildMapper(MARC_FORMAT).mapRecord(parsedRecord, mappingParameters, mappingRules);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      getInstanceById(instanceId, instanceCollection)
        .compose(existingInstance -> updateInstance(existingInstance, mappedInstance))
        .compose(updatedInstance -> updateInstanceInStorage(updatedInstance, instanceCollection))
        .setHandler(future::handle);
    } catch (Exception e) {
      LOGGER.error("Error updating inventory instance", e);
      future.fail(e);
    }
    return future;
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
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
      JsonObject mergedInstanceAsJson = mergeInstances(existing, mapped);
      Instance mergedInstance = InstanceUtil.jsonToInstance(mergedInstanceAsJson);
      future.complete(mergedInstance);
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      future.fail(e);
    }
    return future;
  }

  private JsonObject mergeInstances(JsonObject existing, JsonObject mapped) {
    //Statistical code and nature of content terms don`t revealed via mergeIn() because of simple array type.
    JsonArray statisticalCodeIds = existing.getJsonArray(STATISTICAL_CODE_IDS_PROPERTY);
    JsonArray natureOfContentTermIds = existing.getJsonArray(NATURE_OF_CONTENT_TERM_IDS_PROPERTY);
    JsonObject mergedInstanceAsJson = existing.mergeIn(mapped);
    mergedInstanceAsJson.put(STATISTICAL_CODE_IDS_PROPERTY, statisticalCodeIds);
    mergedInstanceAsJson.put(NATURE_OF_CONTENT_TERM_IDS_PROPERTY, natureOfContentTermIds);
    return mergedInstanceAsJson;
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
