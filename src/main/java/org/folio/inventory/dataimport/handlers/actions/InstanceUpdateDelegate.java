package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OptimisticLockingException;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import static java.lang.String.format;
import static org.apache.commons.lang3.BooleanUtils.isNotTrue;
import static org.apache.commons.lang3.BooleanUtils.isTrue;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersUpdateDelegate;
import static org.folio.inventory.dataimport.util.MappingConstants.MARC_BIB_RECORD_FORMAT;

public class InstanceUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(InstanceUpdateDelegate.class);

  public static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  public static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  public static final String QM_RELATED_RECORD_VERSION_KEY = "RELATED_RECORD_VERSION";

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
      LOGGER.info("handle:: Parsed record content: {}", parsedRecord.encodePrettily());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      LOGGER.info("Instance update with instanceId: {}", instanceId);
      RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
      var mappedInstance = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      return InstanceUtil.findInstanceById(instanceId, instanceCollection)
        .onSuccess(existingInstance -> fillVersion(existingInstance, eventPayload))
        .compose(existingInstance -> updateInstance(existingInstance, mappedInstance))
        .compose(updatedInstance -> updateInstanceInStorage(updatedInstance, instanceCollection));
    } catch (Exception e) {
      LOGGER.error("Error updating inventory instance", e);
      return Future.failedFuture(e);
    }
  }

  public Instance handleBlocking(Map<String, String> eventPayload, Record marcRecord, Context context) throws Exception {
    logParametersUpdateDelegate(LOGGER, eventPayload, marcRecord, context);
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      LOGGER.info("Instance update with instanceId: {}", instanceId);
      RecordMapper<org.folio.Instance> recordMapper = RecordMapperBuilder.buildMapper(MARC_BIB_RECORD_FORMAT);
      var mappedInstance = recordMapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
      var modified = JsonObject.mapFrom(mappedInstance);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);

      return instanceCollection.findByIdAndUpdate(instanceId, modified, context);
    } catch (Exception ex) {
      LOGGER.error("Error updating inventory instance: {}", ex.getMessage());
      throw ex;
    }
  }

  private void fillVersion(Instance existingInstance, Map<String, String> eventPayload) {
    LOGGER.info("fillVersion:: Filling version for existing instance with id: {}", existingInstance.getId());
    if (eventPayload.containsKey(QM_RELATED_RECORD_VERSION_KEY)) {
      LOGGER.info("fillVersion:: Setting version for existing instance with id: {} to {}", existingInstance.getId(),
        eventPayload.get(QM_RELATED_RECORD_VERSION_KEY));
      existingInstance.setVersion(eventPayload.get(QM_RELATED_RECORD_VERSION_KEY));
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private Future<Instance> updateInstance(Instance existingInstance, org.folio.Instance mappedInstance) {
    LOGGER.info("updateInstance:: Updating instance with id: {}", existingInstance.getId());
    try {
      mappedInstance.setId(existingInstance.getId());
      LOGGER.info("updateInstance:: existingInstance isDeleted: {}, mappedInstance isDeleted: {}",
        existingInstance.getDeleted(), mappedInstance.getDeleted());
      if (isNotTrue(existingInstance.getDeleted()) && isTrue(mappedInstance.getDeleted())) {
        LOGGER.info("updateInstance:: Instance is marked as deleted, setting discovery and staff suppress to true");
        mappedInstance.withDiscoverySuppress(true);
        mappedInstance.withStaffSuppress(true);
      } else {
        LOGGER.info("updateInstance:: existingInstance discoverySuppress: {}, staffSuppress: {}",
          existingInstance.getDiscoverySuppress(), existingInstance.getStaffSuppress());
        mappedInstance.withStaffSuppress(existingInstance.getStaffSuppress());
        mappedInstance.withDiscoverySuppress(existingInstance.getDiscoverySuppress());
      }

      JsonObject existing = JsonObject.mapFrom(existingInstance);
      LOGGER.info("updateInstance:: existing instance: {}", existing.encodePrettily());
      JsonObject mapped = JsonObject.mapFrom(mappedInstance);
      LOGGER.info("updateInstance:: mapped instance: {}", mapped.encodePrettily());
      JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existing, mapped);
      LOGGER.info("updateInstance:: mergedInstanceAsJson instance: {}", mergedInstanceAsJson.encodePrettily());

      Instance mergedInstance = Instance.fromJson(mergedInstanceAsJson);
      LOGGER.info("updateInstance:: Merged instance: {}", mergedInstance);
      return Future.succeededFuture(mergedInstance);
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      return Future.failedFuture(e);
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
