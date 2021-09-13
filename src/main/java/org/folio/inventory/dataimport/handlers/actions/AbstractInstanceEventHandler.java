package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.EventManager;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.resource.MappingRules;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_ERROR;

public abstract class AbstractInstanceEventHandler implements EventHandler {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractInstanceEventHandler.class);
  protected static final String MARC_FORMAT = "MARC";
  protected static final String MARC_BIB_RECORD_FORMAT = "MARC_BIB";
  protected static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  protected static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  protected static final String INSTANCE_PATH = "instance";
  protected static final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");

  protected final Storage storage;

  public AbstractInstanceEventHandler(Storage storage) {
    this.storage = storage;
  }

  protected void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(INSTANCE.value(), new JsonObject().encode());
  }

  protected org.folio.Instance defaultMapRecordToInstance(DataImportEventPayload dataImportEventPayload,
                                                          JsonObject mappingRules, MappingParameters mappingParameters) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      JsonObject parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_BIBLIOGRAPHIC.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      org.folio.Instance instance = RecordToInstanceMapperBuilder.buildMapper(MARC_FORMAT).mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(instance))));
      return instance;
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Instance", e);
      throw new JsonMappingException("Error in default mapper.", e);
    }
  }

  protected Future<Void> updateInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Void> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(),
      failure -> {
        LOGGER.error(format("Error updating Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
