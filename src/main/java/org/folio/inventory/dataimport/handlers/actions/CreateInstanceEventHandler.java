package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateInstanceEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateInstanceEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String MARC_FORMAT = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String INSTANCE_PATH = "instance";
  private final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");

  private Storage storage;

  public CreateInstanceEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty() ||
        isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value())) ||
        isEmpty(dataImportEventPayload.getContext().get(MAPPING_RULES_KEY)) ||
        isEmpty(dataImportEventPayload.getContext().get(MAPPING_PARAMS_KEY))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      prepareEvent(dataImportEventPayload);
      defaultMapRecordToInstance(dataImportEventPayload);
      MappingManager.map(dataImportEventPayload);
      JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
      if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
        instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
      }
      instanceAsJson.put("id", UUID.randomUUID().toString());
      instanceAsJson.put("source", MARC_FORMAT);

      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
      if (errors.isEmpty()) {
        Instance mappedInstance = InstanceUtil.jsonToInstance(instanceAsJson);
        addInstance(mappedInstance, instanceCollection)
          .setHandler(ar -> {
            if (ar.succeeded()) {
              dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(ar.result()));
              dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_CREATED.value());
              future.complete(dataImportEventPayload);
            } else {
              LOGGER.error("Error creating inventory Instance", ar.cause());
              future.completeExceptionally(ar.cause());
            }
          });
      } else {
        String msg = String.format("Mapped Instance is invalid: %s", errors.toString());
        LOGGER.error(msg);
        future.completeExceptionally(new EventProcessingException(msg));
      }
    } catch (Exception e) {
      LOGGER.error("Error creating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  private void defaultMapRecordToInstance(DataImportEventPayload dataImportEventPayload) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      JsonObject mappingRules = new JsonObject(context.get(MAPPING_RULES_KEY));
      JsonObject parsedRecord = new JsonObject(context.get(MARC_BIBLIOGRAPHIC.value())).getJsonObject("parsedRecord").getJsonObject("content");
      MappingParameters mappingParameters = new JsonObject(context.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      org.folio.Instance instance = RecordToInstanceMapperBuilder.buildMapper(MARC_FORMAT).mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(instance))));
    } catch (Exception e) {
      LOGGER.error("Error in default mapper.", e);
    }

  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(INSTANCE.value(), new JsonObject().encode());
  }

  private Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    Future<Instance> future = Future.future();
    instanceCollection.add(instance, success -> future.complete(success.getResult()),
      failure -> {
        LOGGER.error("Error posting Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode());
        future.fail(failure.getReason());
      });
    return future;
  }
}
