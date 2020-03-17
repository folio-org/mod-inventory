package org.folio.inventory.dataimport.handlers.action;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.Holdingsrecord;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingEventHandler.class);
  private static final String CREATED_HOLDINGS_RECORD_EVENT_TYPE = "DI_INVENTORY_HOLDING_CREATED";
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String PERMANENT_LOCATION_ID_FIELD = "permanentLocationId";
  private static final String INSTANCE_ID_ERROR_MESSAGE = "Error: 'instanceId' is empty";
  private static final String PERMANENT_LOCATION_ID_ERROR_MESSAGE = "Can`t create Holding entity: 'permanentLocationId' is empty";
  private static final String SAVE_HOLDING_ERROR_MESSAGE = "Can`t save new holding";
  private static final String CREATING_HOLDING_ENTITY_ERROR_MESSAGE = "Can`t create Holding entity";

  private Storage storage;

  public CreateHoldingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      prepareEvent(dataImportEventPayload);
      MappingManager.map(dataImportEventPayload);
      JsonObject holdingAsJson = new JsonObject(dataImportEventPayload.getContext().get(HOLDINGS.value()));

      fillInstanceIdIfNeeded(future, dataImportEventPayload, holdingAsJson);

      if (isEmpty(holdingAsJson.getString(PERMANENT_LOCATION_ID_FIELD))) {
        throwException(future, PERMANENT_LOCATION_ID_ERROR_MESSAGE);
      } else {
        Holdingsrecord holding = ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), Holdingsrecord.class);
        Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
        HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
        holdingsRecords.add(holding, holdingSuccess -> constructDataImportEventPayload(future, dataImportEventPayload, holdingSuccess),
          failure -> throwException(future, SAVE_HOLDING_ERROR_MESSAGE));
      }
    } catch (IOException e) {
      LOGGER.error(CREATING_HOLDING_ENTITY_ERROR_MESSAGE, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == ActionProfile.Action.CREATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.HOLDINGS;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getCurrentNode()
      .setContent(new JsonObject((LinkedHashMap) dataImportEventPayload.getCurrentNode().getContent()).mapTo(MappingProfile.class));
  }

  private void constructDataImportEventPayload(CompletableFuture<DataImportEventPayload> future, DataImportEventPayload dataImportEventPayload, Success<Holdingsrecord> holdingSuccess) {
    Holdingsrecord createdHolding = holdingSuccess.getResult();
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(createdHolding));
    dataImportEventPayload.setEventType(CREATED_HOLDINGS_RECORD_EVENT_TYPE);
    future.complete(dataImportEventPayload);
  }

  private void fillInstanceIdIfNeeded(CompletableFuture<DataImportEventPayload> future, DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson) {
    if (isEmpty(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
      JsonObject instanceAsJson = new JsonObject(instanceAsString);
      String instanceId = instanceAsJson.getString("id");
      if (isEmpty(instanceId)) {
        throwException(future, INSTANCE_ID_ERROR_MESSAGE);
      } else {
        holdingAsJson.put(INSTANCE_ID_FIELD, instanceId);
        dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingAsJson.encode());
      }
    }
  }

  private void throwException(CompletableFuture<DataImportEventPayload> future, String errorMessage) {
    LOGGER.error(errorMessage);
    future.completeExceptionally(new EventProcessingException(errorMessage));
  }
}
