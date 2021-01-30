package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.logging.log4j.util.Strings.isNotEmpty;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingEventHandler.class);
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String HOLDINGS_PATH_FIELD = "holdings";
  private static final String PERMANENT_LOCATION_ID_FIELD = "permanentLocationId";
  private static final String PERMANENT_LOCATION_ID_ERROR_MESSAGE = "Can`t create Holding entity: 'permanentLocationId' is empty";
  private static final String SAVE_HOLDING_ERROR_MESSAGE = "Can`t save new holding";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t create Holding entity: context is empty or doesn`t exists";
  private static final String PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG = "Failed to extract instanceId from instance entity or parsed record";

  private final Storage storage;

  public CreateHoldingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (dataImportEventPayload.getContext() == null
        || StringUtils.isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))) {
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      prepareEvent(dataImportEventPayload);
      MappingManager.map(dataImportEventPayload);
      JsonObject holdingAsJson = new JsonObject(dataImportEventPayload.getContext().get(HOLDINGS.value()));
      if (holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD) != null) {
        holdingAsJson = holdingAsJson.getJsonObject(HOLDINGS_PATH_FIELD);
      }
      holdingAsJson.put("id", UUID.randomUUID().toString());
      fillInstanceIdIfNeeded(dataImportEventPayload, holdingAsJson);
      checkIfPermanentLocationIdExists(holdingAsJson);

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
      HoldingsRecord holding = ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord.class);
      holdingsRecords.add(holding, holdingSuccess -> constructDataImportEventPayload(future, dataImportEventPayload, holdingSuccess),
        failure -> {
          LOGGER.error(SAVE_HOLDING_ERROR_MESSAGE);
          future.completeExceptionally(new EventProcessingException(SAVE_HOLDING_ERROR_MESSAGE));
        });
    } catch (Exception e) {
      LOGGER.error(e);
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
  }

  private void fillInstanceIdIfNeeded(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson) throws IOException {
    if (isBlank(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      String instanceId = null;
      String instanceAsString = dataImportEventPayload.getContext().get(EntityType.INSTANCE.value());

      if (isNotEmpty(instanceAsString)) {
        JsonObject holdingsRecord = new JsonObject(instanceAsString);
        instanceId = holdingsRecord.getString("id");
      }
      if (isBlank(instanceId)) {
        String recordAsString = dataImportEventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value());
        Record record = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);
        instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      }
      if (isBlank(instanceId)) {
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_INSTANCE_ID_ERROR_MSG);
      }
      fillInstanceId(dataImportEventPayload, holdingAsJson, instanceId);
    }
  }

  private void checkIfPermanentLocationIdExists(JsonObject holdingAsJson) {
    if (isEmpty(holdingAsJson.getString(PERMANENT_LOCATION_ID_FIELD))) {
      throw new EventProcessingException(PERMANENT_LOCATION_ID_ERROR_MESSAGE);
    }
  }

  private void constructDataImportEventPayload(CompletableFuture<DataImportEventPayload> future, DataImportEventPayload dataImportEventPayload, Success<HoldingsRecord> holdingSuccess) {
    HoldingsRecord createdHolding = holdingSuccess.getResult();
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(createdHolding));
    dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_CREATED.value());
    future.complete(dataImportEventPayload);
  }

  private void fillInstanceId(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson, String instanceId) {
    holdingAsJson.put(INSTANCE_ID_FIELD, instanceId);
    dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingAsJson.encode());
  }
}
