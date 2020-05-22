package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.ActionProfile.Action.REPLACE;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_REPLACED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.HoldingsRecord;
import org.folio.inventory.common.Context;
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

public class UpdateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateHoldingEventHandler.class);

  private static final String UPDATE_HOLDING_ERROR_MESSAGE = "Can`t update  holding";
  private static final String CONTEXT_EMPTY_ERROR_MESSAGE = "Can`t update Holding entity: context or Holding-entity are empty or doesn`t exist!";
  private static final String EMPTY_REQUIRED_FIELDS_ERROR_MESSAGE = "Can`t udpate Holding entity: one of required fields(hrid, permanentLocationId, instanceId) are empty!";

  private final Storage storage;

  public UpdateHoldingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (dataImportEventPayload.getContext().isEmpty()
        || StringUtils.isEmpty(dataImportEventPayload.getContext().get(HOLDINGS.value()))) {
        throw new EventProcessingException(CONTEXT_EMPTY_ERROR_MESSAGE);
      }
      HoldingsRecord tmpHoldingsRecord = ObjectMapperTool.getMapper()
        .readValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord.class);

      String holdingId = tmpHoldingsRecord.getId();
      String hrid = tmpHoldingsRecord.getHrid();
      String instanceId = tmpHoldingsRecord.getInstanceId();
      String permanentLocationId = tmpHoldingsRecord.getPermanentLocationId();
      if (StringUtils.isAnyBlank(hrid, instanceId, permanentLocationId, holdingId)) {
        throw new EventProcessingException(EMPTY_REQUIRED_FIELDS_ERROR_MESSAGE);
      }
      prepareEvent(dataImportEventPayload);

      MappingManager.map(dataImportEventPayload);

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
      HoldingsRecord holding = ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), HoldingsRecord.class);
      holding.setId(holdingId);
      holding.setHrid(hrid);
      holding.setInstanceId(instanceId);
      holding.setPermanentLocationId(permanentLocationId);

      holdingsRecords.update(holding, holdingSuccess -> constructDataImportEventPayload(future, dataImportEventPayload, holding),
        failure -> {
          LOGGER.error(UPDATE_HOLDING_ERROR_MESSAGE);
          future.completeExceptionally(new EventProcessingException(UPDATE_HOLDING_ERROR_MESSAGE));
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
      return actionProfile.getAction() == REPLACE && actionProfile.getFolioRecord() == HOLDINGS;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void constructDataImportEventPayload(CompletableFuture<DataImportEventPayload> future, DataImportEventPayload dataImportEventPayload, HoldingsRecord holding) {
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(holding));
    dataImportEventPayload.setEventType(DI_INVENTORY_HOLDING_REPLACED.value());
    future.complete(dataImportEventPayload);
  }
}
