package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

public class MarcBibModifiedPostProcessingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibModifiedPostProcessingEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Event does not contain required data to update Instance";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  private final InstanceUpdateDelegate instanceUpdateDelegate;

  public MarcBibModifiedPostProcessingEventHandler(InstanceUpdateDelegate updateInstanceDelegate) {
    this.instanceUpdateDelegate = updateInstanceDelegate;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MARC_BIBLIOGRAPHIC.value()))
        || isBlank(payloadContext.get(MAPPING_RULES_KEY))
        || isBlank(payloadContext.get(MAPPING_PARAMS_KEY))) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }

      Record record = new JsonObject(payloadContext.get(MARC_BIBLIOGRAPHIC.value())).mapTo(Record.class);
      String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      if (isBlank(instanceId)) {
        future.complete(dataImportEventPayload);
        return future;
      }

      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      instanceUpdateDelegate.handle(dataImportEventPayload.getContext(), record, context)
        .onComplete(updateAr -> {
          if (updateAr.succeeded()) {
            dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(updateAr.result()));
            future.complete(dataImportEventPayload);
          } else {
            LOGGER.error("Error updating inventory instance", updateAr.cause());
            future.completeExceptionally(updateAr.cause());
          }
        });
    } catch (Exception e) {
      LOGGER.error("Error updating inventory instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MAPPING_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MappingProfile mappingProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MappingProfile.class);
      return DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value().equals(dataImportEventPayload.getEventType())
        && mappingProfile.getExistingRecordType() == EntityType.MARC_BIBLIOGRAPHIC;
    }
    return false;
  }
}
