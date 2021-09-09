package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.HoldingCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.processing.mapping.mapper.writer.marc.MarcRecordModifier.MATCHED_MARC_BIB_KEY;

public class MarcBibMatchedPostProcessingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibMatchedPostProcessingEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Event does not contain required data to load Instance";
  private static final String ERROR_INSTANCE_MSG = "Error loading inventory instance for MARC BIB";
  private static final String ERROR_HOLDING_MSG = "Error loading inventory holdings for MARC BIB";
  private final Storage storage;

  public MarcBibMatchedPostProcessingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext) || isBlank(payloadContext.get(MATCHED_MARC_BIB_KEY))) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      Record record = new JsonObject(payloadContext.get(MATCHED_MARC_BIB_KEY)).mapTo(Record.class);
      String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      HoldingCollection holdingCollection = storage.getHoldingCollection(context);
      if (isBlank(instanceId)) {
        future.complete(dataImportEventPayload);
        return future;
      }

      instanceCollection.findById(instanceId)
        .whenComplete((v, t) -> {
          if (t == null && v != null) {
            dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(v));
            try {
              holdingCollection.findByCql(format("instanceId=%s", v.getId()), PagingParameters.defaults(),
                findResult -> {
                  if (findResult.getResult() != null && findResult.getResult().totalRecords == 1) {
                    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encode(findResult.getResult().records.get(0)));
                  }
                  future.complete(dataImportEventPayload);
                },
                failure -> {
                  LOGGER.error(ERROR_HOLDING_MSG + format(". StatusCode: %s. Message: %s", failure.getStatusCode(), failure.getReason()));
                  future.complete(dataImportEventPayload);
                });
            } catch (UnsupportedEncodingException e) {
              LOGGER.error(ERROR_HOLDING_MSG, e);
              future.complete(dataImportEventPayload);
            }
          } else {
            LOGGER.error(ERROR_INSTANCE_MSG, t);
            future.complete(dataImportEventPayload);
          }
        });
    } catch (Exception e) {
      LOGGER.error(ERROR_INSTANCE_MSG, e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    return DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value().equals(dataImportEventPayload.getEventType());
  }
}
