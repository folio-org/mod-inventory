package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.dataimport.util.LoggerUtil.logParametersEventHandler;

public class MarcBibMatchedPostProcessingEventHandler implements EventHandler {

  private static final Logger LOGGER = LogManager.getLogger(MarcBibMatchedPostProcessingEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Event does not contain required data to load Instance";
  private static final String MATCHED_RECORD_NOT_EXISTS_MSG = "Record by MATCHED_MARC_BIBLIOGRAPHIC-key doesn't exist in the payload";
  private static final String ERROR_INSTANCE_MSG = "Error loading inventory instance for MARC BIB";
  private static final String ERROR_HOLDING_MSG = "Error loading inventory holdings for MARC BIB";
  private static final String MATCHED_MARC_BIB_KEY = "MATCHED_MARC_BIBLIOGRAPHIC";
  private final Storage storage;
  private final ConsortiumService consortiumService;

  public MarcBibMatchedPostProcessingEventHandler(Storage storage, ConsortiumService consortiumService) {
    this.storage = storage;
    this.consortiumService = consortiumService;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    logParametersEventHandler(LOGGER, dataImportEventPayload);
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (isNull(payloadContext)) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }
      if (isBlank(payloadContext.get(MATCHED_MARC_BIB_KEY))) {
        LOGGER.info(MATCHED_RECORD_NOT_EXISTS_MSG);
        future.complete(dataImportEventPayload);
        return future;
      }

      Record matchedRecord = new JsonObject(payloadContext.get(MATCHED_MARC_BIB_KEY)).mapTo(Record.class);
      String instanceId = ParsedRecordUtil.getAdditionalSubfieldValue(matchedRecord.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.I);
      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      HoldingsRecordCollection holdingsRecordCollection = storage.getHoldingsRecordCollection(context);
      if (isBlank(instanceId)) {
        future.complete(dataImportEventPayload);
        return future;
      }

      instanceCollection.findById(instanceId)
        .thenCompose(localInstance -> {
          if (localInstance == null) {
            LOGGER.debug("handle:: Instance not found at local tenant during marcBibMatchPostProcessing, tenantId: {}, instanceId: {}",
              context.getTenantId(), instanceId);
            return getInstanceFromCentralTenantIfLocalTenantInConsortium(instanceId, context);
          }
          return CompletableFuture.completedFuture(localInstance);
        })
        .whenComplete((v, t) -> {
          if (t == null && v != null) {
            LOGGER.debug("handle:: Instance found during marcBibMatchPostProcessing, tenantId: {}, instanceId: {}",
              context.getTenantId(), instanceId);
            dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(v));
            try {
              holdingsRecordCollection.findByCql(format("instanceId=%s", v.getId()), PagingParameters.defaults(),
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

  private CompletionStage<Instance> getInstanceFromCentralTenantIfLocalTenantInConsortium(String instanceId, Context context) {
    return consortiumService.getConsortiumConfiguration(context)
      .toCompletionStage()
      .thenCompose(consortiumConfigurationOptional -> {
        if (consortiumConfigurationOptional.isPresent()) {
          Context consortiumContext = constructContext(consortiumConfigurationOptional.get().getCentralTenantId(), context.getToken(), context.getOkapiLocation());
          LOGGER.debug("getInstanceFromCentralTenantIfLocalTenantInConsortium:: Searching instance at central tenant, centralTenantId: {}, localTenantId: {}, instanceId: {}",
            consortiumContext.getTenantId(), context.getTenantId(), instanceId);
          return storage.getInstanceCollection(consortiumContext).findById(instanceId);
        }
        return CompletableFuture.completedFuture(null);
      });
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    return DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value().equals(dataImportEventPayload.getEventType());
  }
}
