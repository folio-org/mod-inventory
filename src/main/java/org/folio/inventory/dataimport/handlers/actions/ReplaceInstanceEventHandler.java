package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.InstanceUtil;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.METADATA_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class ReplaceInstanceEventHandler extends AbstractInstanceEventHandler { // NOSONAR

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC or INSTANCE data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to update an Instance requires a mapping profile";

  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  public ReplaceInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    super(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) { // NOSONAR
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_UPDATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null
        || payloadContext.isEmpty()
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))
        || isEmpty(dataImportEventPayload.getContext().get(MAPPING_RULES_KEY))
        || isEmpty(dataImportEventPayload.getContext().get(MAPPING_PARAMS_KEY))
        || isEmpty(dataImportEventPayload.getContext().get(INSTANCE.value()))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Instance instanceToUpdate = Instance.fromJson(new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value())));

      prepareEvent(dataImportEventPayload);

      org.folio.Instance mapped =  defaultMapRecordToInstance(dataImportEventPayload);
      Instance mergedInstance = InstanceUtil.mergeFieldsWhichAreNotControlled(instanceToUpdate, mapped);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(mergedInstance))));

      MappingManager.map(dataImportEventPayload);
      JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
      if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
        instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
      }

      Set<String> precedingSucceedingIds = new HashSet<>();
      precedingSucceedingIds.addAll(instanceToUpdate.getPrecedingTitles()
        .stream()
        .filter(pr -> isNotEmpty(pr.id))
        .map(pr -> pr.id)
        .collect(Collectors.toList()));
      precedingSucceedingIds.addAll(instanceToUpdate.getSucceedingTitles()
        .stream()
        .filter(pr -> isNotEmpty(pr.id))
        .map(pr -> pr.id)
        .collect(Collectors.toList()));
      instanceAsJson.put("id", instanceToUpdate.getId());
      instanceAsJson.put(HRID_KEY, instanceToUpdate.getHrid());
      instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
      instanceAsJson.put(METADATA_KEY, instanceToUpdate.getMetadata());

      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
      if (errors.isEmpty()) {
        Instance mappedInstance = Instance.fromJson(instanceAsJson);
        JsonObject finalInstanceAsJson = instanceAsJson;
        updateInstance(mappedInstance, instanceCollection)
          .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(mappedInstance, context))
          .map(precedingSucceedingTitles -> precedingSucceedingTitles.stream()
            .map(titleJson -> titleJson.getString("id"))
            .collect(Collectors.toSet()))
          .compose(titlesIds -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(titlesIds, context))
          .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(mappedInstance, context))
          .onComplete(ar -> {
            if (ar.succeeded()) {
              dataImportEventPayload.getContext().put(INSTANCE.value(), finalInstanceAsJson.encode());
              future.complete(dataImportEventPayload);
            } else {
              LOGGER.error("Error updating inventory Instance", ar.cause());
              future.completeExceptionally(ar.cause());
            }
          });
      } else {
        String msg = String.format("Mapped Instance is invalid: %s", errors.toString());
        LOGGER.error(msg);
        future.completeExceptionally(new EventProcessingException(msg));
      }
    } catch (Exception e) {
      LOGGER.error("Error updating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == UPDATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value();
  }
}
