package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
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

  public ReplaceInstanceEventHandler(Storage storage, HttpClient client) {
    this.storage = storage;
    this.client = client;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) { // NOSONAR
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null
        || payloadContext.isEmpty()
        || isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()))
        || isEmpty(dataImportEventPayload.getContext().get(MAPPING_RULES_KEY))
        || isEmpty(dataImportEventPayload.getContext().get(MAPPING_PARAMS_KEY))
        || isEmpty(dataImportEventPayload.getContext().get(INSTANCE.value()))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Instance instanceToUpdate = InstanceUtil.jsonToInstance(new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value())));

      prepareEvent(dataImportEventPayload);
      defaultMapRecordToInstance(dataImportEventPayload);
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
      CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(context);
      CollectionResourceRepository precedingSucceedingTitlesRepository = new CollectionResourceRepository(precedingSucceedingTitlesClient);
      List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
      if (errors.isEmpty()) {
        Instance mappedInstance = InstanceUtil.jsonToInstance(instanceAsJson);
        JsonObject finalInstanceAsJson = instanceAsJson;
        updateInstance(mappedInstance, instanceCollection)
          .compose(ar -> deletePrecedingSucceedingTitles(precedingSucceedingIds, precedingSucceedingTitlesRepository))
          .compose(ar -> createPrecedingSucceedingTitles(mappedInstance, precedingSucceedingTitlesRepository))
          .setHandler(ar -> {
            if (ar.succeeded()) {
              dataImportEventPayload.getContext().put(INSTANCE.value(), finalInstanceAsJson.encode());
              dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_UPDATED.value());
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
