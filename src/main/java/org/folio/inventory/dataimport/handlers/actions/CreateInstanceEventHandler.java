package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateInstanceEventHandler extends AbstractInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(CreateInstanceEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload - event payload context does not contain MARC_BIBLIOGRAPHIC data";
  static final String ACTION_HAS_NO_MAPPING_MSG = "Action profile to create an Instance requires a mapping profile";

  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  public CreateInstanceEventHandler(Storage storage, PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    super(storage);
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.setEventType(DI_INVENTORY_INSTANCE_CREATED.value());

      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || payloadContext.isEmpty() ||
        isEmpty(dataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value())) ||
        isEmpty(dataImportEventPayload.getContext().get(MAPPING_RULES_KEY)) ||
        isEmpty(dataImportEventPayload.getContext().get(MAPPING_PARAMS_KEY))
      ) {
        LOGGER.error(PAYLOAD_HAS_NO_DATA_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
      }
      if (dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().isEmpty()) {
        LOGGER.error(ACTION_HAS_NO_MAPPING_MSG);
        return CompletableFuture.failedFuture(new EventProcessingException(ACTION_HAS_NO_MAPPING_MSG));
      }

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      Record record = new JsonObject(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()))
        .mapTo(Record.class);

      prepareEvent(dataImportEventPayload);
      defaultMapRecordToInstance(dataImportEventPayload);
      MappingManager.map(dataImportEventPayload);
      JsonObject instanceAsJson = new JsonObject(dataImportEventPayload.getContext().get(INSTANCE.value()));
      if (instanceAsJson.getJsonObject(INSTANCE_PATH) != null) {
        instanceAsJson = instanceAsJson.getJsonObject(INSTANCE_PATH);
      }
      instanceAsJson.put("id", record.getId());
      instanceAsJson.put(SOURCE_KEY, MARC_FORMAT);
      instanceAsJson.remove(HRID_KEY);

      LOGGER.debug("Creating instance with id: {}", record.getId());

      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(instanceAsJson, requiredFields);
      if (errors.isEmpty()) {
        Instance mappedInstance = Instance.fromJson(instanceAsJson);
        addInstance(mappedInstance, instanceCollection)
          .compose(createdInstance -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(mappedInstance, context).map(createdInstance))
          .onSuccess(ar -> {
            dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(ar));
            future.complete(dataImportEventPayload);
          })
          .onFailure(ar -> {
            LOGGER.error("Error creating inventory Instance", ar);
            future.completeExceptionally(ar);
          });
      } else {
        String msg = format("Mapped Instance is invalid: %s", errors.toString());
        LOGGER.error(msg);
        future.completeExceptionally(new EventProcessingException(msg));
      }
    } catch (Exception e) {
      LOGGER.error("Error creating inventory Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_INSTANCE_CREATED_READY_FOR_POST_PROCESSING.value();
  }

  private Future<Instance> addInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.add(instance, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error posting Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
