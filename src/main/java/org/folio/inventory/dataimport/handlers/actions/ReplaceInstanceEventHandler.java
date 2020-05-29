package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
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
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.folio.ActionProfile.Action.REPLACE;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.inventory.domain.instances.Instance.HRID_KEY;
import static org.folio.inventory.domain.instances.Instance.METADATA_KEY;
import static org.folio.inventory.domain.instances.Instance.SOURCE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class ReplaceInstanceEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReplaceInstanceEventHandler.class);

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC or INSTANCE data";
  private static final String MARC_FORMAT = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String INSTANCE_PATH = "instance";
  private final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");

  private Storage storage;
  private HttpClient client;

  public ReplaceInstanceEventHandler(Storage storage, HttpClient client) {
    this.storage = storage;
    this.client = client;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
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

  private void defaultMapRecordToInstance(DataImportEventPayload dataImportEventPayload) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      JsonObject mappingRules = new JsonObject(context.get(MAPPING_RULES_KEY));
      JsonObject parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_BIBLIOGRAPHIC.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
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
      return actionProfile.getAction() == REPLACE && actionProfile.getFolioRecord() == INSTANCE;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(INSTANCE.value(), new JsonObject().encode());
  }

  private Future<Void> updateInstance(Instance instance, InstanceCollection instanceCollection) {
    Future<Void> future = Future.future();
    instanceCollection.update(instance, success -> future.complete(),
      failure -> {
        LOGGER.error("Error updating Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode());
        future.fail(failure.getReason());
      });
    return future;
  }

  private Future<Void> deletePrecedingSucceedingTitles(Set<String> ids, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    Future<Void> future = Future.future();
    List<CompletableFuture<Response>> deleteFutures = new ArrayList<>();

    ids.forEach(id -> deleteFutures.add(precedingSucceedingTitlesRepository.delete(id)));
    CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture<?>[]{}))
      .whenComplete((v, e) -> {
        if (e != null) {
          future.fail(e);
          return;
        }
        future.complete();
      });
    return future;
  }

  private Future<Void> createPrecedingSucceedingTitles(Instance instance, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    Future<Void> future = Future.future();
    List<PrecedingSucceedingTitle> precedingSucceedingTitles = new ArrayList<>();
    preparePrecedingTitles(instance, precedingSucceedingTitles);
    prepareSucceedingTitles(instance, precedingSucceedingTitles);
    List<CompletableFuture<Response>> postFutures = new ArrayList<>();

    precedingSucceedingTitles.forEach(title -> postFutures.add(precedingSucceedingTitlesRepository.post(title)));
    CompletableFuture.allOf(postFutures.toArray(new CompletableFuture<?>[] {}))
      .whenComplete((v, e) -> {
        if (e != null) {
          future.fail(e);
          return;
        }
        future.complete();
      });
    return future;
  }

  private void preparePrecedingTitles(Instance instance, List<PrecedingSucceedingTitle> preparedTitles) {
    if (instance.getPrecedingTitles() != null) {
      for (PrecedingSucceedingTitle parent : instance.getPrecedingTitles()) {
        PrecedingSucceedingTitle precedingSucceedingTitle = new PrecedingSucceedingTitle(
          UUID.randomUUID().toString(),
          parent.precedingInstanceId,
          instance.getId(),
          parent.title,
          parent.hrid,
          parent.identifiers);
        preparedTitles.add(precedingSucceedingTitle);
      }
    }
  }

  private void prepareSucceedingTitles(Instance instance, List<PrecedingSucceedingTitle> preparedTitles) {
    if (instance.getSucceedingTitles() != null) {
      for (PrecedingSucceedingTitle child : instance.getSucceedingTitles()) {
        PrecedingSucceedingTitle precedingSucceedingTitle = new PrecedingSucceedingTitle(
          UUID.randomUUID().toString(),
          instance.getId(),
          child.succeedingInstanceId,
          child.title,
          child.hrid,
          child.identifiers);
        preparedTitles.add(precedingSucceedingTitle);
      }
    }
  }

  private CollectionResourceClient createPrecedingSucceedingTitlesClient(Context context) {
    try {
      OkapiHttpClient okapiClient = createHttpClient(context);
      return new CollectionResourceClient(okapiClient, new URL(context.getOkapiLocation() + "/preceding-succeeding-titles"));
    } catch (MalformedURLException e) {
      throw new EventProcessingException("Error creation of precedingSucceedingClient", e);
    }
  }

  private OkapiHttpClient createHttpClient(Context context) throws MalformedURLException {
    return new OkapiHttpClient(client, new URL(context.getOkapiLocation()), context.getTenantId(),
      context.getToken(), null, null, null);
  }
}
