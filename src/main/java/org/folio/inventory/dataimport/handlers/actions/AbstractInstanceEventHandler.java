package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;

public abstract class AbstractInstanceEventHandler implements EventHandler {
  protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractInstanceEventHandler.class);
  protected static final String MARC_FORMAT = "MARC";
  protected static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  protected static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  protected static final String INSTANCE_PATH = "instance";
  protected final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");

  protected Storage storage;
  protected HttpClient client;

  protected Future<Void> createPrecedingSucceedingTitles(Instance instance, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    Promise<Void> promise = Promise.promise();
    List<PrecedingSucceedingTitle> precedingSucceedingTitles = new ArrayList<>();
    preparePrecedingTitles(instance, precedingSucceedingTitles);
    prepareSucceedingTitles(instance, precedingSucceedingTitles);
    List<CompletableFuture<Response>> postFutures = new ArrayList<>();

    precedingSucceedingTitles.forEach(title -> postFutures.add(precedingSucceedingTitlesRepository.post(title)));
    CompletableFuture.allOf(postFutures.toArray(new CompletableFuture<?>[]{}))
      .whenComplete((v, e) -> {
        if (e != null) {
          promise.fail(e);
          return;
        }
        promise.complete();
      });
    return promise.future();
  }

  protected Future<Void> deletePrecedingSucceedingTitles(Set<String> ids, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    Promise<Void> promise = Promise.promise();
    List<CompletableFuture<Response>> deleteFutures = new ArrayList<>();

    ids.forEach(id -> deleteFutures.add(precedingSucceedingTitlesRepository.delete(id)));
    CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture<?>[]{}))
      .whenComplete((v, e) -> {
        if (e != null) {
          promise.fail(e);
          return;
        }
        promise.complete();
      });
    return promise.future();
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

  protected CollectionResourceClient createPrecedingSucceedingTitlesClient(Context context) {
    try {
      OkapiHttpClient okapiClient = createHttpClient(context);
      return new CollectionResourceClient(okapiClient, new URL(context.getOkapiLocation() + "/preceding-succeeding-titles"));
    } catch (MalformedURLException e) {
      throw new EventProcessingException("Error creation of precedingSucceedingClient", e);
    }
  }

  protected OkapiHttpClient createHttpClient(Context context) throws MalformedURLException {
    return new OkapiHttpClient(client, new URL(context.getOkapiLocation()), context.getTenantId(),
      context.getToken(), null, null, null);
  }


  protected void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
    dataImportEventPayload.getContext().put(INSTANCE.value(), new JsonObject().encode());
  }

  protected org.folio.Instance defaultMapRecordToInstance(DataImportEventPayload dataImportEventPayload) {
    try {
      HashMap<String, String> context = dataImportEventPayload.getContext();
      JsonObject mappingRules = new JsonObject(context.get(MAPPING_RULES_KEY));
      JsonObject parsedRecord = new JsonObject((String) new JsonObject(context.get(MARC_BIBLIOGRAPHIC.value()))
        .mapTo(Record.class).getParsedRecord().getContent());
      MappingParameters mappingParameters = new JsonObject(context.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);
      org.folio.Instance instance = RecordToInstanceMapperBuilder.buildMapper(MARC_FORMAT).mapRecord(parsedRecord, mappingParameters, mappingRules);
      dataImportEventPayload.getContext().put(INSTANCE.value(), Json.encode(new JsonObject().put(INSTANCE_PATH, JsonObject.mapFrom(instance))));
      return instance;
    } catch (Exception e) {
      LOGGER.error("Failed to map Record to Instance", e);
      throw new JsonMappingException("Error in default mapper.", e);
    }
  }

  protected Future<Void> updateInstance(Instance instance, InstanceCollection instanceCollection) {
    Promise<Void> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(),
      failure -> {
        LOGGER.error("Error updating Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode());
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
