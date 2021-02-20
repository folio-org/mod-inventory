package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;

public abstract class AbstractInstanceEventHandler implements EventHandler {
  protected static final Logger LOGGER = LogManager.getLogger(AbstractInstanceEventHandler.class);
  protected static final String MARC_FORMAT = "MARC";
  protected static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  protected static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  protected static final String INSTANCE_PATH = "instance";
  protected static final List<String> requiredFields = Arrays.asList("source", "title", "instanceTypeId");

  protected final Storage storage;
  protected final BiFunction<WebClient, Context, OkapiHttpClient> okapiClientCreator;
  private final WebClient webClient;

  public AbstractInstanceEventHandler(Storage storage, WebClient webClient,
    BiFunction<WebClient, Context, OkapiHttpClient> okapiClientCreator) {

    this.storage = storage;
    this.webClient = webClient;
    this.okapiClientCreator = okapiClientCreator;
  }

  public AbstractInstanceEventHandler(Storage storage, WebClient webClient) {
    this.storage = storage;
    this.webClient = webClient;
    this.okapiClientCreator = this::createHttpClient;
  }


  protected Future<Void> createPrecedingSucceedingTitles(Instance instance, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    List<PrecedingSucceedingTitle> precedingSucceedingTitles = new ArrayList<>();
    preparePrecedingTitles(instance, precedingSucceedingTitles);
    prepareSucceedingTitles(instance, precedingSucceedingTitles);

    precedingSucceedingTitles.forEach(title -> precedingSucceedingTitlesRepository
      .post(title)
      .whenComplete((v, e) -> {
          if (e != null) {
            LOGGER.error("Error during creating PrecedingSucceedingTitle for instance {}", instance.getId(), e);
            LOGGER.info("Error during creating PrecedingSucceedingTitles retry creating new PrecedingSucceedingTitles");
            precedingSucceedingTitlesRepository.post(title);
          }
        }
      ));
    return Future.succeededFuture();
  }

  protected Future<Void> deletePrecedingSucceedingTitles(Set<String> ids, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    ids.forEach(id -> precedingSucceedingTitlesRepository
      .delete(id)
      .whenComplete((v, e) -> {
          if (e != null) {
            LOGGER.error("Error during deleting PrecedingSucceedingTitles with ids {}", id, e);
            LOGGER.info("Error during deleting PrecedingSucceedingTitles retry delete PrecedingSucceedingTitles");
            precedingSucceedingTitlesRepository.delete(id);
          }
        }
      ));
    return Future.succeededFuture();
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
      OkapiHttpClient okapiClient = okapiClientCreator.apply(webClient, context);
      return new CollectionResourceClient(okapiClient, new URL(context.getOkapiLocation() + "/preceding-succeeding-titles"));
    } catch (MalformedURLException e) {
      throw new EventProcessingException("Error creation of precedingSucceedingClient", e);
    }
  }

  @SneakyThrows
  private OkapiHttpClient createHttpClient(WebClient webClient, Context context) {
    return new OkapiHttpClient(webClient, new URL(context.getOkapiLocation()),
      context.getTenantId(), context.getToken(), null, null, null);
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
        LOGGER.error(format("Error updating Instance cause %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
