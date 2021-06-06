package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.defaultmapper.RecordToInstanceMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;

import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static java.lang.String.format;

public class InstanceUpdateDelegate {

  private static final Logger LOGGER = LogManager.getLogger(InstanceUpdateDelegate.class);

  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String MARC_FORMAT = "MARC";

  private final Storage storage;
  private final WebClient webClient;
  private final Function<Context, OkapiHttpClient> okapiHttpClientCreator;

  public InstanceUpdateDelegate(Storage storage, WebClient webClient) {
    this.storage = storage;
    this.webClient = webClient;
    this.okapiHttpClientCreator = this::createHttpClient;
  }

  public InstanceUpdateDelegate(Storage storage, Function<Context, OkapiHttpClient> okapiHttpClientCreator) {
    this.storage = storage;
    this.webClient = null;
    this.okapiHttpClientCreator = okapiHttpClientCreator;
  }

  public Future<Instance> handle(Map<String, String> eventPayload, Record marcRecord, Context context) {
    try {
      JsonObject mappingRules = new JsonObject(eventPayload.get(MAPPING_RULES_KEY));
      MappingParameters mappingParameters = new JsonObject(eventPayload.get(MAPPING_PARAMS_KEY)).mapTo(MappingParameters.class);

      JsonObject parsedRecord = retrieveParsedContent(marcRecord.getParsedRecord());
      String instanceId = marcRecord.getExternalIdsHolder().getInstanceId();
      org.folio.Instance mappedInstance = RecordToInstanceMapperBuilder.buildMapper(MARC_FORMAT).mapRecord(parsedRecord, mappingParameters, mappingRules);
      InstanceCollection instanceCollection = storage.getInstanceCollection(context);
      CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(context);
      CollectionResourceRepository precedingSucceedingTitlesRepository = new CollectionResourceRepository(precedingSucceedingTitlesClient);

      Promise<Instance> instanceUpdatePromise = Promise.promise();
      return getInstanceById(instanceId, instanceCollection)
        .compose(existingInstance -> updateInstance(existingInstance, mappedInstance))
        .compose(updatedInstance -> updateInstanceInStorage(updatedInstance, instanceCollection))
        .onSuccess(instanceUpdatePromise::complete)
        .compose(updatedInstance -> getExistingPrecedingSucceedingTitles(updatedInstance, precedingSucceedingTitlesClient))
        .compose(precedingSucceedingTitles -> deletePrecedingSucceedingTitles(precedingSucceedingTitles, precedingSucceedingTitlesRepository))
        .compose(ar -> createPrecedingSucceedingTitles(instanceUpdatePromise.future().result(), precedingSucceedingTitlesRepository))
        .map(instanceUpdatePromise.future().result());
    } catch (Exception e) {
      LOGGER.error("Error updating inventory instance", e);
      return Future.failedFuture(e);
    }
  }

  private JsonObject retrieveParsedContent(ParsedRecord parsedRecord) {
    return parsedRecord.getContent() instanceof String
      ? new JsonObject(parsedRecord.getContent().toString())
      : JsonObject.mapFrom(parsedRecord.getContent());
  }

  private Future<Instance> getInstanceById(String instanceId, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(format("Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<Instance> updateInstance(Instance existingInstance, org.folio.Instance mappedInstance) {
    try {
      mappedInstance.setId(existingInstance.getId());
      JsonObject existing = JsonObject.mapFrom(existingInstance);
      JsonObject mapped = JsonObject.mapFrom(mappedInstance);
      JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existing, mapped);
      Instance mergedInstance = Instance.fromJson(mergedInstanceAsJson);
      return Future.succeededFuture(mergedInstance);
    } catch (Exception e) {
      LOGGER.error("Error updating instance", e);
      return Future.failedFuture(e);
    }
  }

  private Future<Instance> updateInstanceInStorage(Instance instance, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.update(instance, success -> promise.complete(instance),
      failure -> {
        LOGGER.error(format("Error updating Instance - %s, status code %s", failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }

  private Future<List<JsonObject>> getExistingPrecedingSucceedingTitles(Instance instance, CollectionResourceClient precedingSucceedingTitlesClient) {
    Promise<List<JsonObject>> promise = Promise.promise();
    String instanceId = instance.getId();
    String queryForPrecedingSucceedingInstances = String.format("query=succeedingInstanceId==(%s)+or+precedingInstanceId==(%s)", instanceId, instanceId);

    precedingSucceedingTitlesClient.getMany(queryForPrecedingSucceedingInstances, response -> {
      if (response.getStatusCode() == 200) {
        JsonObject json = response.getJson();
        List<JsonObject> precedingSucceedingTitles = JsonArrayHelper.toList(json.getJsonArray("precedingSucceedingTitles"));
        promise.complete(precedingSucceedingTitles);
      } else {
        String msg = format("Error retrieving existing preceding and succeeding titles. Status code: %s", response.getStatusCode());
        LOGGER.error(msg);
        promise.fail(msg);
      }
    });

    return promise.future();
  }

  protected CollectionResourceClient createPrecedingSucceedingTitlesClient(Context context) {
    try {
      OkapiHttpClient okapiClient = okapiHttpClientCreator.apply(context);
      return new CollectionResourceClient(okapiClient, new URL(context.getOkapiLocation() + "/preceding-succeeding-titles"));
    } catch (MalformedURLException e) {
      throw new EventProcessingException("Error creation of precedingSucceedingClient", e);
    }
  }

  private OkapiHttpClient createHttpClient(Context context) {
    try {
      return new OkapiHttpClient(webClient, new URL(context.getOkapiLocation()),
        context.getTenantId(), context.getToken(), null, null, null);
    } catch (MalformedURLException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected Future<Void> createPrecedingSucceedingTitles(Instance instance, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    List<PrecedingSucceedingTitle> precedingSucceedingTitles = new ArrayList<>();
    preparePrecedingTitles(instance, precedingSucceedingTitles);
    prepareSucceedingTitles(instance, precedingSucceedingTitles);

    precedingSucceedingTitles.forEach(title -> precedingSucceedingTitlesRepository.post(title)
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

  protected Future<Void> deletePrecedingSucceedingTitles(List<JsonObject> precedingSucceedingTitles, CollectionResourceRepository precedingSucceedingTitlesRepository) {
    precedingSucceedingTitles.stream()
      .map(titleJson -> titleJson.getString("id"))
      .forEach(id -> precedingSucceedingTitlesRepository.delete(id)
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
}
