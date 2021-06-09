package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CollectionResourceRepository;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.processing.exceptions.EventProcessingException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static java.lang.String.format;

public class PrecedingSucceedingTitlesHelper {

  private static final Logger LOGGER = LogManager.getLogger(PrecedingSucceedingTitlesHelper.class);

  private WebClient webClient;
  private Function<Context, OkapiHttpClient> okapiHttpClientCreator;

  public PrecedingSucceedingTitlesHelper(WebClient webClient) {
    this.webClient = webClient;
    this.okapiHttpClientCreator = this::createHttpClient;
  }

  public PrecedingSucceedingTitlesHelper(Function<Context, OkapiHttpClient> okapiHttpClientCreator) {
    this.okapiHttpClientCreator = okapiHttpClientCreator;
  }

  public Future<List<JsonObject>> getExistingPrecedingSucceedingTitles(Instance instance, Context context) {
    CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(context);

    Promise<List<JsonObject>> promise = Promise.promise();
    String instanceId = instance.getId();
    String queryForPrecedingSucceedingInstances = String.format("query=succeedingInstanceId==(%s)+or+precedingInstanceId==(%s)", instanceId, instanceId);

    precedingSucceedingTitlesClient.getMany(queryForPrecedingSucceedingInstances, response -> {
      if (response.getStatusCode() == 200) {
        JsonObject json = response.getJson();
        List<JsonObject> precedingSucceedingTitles = JsonArrayHelper.toList(json.getJsonArray("precedingSucceedingTitles"));
        promise.complete(precedingSucceedingTitles);
      } else {
        String msg = format("Error retrieving existing preceding and succeeding titles. Response status code: %s", response.getStatusCode());
        LOGGER.error(msg);
        promise.fail(msg);
      }
    });

    return promise.future();
  }

  public Future<Void> deletePrecedingSucceedingTitles(Set<String> titlesIds, Context context) {
    CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(context);
    CollectionResourceRepository precedingSucceedingTitlesRepository = new CollectionResourceRepository(precedingSucceedingTitlesClient);

    titlesIds.forEach(id -> precedingSucceedingTitlesRepository
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

  public Future<Void> createPrecedingSucceedingTitles(Instance instance, Context context) {
    CollectionResourceClient precedingSucceedingTitlesClient = createPrecedingSucceedingTitlesClient(context);
    CollectionResourceRepository precedingSucceedingTitlesRepository = new CollectionResourceRepository(precedingSucceedingTitlesClient);

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
      OkapiHttpClient okapiClient = okapiHttpClientCreator.apply(context);
      return new CollectionResourceClient(okapiClient, new URL(context.getOkapiLocation() + "/preceding-succeeding-titles"));
    } catch (MalformedURLException e) {
      throw new EventProcessingException("Error during creating precedingSucceedingTitlesClient", e);
    }
  }

  @SneakyThrows
  private OkapiHttpClient createHttpClient(Context context) {
    return new OkapiHttpClient(webClient, new URL(context.getOkapiLocation()),
      context.getTenantId(), context.getToken(), null, null, null);
  }

}
