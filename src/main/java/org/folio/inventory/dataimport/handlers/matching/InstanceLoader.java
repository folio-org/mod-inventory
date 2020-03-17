package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.storage.Storage;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

public class InstanceLoader implements MatchValueLoader {

  private static final Logger LOG = LoggerFactory.getLogger(InstanceLoader.class);

  private Storage storage;

  public InstanceLoader(Storage storage) {
    this.storage = storage;
  }

  @Override
  public LoadResult loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    if (loadQuery == null){
      return new LoadResult();
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(EntityType.INSTANCE.value());
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());
    try {
      storage.getInstanceCollection(context).findByCql(loadQuery.getCql(), PagingParameters.defaults(),
        success -> {
          MultipleRecords<Instance> instanceCollection = success.getResult();
          if (instanceCollection.totalRecords == 1) {
            loadResult.setValue(JsonObject.mapFrom(instanceCollection.records.get(0)).encode());
          } else if (instanceCollection.totalRecords > 1) {
            String errorMessage = "Found multiple instances matching specified conditions";
            LOG.error(errorMessage);
            future.completeExceptionally(new MatchingException(errorMessage));
          }
          future.complete(loadResult);
        },
        failure -> {
          LOG.error(failure.getReason());
          future.completeExceptionally(new MatchingException(failure.getReason()));
        });
    } catch (UnsupportedEncodingException e) {
      LOG.error("Failed to retrieve instances");
      future.completeExceptionally(e);
    }
    return future.join();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.INSTANCE == entityType;
  }

}
