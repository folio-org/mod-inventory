package org.folio.inventory.dataimport.handlers.matching.loaders;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.processing.exceptions.MatchingException;
import org.folio.processing.matching.loader.LoadResult;
import org.folio.processing.matching.loader.MatchValueLoader;
import org.folio.processing.matching.loader.query.LoadQuery;
import org.folio.rest.jaxrs.model.EntityType;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

public abstract class AbstractLoader<T> implements MatchValueLoader {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractLoader.class);

  @Override
  public LoadResult loadEntity(LoadQuery loadQuery, DataImportEventPayload eventPayload) {
    if (loadQuery == null){
      return new LoadResult();
    }
    CompletableFuture<LoadResult> future = new CompletableFuture<>();
    LoadResult loadResult = new LoadResult();
    loadResult.setEntityType(getEntityType().value());
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl());

    Vertx.vertx().executeBlocking(blockingFuture -> {
      try {
        getSearchableCollection(context).findByCql(loadQuery.getCql(), PagingParameters.defaults(),
          success -> {
            MultipleRecords<T> collection = success.getResult();
            if (collection.totalRecords == 1) {
              loadResult.setValue(JsonObject.mapFrom(collection.records.get(0)).encode());
            } else if (collection.totalRecords > 1) {
              String errorMessage = "Found multiple records matching specified conditions";
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
        LOG.error("Failed to retrieve records");
        future.completeExceptionally(e);
      }
    }, null);

    return future.join();
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return getEntityType() == entityType;
  }

  protected abstract EntityType getEntityType();

  protected abstract SearchableCollection<T> getSearchableCollection(Context context);
}
