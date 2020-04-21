package org.folio.inventory.resources;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.EndpointFailureHandler.getKnownException;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;

import java.util.concurrent.CompletableFuture;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.Clients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public abstract class AbstractInventoryResource {
  private static final Logger log = LoggerFactory.getLogger(AbstractInventoryResource.class);

  protected final Storage storage;
  protected final HttpClient client;

  protected AbstractInventoryResource(final Storage storage, final HttpClient client) {
    this.storage = storage;
    this.client = client;
  }

  public abstract void register(Router router);

  /**
   * Handler for particular path. Creates commonly used objects and handles failures.
   *
   * @param handler - Handler for path.
   * @param <T>     - return type.
   * @return Vert.x handler.
   */
  protected <T> Handler<RoutingContext> handle(RouteHandler<T> handler) {
    return routingContext -> {
      final WebContext webContext = new WebContext(routingContext);

      completedFuture(new Clients(routingContext, client))
        .thenCompose(clients -> handler.handle(routingContext, webContext, clients))
        .exceptionally(error -> {
            log.warn("Error occurred", error);

            handleFailure(getKnownException(error), routingContext);
            return null;
          }
        );
    };
  }

  @FunctionalInterface
  protected interface RouteHandler<T> {
    CompletableFuture<T> handle(RoutingContext routingContext, WebContext webContext,
                                Clients clients);
  }
}
