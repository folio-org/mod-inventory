package org.folio.inventory.storage.external;

import java.net.MalformedURLException;
import java.net.URL;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.exceptions.InternalServerErrorException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;

public final class Clients {
  private final CollectionResourceRepository requestStorageRepository;

  /**
   * @param routingContext - Routing context.
   * @param client         - Http client.
   * @throws InternalServerErrorException - in case a URL parse exception occurred.
   */
  public Clients(RoutingContext routingContext, HttpClient client) {
    try {
      final WebContext context = new WebContext(routingContext);
      final OkapiHttpClient httpClient = createHttpClient(client, routingContext, context);

      requestStorageRepository = createCollectionResourceRepository(httpClient, context,
        "/request-storage/requests");
    } catch (MalformedURLException ex) {
      throw new InternalServerErrorException(ex);
    }
  }

  public CollectionResourceRepository getRequestStorageRepository() {
    return requestStorageRepository;
  }

  private OkapiHttpClient createHttpClient(
    HttpClient client, RoutingContext routingContext, WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient(WebClient.wrap(client), context,
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s", exception.toString())));
  }

  private CollectionResourceRepository createCollectionResourceRepository(
    OkapiHttpClient client, WebContext context, String rootPath) throws MalformedURLException {

    return new CollectionResourceRepository(createCollectionResourceClient(client, context, rootPath));
  }

  private CollectionResourceClient createCollectionResourceClient(
    OkapiHttpClient client, WebContext context, String rootPath) throws MalformedURLException {

    return new CollectionResourceClient(client,
      new URL(context.getOkapiLocation() + rootPath));
  }
}
