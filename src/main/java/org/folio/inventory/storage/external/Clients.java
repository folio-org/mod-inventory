package org.folio.inventory.storage.external;

import java.net.MalformedURLException;
import java.net.URL;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.exceptions.InternalServerErrorException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.RoutingContext;

public final class Clients {
  private final CollectionResourceClient requestStorageClient;

  /**
   * @param routingContext - Routing context.
   * @param client         - Http client.
   * @throws InternalServerErrorException - in case a URL parse exception occurred.
   */
  public Clients(RoutingContext routingContext, HttpClient client) {
    try {
      final WebContext context = new WebContext(routingContext);
      final OkapiHttpClient httpClient = createHttpClient(client, routingContext, context);

      requestStorageClient = createCollectionResourceClient(httpClient, context,
        "/request-storage/requests");
    } catch (MalformedURLException ex) {
      throw new InternalServerErrorException(ex);
    }
  }

  public CollectionResourceClient getRequestStorageClient() {
    return requestStorageClient;
  }

  private OkapiHttpClient createHttpClient(
    HttpClient client, RoutingContext routingContext, WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient(client, context,
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s", exception.toString())));
  }

  private CollectionResourceClient createCollectionResourceClient(
    OkapiHttpClient client, WebContext context, String rootPath) throws MalformedURLException {

    return new CollectionResourceClient(client,
      new URL(context.getOkapiLocation() + rootPath));
  }
}
