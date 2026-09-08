package org.folio.inventory.client;

import static org.folio.HttpStatus.SC_NOT_FOUND;
import static org.folio.HttpStatus.SC_NO_CONTENT;
import static org.folio.HttpStatus.SC_OK;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.InstanceLinkDtoCollection;
import org.folio.inventory.common.Context;
import org.folio.inventory.exceptions.InstanceLinksException;
import org.folio.inventory.support.http.client.OkapiHttpClient;

public class InstanceLinkClient {

  private static final Logger LOGGER = LogManager.getLogger(InstanceLinkClient.class);

  private static final String LINKS_PATH = "/links/instances";
  private static final String LINKS_INSTANCE_PATH = LINKS_PATH + "/%s";

  private final WebClient webClient;
  private final Function<Context, OkapiHttpClient> okapiHttpClientCreator;

  public InstanceLinkClient(WebClient webClient) {
    this.webClient = webClient;
    this.okapiHttpClientCreator = this::createOkapiHttpClient;
  }

  public CompletableFuture<Optional<InstanceLinkDtoCollection>> getLinksByInstanceId(String instanceId,
                                                                                     Context context) {
    LOGGER.trace("getLinksByInstanceId: instanceId: {}", instanceId);
    var client = okapiHttpClientCreator.apply(context);
    var url = context.getOkapiLocation() + LINKS_INSTANCE_PATH.formatted(instanceId);
    return client.get(url)
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == SC_OK) {
          LOGGER.info("getLinksByInstanceId: InstanceLinkDtoCollection loaded for instanceId '{}'", instanceId);
          var dto = Json.decodeValue(httpResponse.body(), InstanceLinkDtoCollection.class);
          return CompletableFuture.completedFuture(Optional.of(dto));
        } else if (httpResponse.statusCode() == SC_NOT_FOUND) {
          LOGGER.warn("getLinksByInstanceId: InstanceLinkDtoCollection not found for instanceId '{}'", instanceId);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          var message = String.format("getLinksByInstanceId: Error for instanceId '%s', status code: %d, response: %s",
            instanceId,
            httpResponse.statusCode(),
            httpResponse.body());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new InstanceLinksException(message));
        }
      });
  }

  public CompletableFuture<Void> updateInstanceLinks(String instanceId,
                                                     InstanceLinkDtoCollection instanceLinkCollection,
                                                     Context context) {
    LOGGER.trace("updateInstanceLinks: instanceId: {}", instanceId);
    var client = okapiHttpClientCreator.apply(context);
    var url = context.getOkapiLocation() + LINKS_INSTANCE_PATH.formatted(instanceId);
    return client.put(url, JsonObject.mapFrom(instanceLinkCollection))
      .toCompletableFuture()
      .thenAccept(httpResponse -> {
        if (httpResponse.statusCode() == SC_NO_CONTENT) {
          LOGGER.info("updateInstanceLinks: InstanceLinkDtoCollection updated for instanceId '{}'", instanceId);
        } else {
          var message =
            String.format("updateInstanceLinks: Error updating for instanceId '%s', status: %d, response: %s",
              instanceId,
              httpResponse.statusCode(),
              httpResponse.body());
          LOGGER.warn(message);
        }
      });
  }

  @SneakyThrows
  private OkapiHttpClient createOkapiHttpClient(Context context) {
    return new OkapiHttpClient(webClient,
      new URI(context.getOkapiLocation()).toURL(),
      context.getTenantId(),
      context.getToken(),
      context.getUserId(),
      context.getRequestId(),
      null);
  }
}
