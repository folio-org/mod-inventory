package org.folio.inventory.client;

import static java.util.Objects.isNull;
import static org.folio.HttpStatus.SC_NOT_FOUND;
import static org.folio.HttpStatus.SC_OK;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.exceptions.OrdersLoadingException;
import org.folio.inventory.support.http.client.OkapiHttpClient;

public class OrdersClient {

  private static final Logger LOGGER = LogManager.getLogger(OrdersClient.class);

  private static final String ORDERS_PATH = "/orders";
  private static final String ORDER_LINES_PATH = ORDERS_PATH + "/order-lines";
  private static final String PO_LINES_FIELD = "poLines";

  private final WebClient webClient;
  private final Function<Context, OkapiHttpClient> okapiHttpClientCreator;

  public OrdersClient(WebClient webClient) {
    this.webClient = webClient;
    this.okapiHttpClientCreator = this::createOkapiHttpClient;
  }

  public CompletableFuture<Optional<JsonArray>> getPoLineCollection(String cql, Context context) {
    LOGGER.trace("Trying to get PoLineCollection for by cql: {}", cql);

    if (isNull(cql)) {
      return CompletableFuture.failedFuture(new OrdersLoadingException("Cql couldn't be null."));
    }

    var client = okapiHttpClientCreator.apply(context);

    return client.get(context.getOkapiLocation() + ORDER_LINES_PATH, Map.of("query", cql))
      .toCompletableFuture()
      .thenCompose(httpResponse -> {
        if (httpResponse.statusCode() == SC_OK) {
          LOGGER.debug("PurchaseOrderLine was loaded for cql '{}'", cql);
          JsonArray poLines = new JsonObject(httpResponse.body()).getJsonArray(PO_LINES_FIELD);
          return CompletableFuture.completedFuture(Optional.ofNullable(poLines));
        } else if (httpResponse.statusCode() == SC_NOT_FOUND) {
          LOGGER.warn("PurchaseOrderLine was not found by cql '{}'", cql);
          return CompletableFuture.completedFuture(Optional.empty());
        } else {
          String message = String.format(
            "Error loading PurchaseOrderLine by cql: '%s', status code: %s, response message: %s",
            cql, httpResponse.statusCode(), httpResponse.body());
          LOGGER.warn(message);
          return CompletableFuture.failedFuture(new OrdersLoadingException(message));
        }
      });
  }

  @SneakyThrows
  private OkapiHttpClient createOkapiHttpClient(Context context) {
    return new OkapiHttpClient(webClient, new URI(context.getOkapiLocation()).toURL(),
      context.getTenantId(), context.getToken(), context.getUserId(), context.getRequestId(), null);
  }
}
