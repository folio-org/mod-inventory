package org.folio.inventory.client;

import static java.util.Objects.isNull;

import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.exceptions.OrdersLoadingException;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.rest.acq.model.PoLineCollection;

public class OrdersClient {

    private static final String CLIENT_PREFIX = "/orders";
    private static final Logger LOGGER = LogManager.getLogger(OrdersClient.class);

    private final WebClient webClient;
    private final Function<Context, OkapiHttpClient> okapiHttpClientCreator;

    public OrdersClient(WebClient webClient) {
        this.webClient = webClient;
        this.okapiHttpClientCreator = this::createOkapiHttpClient;
    }

    public CompletableFuture<Optional<PoLineCollection>> getPoLineCollection(String cql, Context context) {
        LOGGER.trace("Trying to get InstanceId for okapi url: {}, tenantId: {}, by cql: {}",
                context.getOkapiLocation(), context.getTenantId(), cql);

        if (isNull(cql)) {
            return CompletableFuture.failedFuture(new OrdersLoadingException("Cql couldn't be null."));
        }

        OkapiHttpClient client = okapiHttpClientCreator.apply(context);

        return client.get(context.getOkapiLocation() + CLIENT_PREFIX + "/order-lines?query=" + cql)
                .toCompletableFuture()
                .thenCompose(httpResponse -> {
                    if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
                        LOGGER.debug("PurchaseOrderLine was loaded for cql '{}'", cql);
                        PoLineCollection poLineCollection = new JsonObject(httpResponse.getBody()).mapTo(PoLineCollection.class);
                        return CompletableFuture.completedFuture(Optional.ofNullable(poLineCollection));
                    } else if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                        LOGGER.warn("PurchaseOrderLine was not found by cql '{}'", cql);
                        return CompletableFuture.completedFuture(Optional.empty());
                    } else {
                        String message = String.format(
                                "Error loading PurchaseOrderLine by cql: '%s', status code: %s, response message: %s",
                                cql, httpResponse.getStatusCode(), httpResponse.getBody());
                        LOGGER.warn(message);
                        return CompletableFuture.failedFuture(new OrdersLoadingException(message));
                    }
                });
    }

    @SneakyThrows
    private OkapiHttpClient createOkapiHttpClient(Context context) {
        return new OkapiHttpClient(webClient, new URL(context.getOkapiLocation()),
                context.getTenantId(), context.getToken(), null, null, null);
    }
}
