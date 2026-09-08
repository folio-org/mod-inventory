package org.folio.inventory.dataimport.handlers.matching.preloaders;

import static java.util.Objects.isNull;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.support.CqlHelper.buildMultipleValuesCqlQuery;

import io.vertx.core.json.JsonArray;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.inventory.client.OrdersClient;
import org.folio.inventory.common.Context;
import org.folio.okapi.common.XOkapiHeaders;

public class OrdersPreloaderHelper {

  private final OrdersClient ordersClient;

  public OrdersPreloaderHelper(OrdersClient ordersClient) {
    this.ordersClient = ordersClient;
  }

  public CompletableFuture<Optional<List<String>>> preload(DataImportEventPayload eventPayload,
                                                           PreloadingFields preloadingField,
                                                           List<String> loadingParameters,
                                                           Function<JsonArray, List<String>> convertPreloadResult) {
    if (isNull(loadingParameters) || loadingParameters.isEmpty()) {
      throw new IllegalArgumentException("Loading parameters for Orders preloading must not be empty");
    }

    return switch (preloadingField) {
      case POL -> {
        String cqlCondition = buildMultipleValuesCqlQuery("poLineNumber==", loadingParameters);
        yield getPoLineCollection(cqlCondition, eventPayload, convertPreloadResult);
      }
      case VRN -> {
        String cqlCondition = buildMultipleValuesCqlQuery("vendorDetail.referenceNumbers=/@refNumber ",
          loadingParameters);
        yield getPoLineCollection(cqlCondition, eventPayload, convertPreloadResult);
      }
      default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown preloading field"));
    };
  }

  private CompletableFuture<Optional<List<String>>> getPoLineCollection(
    String cql, DataImportEventPayload eventPayload,
    Function<JsonArray, List<String>> convertPreloadResult) {
    Context context = constructContext(eventPayload.getTenant(), eventPayload.getToken(), eventPayload.getOkapiUrl(),
      eventPayload.getContext().get(DataImportHeaders.USER_ID), eventPayload.getContext().get(
        XOkapiHeaders.REQUEST_ID.toLowerCase()));

    return ordersClient.getPoLineCollection(cql, context)
      .thenApply(poLines -> {
        if (poLines.isEmpty() || poLines.get().isEmpty()) {
          return Optional.empty();
        }
        return poLines.map(convertPreloadResult);
      });
  }
}
