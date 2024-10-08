package org.folio.inventory.resources;

import static java.lang.String.format;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.folio.inventory.support.CqlHelper.multipleRecordsCqlQuery;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.TenantItem;
import org.folio.TenantItemPair;
import org.folio.TenantItemPairCollection;
import org.folio.TenantItemResponse;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.support.JsonArrayHelper;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * Resource that allows to get Inventory items from multiple tenants at once.
 * User should have an affiliation in order to be able to retrieve items from the corresponding tenant.
 */
public class TenantItems {

  private static final Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private static final String TENANT_ITEMS_PATH = "/inventory/tenant-items";
  public static final String ITEMS_FIELD = "items";
  public static final String ITEM_FIELD = "item";
  public static final String TOTAL_RECORDS_FIELD = "totalRecords";
  public static final String TENANT_ID_FIELD = "tenantId";

  private final HttpClient client;

  public TenantItems(HttpClient client) {
    this.client = client;
  }

  public void register(Router router) {
    router.post(TENANT_ITEMS_PATH + "*").handler(BodyHandler.create());
    router.post(TENANT_ITEMS_PATH).handler(this::getItemsFromTenants);
  }

  /**
   *  This API is meant to be used by UI to fetch different items from several
   *  tenants together within one call
   *
   */
  private void getItemsFromTenants(RoutingContext routingContext) {
    var getItemsFutures = routingContext.body().asPojo(TenantItemPairCollection.class)
      .getTenantItemPairs().stream()
      .collect(groupingBy(TenantItemPair::getTenantId, mapping(TenantItemPair::getItemId, toList())))
      .entrySet().stream()
      .map(tenantToItems -> getItemsWithTenantId(tenantToItems.getKey(), tenantToItems.getValue(), routingContext))
      .toList();

    CompletableFuture.allOf(getItemsFutures.toArray(new CompletableFuture[0]))
      .thenApply(v -> getItemsFutures.stream()
        .map(CompletableFuture::join)
        .flatMap(List::stream)
        .toList())
      .thenApply(items -> new TenantItemResponse().withTenantItems(items).withTotalRecords(items.size()))
      .thenAccept(response -> JsonResponse.success(routingContext.response(), JsonObject.mapFrom(response)));
  }

  private CompletableFuture<List<TenantItem>> getItemsWithTenantId(String tenantId, List<String> itemIds, RoutingContext routingContext) {
    LOG.info("getItemsWithTenantId:: Fetching items - {} from tenant - {}", itemIds, tenantId);
    var context = new WebContext(routingContext);
    CollectionResourceClient itemsStorageClient;
    try {
      OkapiHttpClient okapiClient = createHttpClient(tenantId, context, routingContext);
      itemsStorageClient = createItemsStorageClient(okapiClient, context);
    }
    catch (MalformedURLException e) {
      invalidOkapiUrlResponse(routingContext, context);
      return CompletableFuture.completedFuture(List.of());
    }

    var getByIdsQuery = multipleRecordsCqlQuery(itemIds);
    var itemsFetched = new CompletableFuture<Response>();
    itemsStorageClient.getAll(getByIdsQuery, itemsFetched::complete);

    return itemsFetched
      .thenApply(this::getItems)
      .thenApply(items -> items.stream()
        .map(item -> new TenantItem()
          .withAdditionalProperty(ITEM_FIELD, item)
          .withAdditionalProperty(TENANT_ID_FIELD, tenantId))
        .toList());
  }

  private List<JsonObject> getItems(Response response) {
    if (response.getStatusCode() != HttpStatus.SC_OK || !response.hasBody()) {
      return List.of();
    }
    return JsonArrayHelper.toList(response.getJson(), ITEMS_FIELD);
  }

  private CollectionResourceClient createItemsStorageClient(OkapiHttpClient client, WebContext context) throws MalformedURLException {
    return new CollectionResourceClient(client, new URL(context.getOkapiLocation() + "/item-storage/items"));
  }

  private OkapiHttpClient createHttpClient(String tenantId, WebContext context,
                                             RoutingContext routingContext) throws MalformedURLException {
    return new OkapiHttpClient(WebClient.wrap(client),
      URI.create(context.getOkapiLocation()).toURL(),
      Optional.ofNullable(tenantId).orElse(context.getTenantId()),
      context.getToken(),
      context.getUserId(),
      context.getRequestId(),
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        format("Failed to contact storage module: %s", exception.toString())));
  }

  private void invalidOkapiUrlResponse(RoutingContext routingContext, WebContext context) {
    ServerErrorResponse.internalError(routingContext.response(),
      String.format("Invalid Okapi URL: %s", context.getOkapiLocation()));
  }

}
