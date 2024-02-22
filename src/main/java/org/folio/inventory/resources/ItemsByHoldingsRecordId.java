package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class ItemsByHoldingsRecordId extends Items {
  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private static final String RELATIVE_ITEMS_FOR_HOLDINGS_PATH = "/inventory/items-by-holdings-id";
  // Supporting API
  private static final String BOUND_WITH_PARTS_STORAGE_PATH = "/inventory-storage/bound-with-parts";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS = "onlyBoundWiths";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM = "onlyBoundWithsSkipDirectlyLinkedItem";

  public ItemsByHoldingsRecordId(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.get(RELATIVE_ITEMS_FOR_HOLDINGS_PATH).handler(this::getAndRespondWithItems);
  }

  /**
   *  This API is dedicated to a UI Inventory use case that, in a holdings record accordion,
   *  lists any items associated with the given holdings record, whether they are regular,
   *  single-title items or bound-with/multi-title items.
   *
   *  Database-wise, single title items directly reference the holdings record by a foreign key,
   *  whereas multi-title items (bound-withs) are associated through a many-to-many holdings/items
   *  relationship in a 'bound_with_part' table.
   *
   *  This means that for a combined list of single-title and multi-title items, following
   *  look-ups are needed:
   *
   *  1) Find any item IDs of multi-title items that contains the title of the
   *  holdings record (in bound_with_parts).
   *  2) Find the actual item records by the list of item IDs, and sort by barcode.
   *  3) Unless 'only-bound-withs' is requested, find any regular, single title items too,
   *  by the foreign key.
   *  4) Combine the two lists of items while eliminating a potential duplicate
   *  (the same holdings record can be referenced by the same item in both ways,
   *  if it's a bound-with.
   *  5) Decorate the items of the list in the same way as is done in /inventory/items -
   *  i.e. resolving UUIDs and adding extra metadata.
   *
   *  Sorting and paging
   *
   *  Regular, single-title items are retrieved by a simple query by holdingsRecordId
   *  and is sorted by the requested sort key, UI Inventory requests barcode sorting.
   *  Therefore, sorting is done by the database.
   *
   *  Bound-withs/multi-title items however, must be looked up in storage by the retrieved
   *  list of UUIDs. Thus, in the very rare, yet observed, use case where the same
   *  title/holdings record appears in 100+ bound-withs, a query by a list of IDs will
   *  violate the URL length limit between mod-inventory and the Inventory storage.
   *  The look-up of items for bound-withs must therefore be partitioned, which is
   *  in turn preventing database level sorting and paging (short of using a database
   *  view for it).
   *
   *  This is the applied sorting: Bound-with items, of which there are rarely more
   *  than a few for a holdings record, are sorted by barcode, and if the occasional
   *  holdings record enumerates both single title items and bound-with items, then
   *  the bound-withs will come first. The single title items, of which there can be
   *  quite a lot under a given holdings record -- up to 24,000 has been observed --
   *  are sorted as supported/requested.
   *
   *  Paging is applied to the resulting list of items before it goes through the
   *  item embellishment.
   *
   */
  private void getAndRespondWithItems(RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);
    String holdingsRecordId = validateRequest(routingContext, context);
    retrieveBoundWithItemIds(routingContext, context, holdingsRecordId);
  }

  private void retrieveBoundWithItemIds(RoutingContext routingContext, WebContext webContext, String holdingsRecordId) {
    CollectionResourceClient boundWithPartsClient =
      getCollectionResourceRepository(
        routingContext,
        webContext,
        BOUND_WITH_PARTS_STORAGE_PATH);

    if (boundWithPartsClient != null) {
      boundWithPartsClient.getMany("holdingsRecordId==" + holdingsRecordId,
        100000,
        0,
        response -> {
          List<String> boundWithItemIds = response.getJson()
            .getJsonArray("boundWithParts").stream()
            .map(part -> ((JsonObject) part).getString("itemId"))
            .toList();
          retrieveBoundWithItemsByItemIds(
            routingContext, webContext, holdingsRecordId, boundWithItemIds);
        });
    }
  }

  private void retrieveBoundWithItemsByItemIds(RoutingContext routingContext,
                                               WebContext webContext,
                                               String holdingsRecordId,
                                               List<String> boundWithItemIds) {
    buildPartitionedItemFetchClient(routingContext)
      .find(boundWithItemIds, this::cqlMatchAnyByIds)
      .thenCompose(boundWithItems -> respondWithBoundWithsOrGoForSingleTitleItemsToo(
        routingContext, webContext, holdingsRecordId, boundWithItems));
  }

  private CompletableFuture<Void> respondWithBoundWithsOrGoForSingleTitleItemsToo(RoutingContext routingContext, WebContext webContext, String holdingsRecordId, List<JsonObject> boundWithItems) {

    List<Item> itemList = boundWithItems.stream()
      .sorted(Comparator.comparing(j -> j.getString("barcode", "~")))
      .map(this::mapItemFromJson)
      .filter(item -> !(skippingDirectlyLinkedItemRequested(webContext) && item.getHoldingId().equals(holdingsRecordId)))
      .toList();

    if (onlyBoundWithsRequested(webContext)) {
      MultipleRecords<Item> finalItems = new MultipleRecords<>(itemList, itemList.size());
      respondWithManyItems(routingContext, webContext, finalItems);
    } else {
      addSingleTitleItemsAndRespond(routingContext, webContext, holdingsRecordId, itemList);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void addSingleTitleItemsAndRespond(
    RoutingContext routingContext, WebContext webContext, String holdingsRecordId,
    List<Item> itemList) {
    String itemQuery = "holdingsRecordId==" + holdingsRecordId;
    PagingParameters pagingParameters = getPagingParameters(webContext);
    try {
      storage.getItemCollection(webContext)
        .findByCql(itemQuery, new PagingParameters(1000000, 0), result -> {
            Map<String, Item> mappedItems = itemList.stream()
              .collect(Collectors.toMap(Item::getId, item -> item));

            List<Item> finalList = new ArrayList<>(itemList);
            for (Item item : result.getResult().records) {
              if (!mappedItems.containsKey(item.getId())) {
                finalList.add(item);
              }
            }
            MultipleRecords<Item> finalItems = new MultipleRecords<>(getPage(finalList,
                pagingParameters.offset, pagingParameters.limit), finalList.size());
            respondWithManyItems(routingContext, webContext, finalItems);
          },
          FailureResponseConsumer.serverError(routingContext.response()));
    } catch (UnsupportedEncodingException e) {
      ServerErrorResponse.internalError(routingContext.response(), e.toString());
    }
  }

  private String validateRequest(RoutingContext routingContext, WebContext context) {
    String queryByHoldingsRecordId = context.getStringParameter("query", null);
    String relationsParam = context.getStringParameter("relations", null);

    if (queryByHoldingsRecordId == null || !queryByHoldingsRecordId.contains("holdingsRecordId")) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Items must be retrieved by 'holdingsRecordId' from this API. Query was: " + queryByHoldingsRecordId);
      return "";
    }

    if (relationsParam != null) {
      if (!Arrays.asList(RELATION_PARAM_ONLY_BOUND_WITHS, RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM).contains(relationsParam)) {
        ClientErrorResponse.badRequest(routingContext.response(),
          "The only valid values of the request parameter 'relations' are: '"
            + RELATION_PARAM_ONLY_BOUND_WITHS + "' and '" +
            RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM + "'");
        return "";
      }
    }

    String[] keyVal = queryByHoldingsRecordId.replaceAll("[()]", "").split("[=]{1,2}");

    if (keyVal.length != 2) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Items must be retrieved by 'holdingsRecordId' from this API: query=holdingsRecordId==[a UUID]. Query was: " + queryByHoldingsRecordId);
      return "";
    }
    return keyVal[1];
  }

  private static boolean onlyBoundWithsRequested(WebContext webContext) {
    String relationsParam = webContext.getStringParameter("relations", null);
    return relationsParam != null &&
      (relationsParam.equals(RELATION_PARAM_ONLY_BOUND_WITHS) ||
        relationsParam.equals(
          RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM));
  }

  private static boolean skippingDirectlyLinkedItemRequested(WebContext webContext) {
    String relationsParam = webContext.getStringParameter("relations", null);
    return relationsParam != null && relationsParam.equals(
      RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM);
  }

  /**
   * Ensures a limit on the page size of no more than 10,000 items at a time,
   * defaults to a limit of 200 items for a page.
   *
   * @param context The web context to extract requested paging from
   * @return Adapted paging parameters.
   */
  private PagingParameters getPagingParameters(WebContext context) {
    final String maxPageSize = "10000";
    String limit = context.getStringParameter("limit", "200");
    String offset = context.getStringParameter("offset", "0");
    if (!StringUtils.isNumeric(limit) || StringUtils.isEmpty(limit)) {
      limit = "200";
    } else if (Integer.parseInt(limit) > Integer.parseInt(maxPageSize)) {
      log.error("A paging of {} items was requested but the /items-by-holdings-id API cuts off the page at {} items.", limit, maxPageSize);
      limit = maxPageSize;
    }
    if (!StringUtils.isNumeric(offset) || StringUtils.isEmpty(offset)) {
      offset = "0";
    }
    PagingParameters enforcedPaging = new PagingParameters(Integer.parseInt(limit), Integer.parseInt(offset));
    log.debug("Paging resolved to limit: {}, offset: {}", enforcedPaging.limit, enforcedPaging.offset);
    return enforcedPaging;
  }

  private static <T> List<T> getPage(List<T> sourceList, int offset, int limit) {
    if (sourceList == null || sourceList.size() <= offset) {
      return Collections.emptyList();
    }
    return sourceList.subList(offset, Math.min(offset + limit, sourceList.size()));
  }

  protected Item mapItemFromJson(JsonObject itemFromServer) {
    return ItemUtil.fromStoredItemRepresentation(itemFromServer);
  }

  private MultipleRecordsFetchClient buildPartitionedItemFetchClient(
    RoutingContext routingContext) {
    WebContext webContext = new WebContext(routingContext);

    CollectionResourceClient baseClient = null;
    try {
      URL api = new URL(webContext.getOkapiLocation() + "/item-storage/items");
      baseClient =
        new CollectionResourceClient(
          createHttpClient(routingContext, webContext), api);
    } catch (MalformedURLException mue) {
      log.error(String.format(
          "Could not create CollectionResourceClient due to malformed URL %s%s",
          webContext.getOkapiLocation(), "/item-storage/items"));
    }
    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName("items")
      .withExpectedStatus(200)
      .withCollectionResourceClient(baseClient)
      .build();
  }

  private CqlQuery cqlMatchAnyByIds(List<String> ids) {
    return CqlQuery.exactMatchAny("id", ids);
  }


  private CollectionResourceClient getCollectionResourceRepository(
    RoutingContext routingContext, WebContext context, String path) {
    CollectionResourceClient collectionResourceClient = null;
    try {
      OkapiHttpClient okapiClient = createHttpClient(routingContext, context);
      collectionResourceClient
        = new CollectionResourceClient(
        okapiClient,
        new URL(context.getOkapiLocation() + path));
    } catch (MalformedURLException mfue) {
      log.error(mfue);
    }
    return collectionResourceClient;
  }

  protected OkapiHttpClient createHttpClient(
    RoutingContext routingContext,
    WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient(WebClient.wrap(client), context,
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        format("Failed to contact storage module: %s", exception.toString())));
  }

}
