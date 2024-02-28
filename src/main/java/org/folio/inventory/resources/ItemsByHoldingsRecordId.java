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
  private static final String RELATIONS_PARAMETER = "relations";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS = "onlyBoundWiths";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM = "onlyBoundWithsSkipDirectlyLinkedItem";

  public ItemsByHoldingsRecordId(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.get(RELATIVE_ITEMS_FOR_HOLDINGS_PATH).handler(this::getSingleTitleAndMultiTitleItems);
  }

  /**
   *  This API is dedicated to a UI Inventory use case, where a holdings record accordion
   *  lists all items associated with the given holdings record, whether they are regular,
   *  single-title items or bound-with/multi-title items.
   *  Database-wise, single title items directly reference the holdings record by a foreign key,
   *  whereas multi-title items (bound-withs) are associated through a many-to-many holdings/items
   *  relationship in a 'bound_with_part' table.
   *  This means that for a combined list of single-title and multi-title items, following
   *  look-ups are needed:
   *  1) Find any item IDs of multi-title items that contains the title of the
   *  holdings record (in bound_with_parts).
   *  2) Find the actual item records by the list of item IDs.
   *  3) Unless 'only-bound-withs' is requested, find any regular, single-title items too,
   *  by the foreign key.
   *  4) Combine the two lists of items, while eliminating any duplicate, and
   *  sort by barcode.
   *  5) Put the items through the method that decorate items for the response
   *  (i.e. resolving UUIDs and adding extra metadata).
   *  Sorting and paging
   *  Bound-withs/multi-title items must be looked up in storage by the retrieved
   *  list of UUIDs. So if a title happens to be a part of over 100 different bound-withs,
   *  then a query by a list of IDs will
   *  violate the URL length limit between mod-inventory and the Inventory storage.
   *  The look-up of items for bound-withs must therefore be partitioned, which is
   *  in turn preventing database level sorting and paging (short of using a database
   *  view for it), meaning that this class must perform the sorting itself.
   *  Paging is applied to the resulting list of items before it goes through the
   *  item embellishment.
   *
   */
  private void getSingleTitleAndMultiTitleItems(RoutingContext routingContext) {
    WebContext webContext = new WebContext(routingContext);
    String holdingsRecordId = validateRequest(routingContext, webContext);

    retrieveBoundWithItemIds(holdingsRecordId, webContext, routingContext)
      .thenCompose(itemIds -> retrieveItemsByItemIds(itemIds, holdingsRecordId,
        webContext, routingContext))
      .thenCompose(listOfItems -> maybeAppendSingleTitleItems(listOfItems, holdingsRecordId,
        webContext, routingContext))
      .thenAccept(combinedListOfItems -> sortAndRespond(combinedListOfItems,
        webContext, routingContext));
  }

  private CompletableFuture<List<String>> retrieveBoundWithItemIds(
    String holdingsRecordId, WebContext webContext, RoutingContext routingContext) {

    CollectionResourceClient boundWithPartsClient =
      getCollectionResourceRepository(
        routingContext,
        webContext,
        BOUND_WITH_PARTS_STORAGE_PATH);

    final CompletableFuture<List<String>> futureListOfItemIds = new CompletableFuture<>();

    if (boundWithPartsClient != null) {
      boundWithPartsClient.getMany("holdingsRecordId==" + holdingsRecordId,
        100000,
        0,
        response -> {
          List<String> boundWithItemIds = response.getJson()
            .getJsonArray("boundWithParts").stream()
            .map(part -> ((JsonObject) part).getString("itemId"))
            .toList();
          futureListOfItemIds.complete(boundWithItemIds);
        });
    }
    return futureListOfItemIds;
  }

  private CompletableFuture<List<Item>> retrieveItemsByItemIds(
    List<String> boundWithItemIds, String holdingsRecordId, WebContext webContext,
    RoutingContext routingContext) {

    return buildPartitionedItemFetchClient(routingContext)
      .find(boundWithItemIds, this::cqlMatchByListOfIds)
      .thenApply(listOfJsonObjects -> listOfItemJsonsToListOfItems(listOfJsonObjects,
        holdingsRecordId, webContext));
  }

  private List<Item> listOfItemJsonsToListOfItems(
    List<JsonObject> listOfItemJsons, String holdingsRecordId, WebContext webContext) {

    return listOfItemJsons.stream()
      .map(this::mapItemFromJson)
      .filter(item -> !(skippingDirectlyLinkedItemRequested(webContext)
        && item.getHoldingId().equals(holdingsRecordId)))
      .toList();
  }

  private CompletableFuture<List<Item>> maybeAppendSingleTitleItems(
    List<Item> itemList, String holdingsRecordId, WebContext webContext,
    RoutingContext routingContext) {
    final CompletableFuture<List<Item>> futureListOfItems = new CompletableFuture<>();

    if (onlyBoundWithsRequested(webContext)) {
      futureListOfItems.complete(itemList);
    } else {
      String itemQuery = "holdingsRecordId==" + holdingsRecordId;
      try {
        storage.getItemCollection(webContext)
          .findByCql(itemQuery,
            new PagingParameters(1000000, 0),
            result -> {
              Map<String, Item> mappedItems = itemList.stream()
                .collect(Collectors.toMap(Item::getId, item -> item));
              List<Item> combinedList = new ArrayList<>(itemList);
              for (Item item : result.getResult().records) {
                if (!mappedItems.containsKey(item.getId())) {
                  combinedList.add(item);
                }
              }
              futureListOfItems.complete(combinedList);
            },
            FailureResponseConsumer.serverError(routingContext.response()));
      } catch (UnsupportedEncodingException uee) {
        ServerErrorResponse.internalError(routingContext.response(), uee.toString());
      }
    }
    return futureListOfItems;
  }

  private void sortAndRespond(List<Item> itemList, WebContext webContext, RoutingContext routingContext) {
    PagingParameters pagingParameters = getPagingParameters(webContext);
    List<Item> sortedList = itemList.stream().sorted(
      Comparator.comparing(i -> i.getBarcode() == null ? "~" : i.getBarcode()))
      .toList();
    MultipleRecords<Item> items = new MultipleRecords<>(getPage(sortedList,
      pagingParameters.offset, pagingParameters.limit), itemList.size());
    respondWithManyItems(routingContext, webContext, items);
  }

  private String validateRequest(RoutingContext routingContext, WebContext context) {
    String queryByHoldingsRecordId = context.getStringParameter("query", null);
    String relationsParam = context.getStringParameter(RELATIONS_PARAMETER, null);

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
    String relationsParam = webContext.getStringParameter(RELATIONS_PARAMETER, null);
    return relationsParam != null &&
      (relationsParam.equals(RELATION_PARAM_ONLY_BOUND_WITHS) ||
        relationsParam.equals(
          RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM));
  }

  private static boolean skippingDirectlyLinkedItemRequested(WebContext webContext) {
    String relationsParam = webContext.getStringParameter(RELATIONS_PARAMETER, null);
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

  private CqlQuery cqlMatchByListOfIds(List<String> ids) {
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
