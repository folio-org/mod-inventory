package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.inventory.support.CqlHelper.buildQueryByIds;

public class ItemsByHoldingsRecordId extends Items
{
  private static final Logger log = LogManager.getLogger( MethodHandles.lookup().lookupClass());

  private static final String RELATIVE_ITEMS_FOR_HOLDINGS_PATH = "/inventory/items-by-holdings-id";
  // Supporting API
  private static final String BOUND_WITH_PARTS_STORAGE_PATH = "/inventory-storage/bound-with-parts";
  private static final String ITEM_STORAGE_PATH = "/item-storage/items";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS  = "onlyBoundWiths";
  private static final String RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM = "onlyBoundWithsSkipDirectlyLinkedItem";

  public ItemsByHoldingsRecordId( final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register( Router router )
  {
    router.get( RELATIVE_ITEMS_FOR_HOLDINGS_PATH ).handler(this::getBoundWithItems );
  }

  private void getBoundWithItems( RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String queryByHoldingsRecordId = context.getStringParameter("query", null);
    String relationsParam = context.getStringParameter( "relations", null );

    if (queryByHoldingsRecordId == null || !queryByHoldingsRecordId.contains( "holdingsRecordId" )) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Items must be retrieved by 'holdingsRecordId' from this API. Query was: " + queryByHoldingsRecordId);
      return;
    }

    if (relationsParam != null) {
      if (! Arrays.asList(RELATION_PARAM_ONLY_BOUND_WITHS, RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM).contains( relationsParam )) {
        ClientErrorResponse.badRequest(routingContext.response(),
          "The only valid values of the request parameter 'relations' are: '"
            + RELATION_PARAM_ONLY_BOUND_WITHS + "' and '" +
            RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM +"'");
        return;
      }
    }

    String[] keyVal = queryByHoldingsRecordId.replaceAll("[()]","").split( "[=]{1,2}" );

    if (keyVal.length != 2) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Items must be retrieved by 'holdingsRecordId' from this API: query=holdingsRecordId==[a UUID]. Query was: " + queryByHoldingsRecordId);
      return;
    }

    String holdingsRecordId = keyVal[1];

    CollectionResourceClient itemsClient =
      getCollectionResourceRepository( routingContext, context, ITEM_STORAGE_PATH );
    itemsClient.getMany("holdingsRecordId=="+holdingsRecordId,
      1000,
      0,
      response -> {
        List<String> holdingsRecordsItemIds = response.getJson()
          .getJsonArray( "items" ).stream()
          .map(item -> ((JsonObject) item).getString("id"))
          .collect( Collectors.toList());

        CollectionResourceClient boundWithPartsClient =
          getCollectionResourceRepository(
            routingContext,
            context,
            BOUND_WITH_PARTS_STORAGE_PATH);

        MultipleRecordsFetchClient itemsFetcher = MultipleRecordsFetchClient.builder()
          .withCollectionPropertyName("boundWithParts")
          .withExpectedStatus(200)
          .withCollectionResourceClient(boundWithPartsClient)
          .build();

        BoundWithPartsCql boundWithPartsCql = new BoundWithPartsCql(holdingsRecordId);
        itemsFetcher.find(holdingsRecordsItemIds, boundWithPartsCql::byHoldingsRecordIdOrListOfItemIds)
            .thenAccept(boundWithParts ->
              joinAndRespondWithManyItems(routingContext, context, boundWithParts, holdingsRecordId, relationsParam));
      });
  }

  private void joinAndRespondWithManyItems(RoutingContext routingContext,
                                           WebContext webContext,
                                           List<JsonObject> boundWithParts,
                                           String holdingsRecordId,
                                           String relationsParam) {
    String itemQuery;
    List<String> itemIds;
    boolean onlyBoundWiths = relationsParam != null &&
      ( relationsParam.equals(RELATION_PARAM_ONLY_BOUND_WITHS ) ||
        relationsParam.equals(
          RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM ) );
    boolean skipDirectlyLinkedItem = relationsParam != null && relationsParam.equals(
      RELATION_PARAM_ONLY_BOUND_WITHS_SKIP_DIRECTLY_LINKED_ITEM );

    itemIds = boundWithParts.stream()
      .map( part -> part.getString( "itemId" ) )
      .collect( Collectors.toList() );

    boolean boundWithsFound = itemIds.size()>0;
    if (boundWithsFound) {
      itemQuery = buildQueryByIds( itemIds );
      if (skipDirectlyLinkedItem)
      {
        itemQuery += " and holdingsRecordId <>" + holdingsRecordId;
      } else if (! onlyBoundWiths) {
        itemQuery +=  " or holdingsRecordId==" + holdingsRecordId;
      }
    } else {
      if ( onlyBoundWiths )
      {
        itemQuery =  "id==(NOOP)";
      }
      else
      {
        itemQuery = "holdingsRecordId==" + holdingsRecordId;
      }
    }
    try {
      storage.getItemCollection(webContext).findByCql(itemQuery,
        new PagingParameters(1000,0), success ->
          respondWithManyItems(routingContext, webContext, success.getResult()),
        FailureResponseConsumer.serverError(routingContext.response()));
    } catch (UnsupportedEncodingException e) {
      ServerErrorResponse.internalError(routingContext.response(), e.toString());
    }
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
    } catch ( MalformedURLException mfue) {
      log.error(mfue);
    }
    return collectionResourceClient;
  }

  protected OkapiHttpClient createHttpClient(
    RoutingContext routingContext,
    WebContext context)
    throws MalformedURLException {

    return new OkapiHttpClient( WebClient.wrap(client), context,
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        format("Failed to contact storage module: %s",
          exception.toString())));
  }

  static class BoundWithPartsCql {
    private final String holdingsId;

    public BoundWithPartsCql(String holdingsRecordId) {
      this.holdingsId = holdingsRecordId;
    }

    public CqlQuery byHoldingsRecordIdOrListOfItemIds(List<String> itemIds) {
      return CqlQuery.exactMatchAny("id", itemIds)
        .or(CqlQuery.exactMatch("holdingsRecordId", this.holdingsId));

    }
  }


}
