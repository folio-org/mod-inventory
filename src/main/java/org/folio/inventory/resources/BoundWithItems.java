package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
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
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.FailureResponseConsumer;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.inventory.support.CqlHelper.buildQueryByIds;

public class BoundWithItems extends Items
{
  private static final Logger log = LogManager.getLogger( MethodHandles.lookup().lookupClass());

  private static final String RELATIVE_BOUND_WITH_ITEMS_PATH = "/inventory/bound-with-items";
  // Supporting API
  private static final String BOUND_WITH_PARTS_STORAGE_PATH = "/inventory-storage/bound-with-parts";
  private static final String BOUND_WITH_PARTS_JSON_ARRAY = "boundWithParts";
  private static final int STATUS_SUCCESS = 200;

  public BoundWithItems(final Storage storage, final HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register( Router router )
  {
    router.get(RELATIVE_BOUND_WITH_ITEMS_PATH).handler(this::getBoundWithItems );
  }

  private void getBoundWithItems( RoutingContext routingContext) {
    WebContext context = new WebContext(routingContext);

    String query = context.getStringParameter("query", null);
    String skipDirectlyLinkedItem = context.getStringParameter( "skipDirectlyLinkedItem", null );
    boolean excludeDirectlyLinkedItem = skipDirectlyLinkedItem != null && !skipDirectlyLinkedItem.equalsIgnoreCase( "false" );

    if (query == null) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Bound-with Items must be retrieved by 'holdingsRecordId' or 'itemId'. Request for all bound-with Items is not supported");
      return;
    }

    if (excludeDirectlyLinkedItem && !query.contains( "holdingsRecordId=" )) {
      ClientErrorResponse.badRequest(routingContext.response(),
        "Incompatible combination of request parameters: skipping the item directly linked to the holdings record only applies when looking for bound-with Items by holdingsRecordId");
      return;
    }

    CollectionResourceClient boundWithPartsClient =
      getCollectionResourceRepository(
        routingContext,
        context,
        BOUND_WITH_PARTS_STORAGE_PATH);

    boundWithPartsClient.getMany(query,
        1000,
        0,
        response -> joinAndRespondWithManyItems( routingContext, context, response, excludeDirectlyLinkedItem ));
  }

  private void joinAndRespondWithManyItems(RoutingContext routingContext,
                                           WebContext webContext,
                                           Response boundWithParts,
                                           boolean skipDirectlyLinkedItem) {
    String itemQuery = "id==(NOOP)";
    List<String> itemIds = new ArrayList<>();

    JsonObject boundWithPartsJson = boundWithParts.getJson();

    JsonArray boundWithPartRecords =
      boundWithPartsJson.getJsonArray( BOUND_WITH_PARTS_JSON_ARRAY );

    if (!boundWithPartRecords.isEmpty())
    {
      itemIds = boundWithPartRecords.stream()
        .map( o -> (JsonObject) o )
        .map( part -> part.getString( "itemId" ) )
        .collect( Collectors.toList() );

      String holdingsRecordId = boundWithPartRecords
        .getJsonObject( 0 )
        .getString( "holdingsRecordId" );

      itemQuery = buildQueryByIds( itemIds )
        + ( skipDirectlyLinkedItem ? " and holdingsRecordId <>" + holdingsRecordId : "" );
    }
    try {
      storage.getItemCollection(webContext).findByCql(itemQuery,
          new PagingParameters(itemIds.size(),0), success ->
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

}
