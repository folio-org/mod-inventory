package org.folio.inventory.support;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.collections4.ListUtils;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.CqlQuery;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.server.ServerErrorResponse;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.folio.inventory.support.http.server.JsonResponse.success;

public final class MoveApiUtil {
  public static final String HOLDINGS_STORAGE = "/holdings-storage/holdings";
  public static final String HOLDINGS_RECORDS_PROPERTY = "holdingsRecords";
  public static final String TARGET_TENANT_ID = "targetTenantId";

  private MoveApiUtil() { }

  public static OkapiHttpClient createHttpClient(HttpClient client, RoutingContext routingContext, WebContext context) throws MalformedURLException {
    return new OkapiHttpClient(WebClient.wrap(client), context,
      exception -> ServerErrorResponse.internalError(routingContext.response(),
        String.format("Failed to contact storage module: %s", exception.toString())));
  }


  private static MultipleRecordsFetchClient createFetchClient(CollectionResourceClient client, String propertyName) {
    return MultipleRecordsFetchClient.builder()
      .withCollectionPropertyName(propertyName)
      .withExpectedStatus(200)
      .withCollectionResourceClient(client)
      .build();
  }

  public static CollectionResourceClient createStorageClient(OkapiHttpClient client, WebContext context, String storageUrl)
    throws MalformedURLException {

    return new CollectionResourceClient(client, new URL(context.getOkapiLocation() + storageUrl));
  }

  public static CollectionResourceClient createHoldingsStorageClient(OkapiHttpClient client, WebContext context)
    throws MalformedURLException {
    return createStorageClient(client, context, HOLDINGS_STORAGE);
  }

  public static MultipleRecordsFetchClient createHoldingsRecordsFetchClient(CollectionResourceClient client) {
    return createFetchClient(client, HOLDINGS_RECORDS_PROPERTY);
  }

  public static CqlQuery fetchByIdCql(List<String> ids) {
    return CqlQuery.exactMatchAny("id", ids);
  }

  public static void successWithEmptyIds(HttpServerResponse response) {
    successWithIds(response, new ArrayList<>());
  }

  public static void successWithIds(HttpServerResponse response, List<String> ids) {
    success(response, new JsonObject().put("nonUpdatedIds", ids));
  }

  public static void respond(RoutingContext routingContext, List<String> itemIdsToUpdate, List<String> updatedItemIds) {
    List<String> nonUpdatedIds = ListUtils.subtract(itemIdsToUpdate, updatedItemIds);
    HttpServerResponse response = routingContext.response();
    if (nonUpdatedIds.isEmpty()) {
      successWithEmptyIds(response);
    } else {
      successWithIds(response, nonUpdatedIds);
    }
  }

}
