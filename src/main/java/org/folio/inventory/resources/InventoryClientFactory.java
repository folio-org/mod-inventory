package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;

/**
 * A factory for creating various HTTP clients used for inventory operations.
 * This abstraction allows for easier testing by mocking client creation.
 */
public interface InventoryClientFactory {

  /**
   * Creates a client for fetching multiple holdings records.
   *
   * @param routingContext The routing context of the current request.
   * @param context        The web context containing tenant and token information.
   * @param client         The shared HttpClient instance.
   * @return A configured MultipleRecordsFetchClient for holdings records.
   */
  MultipleRecordsFetchClient createHoldingsRecordsFetchClient(RoutingContext routingContext, WebContext context, HttpClient client);

  /**
   * Creates a client for interacting with the Source Record Storage (SRS) API.
   *
   * @param context The context containing tenant, token, and user information.
   * @param client  The shared HttpClient instance.
   * @return A configured SourceStorageRecordsClientWrapper.
   */
  SourceStorageRecordsClientWrapper createSourceStorageRecordsClient(Context context, HttpClient client);

}
