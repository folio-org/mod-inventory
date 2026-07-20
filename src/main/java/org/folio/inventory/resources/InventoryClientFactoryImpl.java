package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.client.wrappers.SourceStorageRecordsClientWrapper;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.MoveApiUtil;

import java.net.MalformedURLException;

/**
 * Default implementation of the InventoryClientFactory.
 */
public class InventoryClientFactoryImpl implements InventoryClientFactory {

  private static final Logger LOGGER = LogManager.getLogger(InventoryClientFactoryImpl.class);

  @Override
  public MultipleRecordsFetchClient createHoldingsRecordsFetchClient(RoutingContext routingContext, WebContext context, HttpClient client) {
    try {
      CollectionResourceClient holdingsStorageClient = MoveApiUtil.createHoldingsStorageClient(
        MoveApiUtil.createHttpClient(client, routingContext, context), context);
      return MoveApiUtil.createHoldingsRecordsFetchClient(holdingsStorageClient);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Failed to create holdings records fetch client due to malformed URL", e);
    }
  }

  @Override
  public SourceStorageRecordsClientWrapper createSourceStorageRecordsClient(Context context, HttpClient client) {
    return new SourceStorageRecordsClientWrapper(
      context.getOkapiLocation(), context.getTenantId(), context.getToken(), context.getUserId(), context.getRequestId(), client);
  }

  @Override
  public CollectionResourceClient createLocationClient(Context context, HttpClient client) {
    try {
      return MoveApiUtil.createLocationStorageClient(
        MoveApiUtil.createHttpClient(client, context,
          throwable -> LOGGER.warn("createLocationClient:: Failed to contact location storage", throwable)),
        context);
    } catch (MalformedURLException e) {
      throw new RuntimeException("Failed to create location client due to malformed URL", e);
    }
  }
}
