package org.folio.inventory.storage.external;

import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.HoldingCollection;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.ingest.IngestJobCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;
import org.folio.inventory.storage.memory.InMemoryIngestJobCollection;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;

public class ExternalStorageCollections implements CollectionProvider {
  private final Vertx vertx;
  private final String baseAddress;
  private final HttpClient client;
  private static final InMemoryIngestJobCollection ingestJobCollection = new InMemoryIngestJobCollection();

  public ExternalStorageCollections(Vertx vertx, String baseAddress, HttpClient client) {
    this.vertx = vertx;
    this.baseAddress = baseAddress;
    this.client = client;
  }

  @Override
  public ItemCollection getItemCollection(String tenantId, String token) {
    return new ExternalStorageModuleItemCollection(vertx, baseAddress,
      tenantId, token, client);
  }

  @Override
  public HoldingCollection getHoldingCollection(String tenantId, String token) {
    return new ExternalStorageModuleHoldingCollection(vertx, baseAddress,
      tenantId, token, client);
  }

  @Override
  public HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token) {
    return new ExternalStorageModuleHoldingsRecordCollection(vertx, baseAddress,
      tenantId, token, client);
  }

  @Override
  public InstanceCollection getInstanceCollection(String tenantId, String token) {
    return new ExternalStorageModuleInstanceCollection(vertx, baseAddress,
      tenantId, token, client);
  }

  @Override
  public AuthorityRecordCollection getAuthorityRecordCollection(String tenantId, String token) {
    return new ExternalStorageModuleAuthorityRecordCollection(vertx, baseAddress,
      tenantId, token, client);
  }

  @Override
  public IngestJobCollection getIngestJobCollection(String tenantId, String token) {
    //There is no external storage implementation for Jobs yet
    return ingestJobCollection;
  }

  @Override
  public UserCollection getUserCollection(String tenantId, String token) {
    return new ExternalStorageModuleUserCollection(vertx, baseAddress,
      tenantId, token, client);
  }
}
