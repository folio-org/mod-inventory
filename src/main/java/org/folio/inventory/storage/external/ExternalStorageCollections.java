package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.HoldingCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.ItemCollection;
import org.folio.inventory.domain.ingest.IngestJobCollection;
import org.folio.inventory.storage.memory.InMemoryIngestJobCollection;

public class ExternalStorageCollections implements CollectionProvider {
  private final Vertx vertx;
  private final String baseAddress;
  private static final InMemoryIngestJobCollection ingestJobCollection = new InMemoryIngestJobCollection();

  public ExternalStorageCollections(Vertx vertx, String baseAddress) {
    this.vertx = vertx;
    this.baseAddress = baseAddress;
  }

  @Override
  public ItemCollection getItemCollection(String tenantId, String token) {
    return new ExternalStorageModuleItemCollection(vertx, baseAddress,
      tenantId, token);
  }

  @Override
  public HoldingCollection getHoldingCollection(String tenantId, String token) {
    return new ExternalStorageModuleHoldingCollection(vertx, baseAddress,
      tenantId, token);
  }

  @Override
  public InstanceCollection getInstanceCollection(String tenantId, String token) {
    return new ExternalStorageModuleInstanceCollection(vertx, baseAddress,
      tenantId, token);
  }

  @Override
  public IngestJobCollection getIngestJobCollection(String tenantId, String token) {
    //There is no external storage implementation for Jobs yet
    return ingestJobCollection;
  }
}
