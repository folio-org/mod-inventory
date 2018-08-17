package org.folio.inventory.domain;

import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.ingest.IngestJobCollection;

public interface CollectionProvider {
  ItemCollection getItemCollection(String tenantId, String token);
  HoldingCollection getHoldingCollection(String tenantId, String token);
  InstanceCollection getInstanceCollection(String tenantId, String token);
  IngestJobCollection getIngestJobCollection(String tenantId, String token);
}
