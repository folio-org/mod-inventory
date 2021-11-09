package org.folio.inventory.domain;

import org.folio.inventory.domain.ingest.IngestJobCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;

public interface CollectionProvider {
  ItemCollection getItemCollection(String tenantId, String token);
  HoldingCollection getHoldingCollection(String tenantId, String token);
  HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token);
  InstanceCollection getInstanceCollection(String tenantId, String token);
  IngestJobCollection getIngestJobCollection(String tenantId, String token);
  AuthorityRecordCollection getAuthorityRecordCollection(String tenantId, String token);
  UserCollection getUserCollection(String tenantId, String token);
}
