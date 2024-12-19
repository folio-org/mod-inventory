package org.folio.inventory.domain;

import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;

public interface CollectionProvider {
  ItemCollection getItemCollection(String tenantId, String token, String userId, String requestId);
  HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token, String userId, String requestId);
  InstanceCollection getInstanceCollection(String tenantId, String token, String userId, String requestId);
  AuthorityRecordCollection getAuthorityCollection(String tenantId, String token, String userId, String requestId);
  UserCollection getUserCollection(String tenantId, String token, String userId, String requestId);
  HoldingsRecordsSourceCollection getHoldingsRecordsSourceCollection(String tenantId, String token, String userId, String requestId);
}
