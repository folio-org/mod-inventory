package org.folio.inventory.domain;

import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;

public interface CollectionProvider {
  ItemCollection getItemCollection(String tenantId, String token);
  HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token);
  InstanceCollection getInstanceCollection(String tenantId, String token);
  AuthorityRecordCollection getAuthorityCollection(String tenantId, String token);
  UserCollection getUserCollection(String tenantId, String token);
  HoldingsRecordsSourceCollection getHoldingsRecordsSourceCollection(String tenantId, String token);

  default InstanceCollection getTraceableInstanceCollection(String tenantId, String token, String recordId, String jobExecutionId) {
    return getInstanceCollection(tenantId, token);
  }

  default HoldingsRecordCollection getTraceableHoldingsRecordCollection(String tenantId, String token, String recordId, String jobExecutionId) {
    return getHoldingsRecordCollection(tenantId, token);
  }

  default ItemCollection getTraceableItemRecordCollection(String tenantId, String token, String recordId, String jobExecutionId) {
    return getItemCollection(tenantId, token);
  }

}
