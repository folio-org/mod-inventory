package org.folio.inventory.storage.external;

import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.CollectionProvider;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.user.UserCollection;

import io.vertx.core.http.HttpClient;

public class ExternalStorageCollections implements CollectionProvider {
  private final String baseAddress;
  private final HttpClient client;

  public ExternalStorageCollections(String baseAddress, HttpClient client) {
    this.baseAddress = baseAddress;
    this.client = client;
  }

  @Override
  public ItemCollection getItemCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleItemCollection(baseAddress, tenantId, token, userId, requestId, client);
  }

  @Override
  public HoldingsRecordCollection getHoldingsRecordCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleHoldingsRecordCollection(baseAddress,
      tenantId, token, userId, requestId, client);
  }

  @Override
  public InstanceCollection getInstanceCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleInstanceCollection(baseAddress,
      tenantId, token, userId, requestId, client);
  }

  @Override
  public AuthorityRecordCollection getAuthorityCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleAuthorityRecordCollection(baseAddress,
        tenantId, token, userId, requestId, client);
  }

  @Override
  public UserCollection getUserCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleUserCollection(baseAddress,
      tenantId, token, userId, requestId, client);
  }

  @Override
  public HoldingsRecordsSourceCollection getHoldingsRecordsSourceCollection(String tenantId, String token, String userId, String requestId) {
    return new ExternalStorageModuleHoldingsRecordsSourceCollection(baseAddress,
      tenantId, token, userId, requestId, client);
  }
}
