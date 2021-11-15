package org.folio.inventory.storage.external;

import org.folio.Authority;
import org.folio.inventory.domain.authority.AuthorityCollection;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

public class ExternalStorageModuleAuthorityCollection extends ExternalStorageModuleCollection<Authority> implements AuthorityCollection {

  ExternalStorageModuleAuthorityCollection(Vertx vertx,
                                           String baseAddress,
                                           String tenant,
                                           String token,
                                           HttpClient client) {
    super(String.format("%s/%s", baseAddress, "authority-storage/authorities"),
        tenant, token, "authorities", client);
  }

  @Override
  protected JsonObject mapToRequest(Authority authority) {
    return JsonObject.mapFrom(authority);
  }

  @Override
  protected Authority mapFromJson(JsonObject fromServer) {
    return fromServer.mapTo(Authority.class);
  }

  @Override
  protected String getId(Authority record) {
    return record.getId();
  }
}
