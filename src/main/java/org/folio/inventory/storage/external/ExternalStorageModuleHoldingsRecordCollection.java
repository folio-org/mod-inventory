package org.folio.inventory.storage.external;

import org.folio.Holdingsrecord;
import org.folio.inventory.domain.HoldingsRecordCollection;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleHoldingsRecordCollection
  extends ExternalStorageModuleCollection<Holdingsrecord>
  implements HoldingsRecordCollection {

  ExternalStorageModuleHoldingsRecordCollection(Vertx vertx,
                                         String baseAddress,
                                         String tenant,
                                         String token,
                                         HttpClient client) {

    super(vertx, String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      tenant, token, "holdingsRecords", client);
  }

  @Override
  protected Holdingsrecord mapFromJson(JsonObject holdingFromServer) {
  return null;
  }

  @Override
  protected String getId(Holdingsrecord record) {
    return record.getId();
  }

  @Override
  protected JsonObject mapToRequest(Holdingsrecord holding) {
return null;
  }
}
