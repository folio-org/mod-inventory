package org.folio.inventory.storage.external;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import java.io.IOException;

class ExternalStorageModuleHoldingsRecordCollection
  extends ExternalStorageModuleCollection<HoldingsRecord>
  implements HoldingsRecordCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleHoldingsRecordCollection.class);


  ExternalStorageModuleHoldingsRecordCollection(Vertx vertx,
                                         String baseAddress,
                                         String tenant,
                                         String token,
                                         HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      tenant, token, "holdingsRecords", client);
  }

  @Override
  protected HoldingsRecord mapFromJson(JsonObject holdingFromServer) {
    try {
      return ObjectMapperTool.getMapper().readValue(holdingFromServer.encode(), HoldingsRecord.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'Holdingsrecord' entity", e);
    }
  }

  @Override
  protected String getId(HoldingsRecord record) {
    return record.getId();
  }

  @Override
  protected JsonObject mapToRequest(HoldingsRecord holding) {
    try {
      return JsonObject.mapFrom(holding);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map 'Holdingsrecord' entity to json", e);
    }
  }
}
