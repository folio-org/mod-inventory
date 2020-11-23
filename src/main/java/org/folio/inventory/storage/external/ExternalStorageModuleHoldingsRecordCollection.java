package org.folio.inventory.storage.external;

import java.io.IOException;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

class ExternalStorageModuleHoldingsRecordCollection
  extends ExternalStorageModuleCollection<HoldingsRecord>
  implements HoldingsRecordCollection {

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
      throw new JsonMappingException("Can`t map json to 'Holdingsrecord' entity", e);
    }
  }

  @Override
  protected String getId(HoldingsRecord record) {
    return record.getId();
  }

  @Override
  protected JsonObject mapToRequest(HoldingsRecord holding) {
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      return new JsonObject(ow.writeValueAsString(holding));
    } catch (IOException e) {
      throw new JsonMappingException("Can`t map 'Holdingsrecord' entity to json", e);
    }
  }
}
