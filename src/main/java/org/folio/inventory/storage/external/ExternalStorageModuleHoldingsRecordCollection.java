package org.folio.inventory.storage.external;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;

class ExternalStorageModuleHoldingsRecordCollection
  extends ExternalStorageModuleCollection<HoldingsRecord>
  implements HoldingsRecordCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleHoldingsRecordCollection.class);
  private static final ObjectMapper mapper = ObjectMapperTool.getMapper();

  /*
    Exclude 'holdingItems' and 'bareHoldingItems' from the response mapping to HoldingsRecord
    due to incompatibilities with changes in mod-inventory-storage.
   */
  static {
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  ExternalStorageModuleHoldingsRecordCollection(String baseAddress,
                                         String tenant,
                                         String token,
                                         String userId,
                                         String requestId,
                                         HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-storage/holdings"),
      tenant, token, userId, requestId, "holdingsRecords", client);
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
