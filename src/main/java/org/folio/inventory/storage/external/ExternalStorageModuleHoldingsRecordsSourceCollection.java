package org.folio.inventory.storage.external;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecordsSource;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;

public class ExternalStorageModuleHoldingsRecordsSourceCollection
  extends ExternalStorageModuleCollection<HoldingsRecordsSource>
  implements HoldingsRecordsSourceCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleHoldingsRecordsSourceCollection.class);

  ExternalStorageModuleHoldingsRecordsSourceCollection(
    String baseAddress,
    String tenant,
    String token,
    String userId,
    String requestId,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "holdings-sources"),
      tenant, token, userId, requestId, "holdingsRecordsSources", client);
  }

  @Override
  protected JsonObject mapToRequest(HoldingsRecordsSource record) {
      return JsonObject.mapFrom(record);
  }

  @Override
  protected HoldingsRecordsSource mapFromJson(JsonObject fromServer) {
    try {
      return ObjectMapperTool.getMapper().readValue(fromServer.encode(), HoldingsRecordsSource.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'holdingsRecordsSources' entity", e);
    }
  }

  @Override
  protected String getId(HoldingsRecordsSource record) {
    return record.getId();
  }
}
