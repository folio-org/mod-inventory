package org.folio.inventory.storage.external;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.HoldingsRecordsSourceCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;
import org.folio.rest.jaxrs.model.HoldingsRecordsSource;

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
  protected JsonObject mapToRequest(HoldingsRecordsSource entity) {
    return JsonObject.mapFrom(entity);
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
  protected String getId(HoldingsRecordsSource entity) {
    return entity.getId();
  }
}
