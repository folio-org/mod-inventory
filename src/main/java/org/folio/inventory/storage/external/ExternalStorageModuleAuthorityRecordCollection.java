package org.folio.inventory.storage.external;

import java.io.IOException;

import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.Authority;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.validation.exceptions.JsonMappingException;

public class ExternalStorageModuleAuthorityRecordCollection
  extends ExternalStorageModuleCollection<Authority>
  implements AuthorityRecordCollection {

  private static final Logger LOGGER = LogManager.getLogger(ExternalStorageModuleAuthorityRecordCollection.class);

  ExternalStorageModuleAuthorityRecordCollection(
    String baseAddress,
    String tenant,
    String token,
    String userId,
    String requestId,
    HttpClient client) {

    super(String.format("%s/%s", baseAddress, "authority-storage/authorities"),
      tenant, token, userId, requestId,"authorities", client);
  }

  @Override
  protected Authority mapFromJson(JsonObject authorityFromServer) {
    try {
      return ObjectMapperTool.getMapper().readValue(authorityFromServer.encode(), Authority.class);
    } catch (IOException e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map json to 'Authority' entity", e);
    }
  }

  @Override
  protected String getId(Authority authority) {
    return authority.getId();
  }

  @Override
  protected JsonObject mapToRequest(Authority authority) {
    try {
      return JsonObject.mapFrom(authority);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new JsonMappingException("Can`t map 'Authority' entity to json", e);
    }
  }
}
