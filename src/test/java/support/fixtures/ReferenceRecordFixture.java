package support.fixtures;

import io.vertx.core.json.JsonObject;
import java.util.function.Function;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import support.dto.ReferenceRecordResponse;
import support.http.ResourceClient;

public class ReferenceRecordFixture {
  private final ResourceClient client;
  private final Function<JsonObject, String> keyMapper;

  public ReferenceRecordFixture(
    OkapiHttpClient httpClient, Function<JsonObject, String> keyMapper) {

    this.client = ResourceClient.forInstanceRelationshipType(httpClient);
    this.keyMapper = keyMapper;
  }

  public ReferenceRecordResponse createIfNotExist(JsonObject toCreate) {
    try {

      final String expectedKey = keyMapper.apply(toCreate);

      for (JsonObject currentEntity : client.getAll()) {
        if (expectedKey.equals(keyMapper.apply(currentEntity))) {
          return new ReferenceRecordResponse(currentEntity);
        }
      }

      return new ReferenceRecordResponse(client.create(toCreate).getJson());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
