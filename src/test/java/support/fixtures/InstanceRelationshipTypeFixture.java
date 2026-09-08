package support.fixtures;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import support.dto.ReferenceRecordResponse;

public class InstanceRelationshipTypeFixture extends ReferenceRecordFixture {
  public InstanceRelationshipTypeFixture(OkapiHttpClient httpClient) {
    super(httpClient, json -> json.getString("name"));
  }

  public ReferenceRecordResponse boundWith() {
    return createIfNotExist(newInstanceRelationship("bound-with"));
  }

  public ReferenceRecordResponse monographicSeries() {
    return createIfNotExist(newInstanceRelationship("monographic series"));
  }

  private JsonObject newInstanceRelationship(String name) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("name", name);
  }
}
