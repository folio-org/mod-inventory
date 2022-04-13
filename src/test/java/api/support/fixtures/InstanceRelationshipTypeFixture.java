package api.support.fixtures;

import java.util.UUID;

import org.folio.inventory.support.http.client.OkapiHttpClient;

import io.vertx.core.json.JsonObject;

public class InstanceRelationshipTypeFixture extends ReferenceRecordFixture {
  public InstanceRelationshipTypeFixture(OkapiHttpClient httpClient) {
    super(httpClient, json -> json.getString("name"));
  }

  public ReferenceRecordResponse boundWith() {
    return createIfNotExist(newInstanceRelationshipType("bound-with"));
  }

  public ReferenceRecordResponse monographicSeries() {
    return createIfNotExist(newInstanceRelationshipType("monographic series"));
  }

  private JsonObject newInstanceRelationshipType(String name) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("name", name);
  }
}
