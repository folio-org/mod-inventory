package api.support.fixtures;

import java.util.UUID;

import org.folio.inventory.support.http.client.OkapiHttpClient;

import io.vertx.core.json.JsonObject;

public class RelatedInstanceTypeFixture extends ReferenceRecordFixture {
  public RelatedInstanceTypeFixture(OkapiHttpClient httpClient) {
    super(httpClient, json -> json.getString("name"));
  }

  public ReferenceRecordResponse related() {
    return createIfNotExist(newRelatedInstanceType("related"));
  }

  private JsonObject newRelatedInstanceType(String name) {
    return new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("name", name);
  }
}
