package api.support.fixtures;

import io.vertx.core.json.JsonObject;

public class ReferenceRecordResponse {
  private final JsonObject representation;

  public ReferenceRecordResponse(JsonObject representation) {
    this.representation = representation;
  }

  public String getName() {
    return representation.getString("name");
  }

  public String getId() {
    return representation.getString("id");
  }
}
