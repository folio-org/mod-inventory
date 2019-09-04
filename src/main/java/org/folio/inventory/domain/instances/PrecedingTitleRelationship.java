package org.folio.inventory.domain.instances;


import io.vertx.core.json.JsonObject;

public class PrecedingTitleRelationship {
  public static final String SUB_INSTANCE_ID_KEY = "subInstanceId";

  private final String id;
  private final String subInstanceId;

  public PrecedingTitleRelationship(String id, String subInstanceId) {
    this.id = id;
    this.subInstanceId = subInstanceId;
  }

  public PrecedingTitleRelationship(JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUB_INSTANCE_ID_KEY));
  }

  public String getId() {
    return id;
  }

  public String getSubInstanceId() {
    return subInstanceId;
  }
}
