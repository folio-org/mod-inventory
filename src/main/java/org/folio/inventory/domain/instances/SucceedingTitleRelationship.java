package org.folio.inventory.domain.instances;


import io.vertx.core.json.JsonObject;

public class SucceedingTitleRelationship {
  public static final String SUPER_INSTANCE_ID_KEY = "superInstanceId";

  private final String id;
  private final String superInstanceId;

  public SucceedingTitleRelationship(String id, String superInstanceId) {
    this.id = id;
    this.superInstanceId = superInstanceId;
  }

  public SucceedingTitleRelationship(JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUPER_INSTANCE_ID_KEY));
  }

  public String getId() {
    return id;
  }

  public String getSuperInstanceId() {
    return superInstanceId;
  }
}
