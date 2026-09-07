package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public record InstanceRelationshipToParent(String id, String superInstanceId, String instanceRelationshipTypeId) {
  // JSON property names
  public static final String SUPER_INSTANCE_ID_KEY = "superInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public InstanceRelationshipToParent(JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUPER_INSTANCE_ID_KEY),
      relationshipJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"id\": \"" + id + "\", \"superInstanceId\": \"" + superInstanceId
           + "\", \"instanceRelationshipTypeId\": \"" + instanceRelationshipTypeId + "\" }";
  }
}
