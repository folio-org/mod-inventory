package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public record InstanceRelationshipToChild(String id, String subInstanceId, String instanceRelationshipTypeId) {
  // JSON property names
  public static final String SUB_INSTANCE_ID_KEY = "subInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public InstanceRelationshipToChild(JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUB_INSTANCE_ID_KEY),
      relationshipJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"id\": \"" + id + "\", \"subInstanceId\": \"" + subInstanceId + "\", \"instanceRelationshipTypeId\": \""
           + instanceRelationshipTypeId + "\" }";
  }
}
