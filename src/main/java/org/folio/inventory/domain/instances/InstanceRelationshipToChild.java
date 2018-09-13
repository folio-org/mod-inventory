package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class InstanceRelationshipToChild {
  // JSON property names
  public static final String SUB_INSTANCE_ID_KEY = "subInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public final String id;
  public final String subInstanceId;
  public final String instanceRelationshipTypeId;

  public InstanceRelationshipToChild (String id, String subInstanceid, String instanceRelationshipTypeId) {
    this.id = id;
    this.subInstanceId = subInstanceid;
    this.instanceRelationshipTypeId = instanceRelationshipTypeId;
  }

  public InstanceRelationshipToChild (JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUB_INSTANCE_ID_KEY), relationshipJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"id\": \"" + id + "\", \"subInstanceId\": \""+ subInstanceId + "\", \"instanceRelationshipTypeId\": \"" + instanceRelationshipTypeId + "\" }";
  }

}
