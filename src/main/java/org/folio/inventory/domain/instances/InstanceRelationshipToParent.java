/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.instances;


import io.vertx.core.json.JsonObject;

/**
 *
 * @author ne
 */
public class InstanceRelationshipToParent {
  // JSON property names
  public static final String SUPER_INSTANCE_ID_KEY = "superInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public final String id;
  public final String superInstanceId;
  public final String instanceRelationshipTypeId;

  public InstanceRelationshipToParent (String id, String superInstanceId, String instanceRelationshipTypeId) {
    this.id = id;
    this.superInstanceId = superInstanceId;
    this.instanceRelationshipTypeId = instanceRelationshipTypeId;
  }

  public InstanceRelationshipToParent (JsonObject relationshipJson) {
    this(relationshipJson.getString("id"), relationshipJson.getString(SUPER_INSTANCE_ID_KEY), relationshipJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"id\": \"" + id + "\", \"superInstanceId\": \""+ superInstanceId + "\", \"instanceRelationshipTypeId\": \"" + instanceRelationshipTypeId + "\" }";
  }
}
