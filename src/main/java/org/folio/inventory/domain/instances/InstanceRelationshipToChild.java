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
public class InstanceRelationshipToChild {
  // JSON property names
  public static final String SUB_INSTANCE_ID_KEY = "subInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public final String subInstanceId;
  public final String instanceRelationshipTypeId;

  public InstanceRelationshipToChild (String subInstanceid, String instanceRelationshipTypeId) {
    this.subInstanceId = subInstanceid;
    this.instanceRelationshipTypeId = instanceRelationshipTypeId;
  }

  public InstanceRelationshipToChild (JsonObject relationshipJson) {
    this(relationshipJson.getString(SUB_INSTANCE_ID_KEY), relationshipJson.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \"subInstanceId\": \""+ subInstanceId + "\", \"instanceRelationshipTypeId\": \"" + instanceRelationshipTypeId + "\" }";
  }

}
