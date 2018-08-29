/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.folio.inventory.domain.instances;

/**
 *
 * @author ne
 */
public class InstanceRelationship {
  // JSON property names
  public static final String SUPER_INSTANCE_ID_KEY = "superInstanceId";
  public static final String SUB_INSTANCE_ID_KEY = "subInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public final String id;
  public final String superInstanceId;
  public final String subInstanceId;
  public final String instanceRelationshipTypeId;

  public InstanceRelationship (String id, String superInstanceId, String subInstanceId, String instanceRelationshipTypeId) {
    this.id = id;
    this.superInstanceId = superInstanceId;
    this.subInstanceId = subInstanceId;
    this.instanceRelationshipTypeId = instanceRelationshipTypeId;
  }
}
