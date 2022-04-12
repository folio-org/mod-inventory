package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class RelatedInstance {
  // JSON property names
  public static final String ID_KEY = "id";
  public static final String SIBLING_INSTANCE_ID_KEY = "siblingInstanceId";
  public static final String INSTANCE_RELATIONSHIP_TYPE_ID_KEY = "instanceRelationshipTypeId";

  public final String id;
  public final String siblingInstanceId;
  public final String instanceRelationshipTypeId;

  public RelatedInstance (String id, String siblingInstanceId, String instanceRelationshipTypeId) {
    this.id = id;
    this.siblingInstanceId = siblingInstanceId;
    this.instanceRelationshipTypeId = instanceRelationshipTypeId;
  }

  public RelatedInstance (JsonObject rel) {
    this(rel.getString(ID_KEY),
         rel.getString(SIBLING_INSTANCE_ID_KEY),
         rel.getString(INSTANCE_RELATIONSHIP_TYPE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \""+ID_KEY+"\": \""+id+"\", \""+SIBLING_INSTANCE_ID_KEY+"\": \""+siblingInstanceId+"\", \""+INSTANCE_RELATIONSHIP_TYPE_ID_KEY+"\": \""+instanceRelationshipTypeId+"\"}";
  }

  @Override
  public boolean equals (Object object) {
    if (object instanceof RelatedInstance) {
      return object.toString().equals(this.toString());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
