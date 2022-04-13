package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class RelatedInstance {
  // JSON property names
  public static final String RELATED_INSTANCE_ID_KEY = "relatedInstanceId";

  public final String relatedInstanceId;

  public RelatedInstance (String relatedInstanceId) {
    this.relatedInstanceId = relatedInstanceId;
  }

  public RelatedInstance (JsonObject rel) {
    this(rel.getString(RELATED_INSTANCE_ID_KEY));
  }

  @Override
  public String toString() {
    return "{ \""+RELATED_INSTANCE_ID_KEY+"\": \""+relatedInstanceId+"\" }";
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
