package org.folio.inventory.resources;

import org.folio.inventory.storage.external.ReferenceRecord;
import org.folio.inventory.support.http.client.Response;

final class UpdateInstanceRelationshipResponseHolder {
  private final Response relationshipsResponse;
  private ReferenceRecord precedingSucceedingRelationshipType;

  public UpdateInstanceRelationshipResponseHolder(Response relationshipsResponse) {
    this.relationshipsResponse = relationshipsResponse;
  }

  public Response getRelationshipsResponse() {
    return relationshipsResponse;
  }

  public ReferenceRecord getPrecedingSucceedingRelationshipType() {
    return precedingSucceedingRelationshipType;
  }

  public UpdateInstanceRelationshipResponseHolder withPrecedingSucceedingRelationshipType(
    ReferenceRecord precedingSucceedingRelationshipType) {
    this.precedingSucceedingRelationshipType = precedingSucceedingRelationshipType;

    return this;
  }
}
