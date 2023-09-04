package org.folio.inventory.consortium.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

public class SharingInstance {

  @JsonProperty("id")
  private UUID id;

  @JsonProperty("instanceIdentifier")
  private UUID instanceIdentifier;

  @JsonProperty("sourceTenantId")
  private String sourceTenantId;

  @JsonProperty("targetTenantId")
  private String targetTenantId;

  @JsonProperty("status")
  private Status status;

  @JsonProperty("error")
  private String error;

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getInstanceIdentifier() {
    return instanceIdentifier;
  }

  public void setInstanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
  }

  public String getSourceTenantId() {
    return sourceTenantId;
  }

  public void setSourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
  }

  public String getTargetTenantId() {
    return targetTenantId;
  }

  public void setTargetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SharingInstance sharingInstance = (SharingInstance) o;
    return Objects.equals(this.id, sharingInstance.id) &&
      Objects.equals(this.instanceIdentifier, sharingInstance.instanceIdentifier) &&
      Objects.equals(this.sourceTenantId, sharingInstance.sourceTenantId) &&
      Objects.equals(this.targetTenantId, sharingInstance.targetTenantId) &&
      Objects.equals(this.status, sharingInstance.status) &&
      Objects.equals(this.error, sharingInstance.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, instanceIdentifier, sourceTenantId, targetTenantId, status, error);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SharingInstance {\n");
    sb.append("    id: ").append(toIndentedString(id)).append("\n");
    sb.append("    instanceIdentifier: ").append(toIndentedString(instanceIdentifier)).append("\n");
    sb.append("    sourceTenantId: ").append(toIndentedString(sourceTenantId)).append("\n");
    sb.append("    targetTenantId: ").append(toIndentedString(targetTenantId)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("    error: ").append(toIndentedString(error)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
