package org.folio.inventory.consortium.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.folio.inventory.domain.items.Status;

import java.util.Objects;
import java.util.UUID;

public class SharingInstance {

  @JsonProperty("instanceIdentifier")
  private UUID instanceIdentifier;

  @JsonProperty("sourceTenantId")
  private String sourceTenantId;

  @JsonProperty("targetTenantId")
  private String targetTenantId;

  @JsonProperty("status")
  private Status status;

  /**
   * Get instanceIdentifier
   *
   * @return instanceIdentifier
   */
  public UUID getInstanceIdentifier() {
    return instanceIdentifier;
  }

  public void setInstanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
  }

  public SharingInstance sourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
    return this;
  }

  /**
   * Get sourceTenantId
   *
   * @return sourceTenantId
   */
  public String getSourceTenantId() {
    return sourceTenantId;
  }

  public void setSourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
  }

  public SharingInstance targetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
    return this;
  }

  /**
   * Get targetTenantId
   *
   * @return targetTenantId
   */
  public String getTargetTenantId() {
    return targetTenantId;
  }

  public void setTargetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
  }

  /**
   * Get status
   *
   * @return status
   */
  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
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
    return Objects.equals(this.instanceIdentifier, sharingInstance.instanceIdentifier) &&
      Objects.equals(this.sourceTenantId, sharingInstance.sourceTenantId) &&
      Objects.equals(this.targetTenantId, sharingInstance.targetTenantId) &&
      Objects.equals(this.status, sharingInstance.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(instanceIdentifier, sourceTenantId, targetTenantId, status);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class SharingInstance {\n");
    sb.append("    instanceIdentifier: ").append(toIndentedString(instanceIdentifier)).append("\n");
    sb.append("    sourceTenantId: ").append(toIndentedString(sourceTenantId)).append("\n");
    sb.append("    targetTenantId: ").append(toIndentedString(targetTenantId)).append("\n");
    sb.append("    status: ").append(toIndentedString(status)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }
}
