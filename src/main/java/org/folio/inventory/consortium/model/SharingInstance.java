package org.folio.inventory.consortium.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"id", "instanceIdentifier", "sourceTenantId", "targetTenantId", "status", "error"})
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
  private SharingStatus status;

  @JsonProperty("error")
  private String error;

  /**
   * Get id
   *
   * @return id
   */
  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

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
  public SharingStatus getStatus() {
    return status;
  }

  public void setStatus(SharingStatus status) {
    this.status = status;
  }

  /**
   * Get error
   *
   * @return error
   */
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
      .append("id", id)
      .append("instanceIdentifier", instanceIdentifier)
      .append("sourceTenantId", sourceTenantId)
      .append("targetTenantId", targetTenantId)
      .append("status", status)
      .append("error", error).toString();
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
}
