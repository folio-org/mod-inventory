package org.folio.inventory.consortium.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;
import java.util.UUID;

/**
 * Entity that is used for sharing instance process
 */
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
   * Returns id of sharedInstance entity
   *
   * @return id of SharedInstance
   */
  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public SharingInstance withId(UUID id) {
    this.id = id;
    return this;
  }

  /**
   * Returns id of instance
   *
   * @return instanceIdentifier
   */
  public UUID getInstanceIdentifier() {
    return instanceIdentifier;
  }

  public void setInstanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
  }

  public SharingInstance withInstanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
    return this;
  }

  /**
   * Returns the tenant id from which pull the instance
   *
   * @return sourceTenantId
   */
  public String getSourceTenantId() {
    return sourceTenantId;
  }

  public void setSourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
  }

  public SharingInstance withSourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
    return this;
  }

  /**
   * Returns the tenant id to which pull the instance
   *
   * @return targetTenantId
   */
  public String getTargetTenantId() {
    return targetTenantId;
  }

  public void setTargetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
  }

  public SharingInstance withTargetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
    return this;
  }

  /**
   * Returns status of sharing process
   *
   * @return status
   */
  public SharingStatus getStatus() {
    return status;
  }

  public void setStatus(SharingStatus status) {
    this.status = status;
  }

  public SharingInstance withStatus(SharingStatus status) {
    this.status = status;
    return this;
  }

  /**
   * Returns the error that existed during sharing process
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
