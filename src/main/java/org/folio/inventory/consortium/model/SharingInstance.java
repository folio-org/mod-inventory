package org.folio.inventory.consortium.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.Valid;
import java.util.Objects;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
  "status"
})
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
  private ConsortiumEnumStatus status;

  @JsonProperty("error")
  private String error;

  /**
   * Get id
   * @return id
   */
  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public SharingInstance instanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
    return this;
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
  public ConsortiumEnumStatus getStatus() {
    return status;
  }

  public void setStatus(ConsortiumEnumStatus status) {
    this.status = status;
  }

  /**
   * Get error
   * @return error
   */
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
      Objects.equals(this.error, sharingInstance.status);
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
