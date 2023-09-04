package org.folio.inventory.consortium.model;

import java.util.Objects;
import java.util.UUID;

public class SharingInstanceResult {

  private UUID id;
  private UUID instanceId;
  private String sourceTenantId;
  private String targetTenantId;
  private String status;
  private String error;

  public SharingInstanceResult() {
  }

  public SharingInstanceResult(UUID instanceId, String sourceTenantId, String targetTenantId, String status, String error) {
    this.id = UUID.randomUUID();
    this.instanceId = instanceId;
    this.sourceTenantId = sourceTenantId;
    this.targetTenantId = targetTenantId;
    this.status = status;
    this.error = error;
  }

  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  public UUID getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(UUID instanceId) {
    this.instanceId = instanceId;
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

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
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
    if (this == o) return true;
    if (!(o instanceof SharingInstanceResult that)) return false;
    return Objects.equals(id, that.id)
      && Objects.equals(instanceId, that.instanceId)
      && Objects.equals(sourceTenantId, that.sourceTenantId)
      && Objects.equals(targetTenantId, that.targetTenantId)
      && status == that.status
      && Objects.equals(error, that.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, instanceId, sourceTenantId, targetTenantId, status, error);
  }
}
