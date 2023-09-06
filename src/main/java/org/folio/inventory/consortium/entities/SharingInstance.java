package org.folio.inventory.consortium.entities;

import java.util.UUID;

/**
 * Entity that is used for sharing instance process
 */
public class SharingInstance {
  private UUID id;
  private UUID instanceIdentifier;
  private String sourceTenantId;
  private String targetTenantId;
  private SharingStatus status;
  private String error;

  /**
   * Returns id of sharedInstance entity
   * @return id of SharedInstance
   */
  public UUID getId() {
    return id;
  }

  public void setId(UUID id) {
    this.id = id;
  }

  /**
   * Returns id of instance
   * @return id of instance
   */
  public UUID getInstanceIdentifier() {
    return instanceIdentifier;
  }

  public void setInstanceIdentifier(UUID instanceIdentifier) {
    this.instanceIdentifier = instanceIdentifier;
  }

  /**
   * Returns the tenant id from which pull the instance
   * @return id of sourceTenant
   */
  public String getSourceTenantId() {
    return sourceTenantId;
  }

  public void setSourceTenantId(String sourceTenantId) {
    this.sourceTenantId = sourceTenantId;
  }

  /**
   * Returns the tenant id to which pull the instance
   * @return id of targetTenant
   */
  public String getTargetTenantId() {
    return targetTenantId;
  }

  public void setTargetTenantId(String targetTenantId) {
    this.targetTenantId = targetTenantId;
  }

  /**
   * Returns status of sharing process
   * @return status
   */
  public SharingStatus getStatus() {
    return status;
  }

  public void setStatus(SharingStatus status) {
    this.status = status;
  }

  /**
   * Returns the error that existed during sharing process
   * @return error as a string
   */
  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }
}
