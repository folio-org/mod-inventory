package org.folio.inventory.consortium.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"id", "authorityId", "authorityNaturalId", "instanceId", "linkingRuleId", "status", "errorCause"})
public class EntityLink {
  @JsonProperty("id")
  private String id;
  @JsonProperty("authorityId")
  private String authorityId;
  @JsonProperty("authorityNaturalId")
  private String authorityNaturalId;
  @JsonProperty("instanceId")
  private String instanceId;
  @JsonProperty("linkingRuleId")
  private int linkingRuleId;
  @JsonProperty("status")
  private String status;
  @JsonProperty("errorCause")
  private String errorCause;

  public EntityLink() {}

  public EntityLink(String id, String authorityId, String authorityNaturalId, String instanceId, int linkingRuleId, String status, String errorCause) {
    this.id = id;
    this.authorityId = authorityId;
    this.authorityNaturalId = authorityNaturalId;
    this.instanceId = instanceId;
    this.linkingRuleId = linkingRuleId;
    this.status = status;
    this.errorCause = errorCause;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getAuthorityId() {
    return authorityId;
  }

  public void setAuthorityId(String authorityId) {
    this.authorityId = authorityId;
  }

  public String getAuthorityNaturalId() {
    return authorityNaturalId;
  }

  public void setAuthorityNaturalId(String authorityNaturalId) {
    this.authorityNaturalId = authorityNaturalId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public int getLinkingRuleId() {
    return linkingRuleId;
  }

  public void setLinkingRuleId(int linkingRuleId) {
    this.linkingRuleId = linkingRuleId;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String getErrorCause() {
    return errorCause;
  }

  public void setErrorCause(String errorCause) {
    this.errorCause = errorCause;
  }
}
