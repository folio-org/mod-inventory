package org.folio.inventory.domain.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"jobId", "record", "type", "tenant", "ts"})
public class InstanceEvent {
  @JsonProperty("jobId")
  private String jobId;
  @JsonProperty("linkIds")
  private List<Integer> linkIds = new LinkedList<>();
  @JsonProperty("record")
  private String record;
  @JsonProperty("type")
  private InstanceEvent.EventType type;
  @JsonProperty("tenant")
  private String tenant;
  @JsonProperty("ts")
  private String ts;

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public InstanceEvent withJobId(String jobId) {
    this.jobId = jobId;
    return this;
  }

  public List<Integer> getLinkIds() {
    return linkIds;
  }

  public void setLinkIds(List<Integer> linkIds) {
    this.linkIds = linkIds;
  }

  public InstanceEvent withLinkIds(List<Integer> linkIds) {
    this.linkIds = linkIds;
    return this;
  }

  public String getRecord() {
    return record;
  }

  public void setRecord(String record) {
    this.record = record;
  }

  public InstanceEvent withRecord(String record) {
    this.record = record;
    return this;
  }

  public EventType getType() {
    return type;
  }

  public void setType(EventType type) {
    this.type = type;
  }

  public InstanceEvent withType(EventType type) {
    this.type = type;
    return this;
  }

  public String getTenant() {
    return tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public InstanceEvent withTenant(String tenant) {
    this.tenant = tenant;
    return this;
  }

  public String getTs() {
    return ts;
  }

  public void setTs(String ts) {
    this.ts = ts;
  }

  public InstanceEvent withTs(String ts) {
    this.ts = ts;
    return this;
  }

  public enum EventType {
    UPDATE("UPDATE"),
    DELETE("DELETE");
    private final String value;
    private static final Map<String, InstanceEvent.EventType> CONSTANTS = new HashMap<>();

    EventType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    @JsonValue
    public String value() {
      return this.value;
    }

    @JsonCreator
    public static InstanceEvent.EventType fromValue(String value) {
      InstanceEvent.EventType constant = CONSTANTS.get(value);
      if (constant == null) {
        throw new IllegalArgumentException(value);
      } else {
        return constant;
      }
    }

    static {
      for (EventType c : values()) {
        CONSTANTS.put(c.value, c);
      }
    }
  }
}
