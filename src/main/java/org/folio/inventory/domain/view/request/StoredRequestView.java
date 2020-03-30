package org.folio.inventory.domain.view.request;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.joda.time.DateTime;

public class StoredRequestView {
  private final Map<String, Object> rawProperties;

  public StoredRequestView(Map<String, Object> rawProperties) {
    this.rawProperties = rawProperties == null
      ? Collections.emptyMap()
      : new HashMap<>(rawProperties);
  }

  public String getId() {
    return rawProperties.get("id").toString();
  }

  public DateTime getHoldShelfExpirationDate() {
    return Optional.ofNullable(rawProperties.get("holdShelfExpirationDate"))
      .map(Object::toString)
      .map(DateTime::parse)
      .orElse(null);
  }

  public RequestStatus getStatus() {
    return RequestStatus.of(rawProperties.get("status").toString());
  }

  public void setStatus(RequestStatus newStatus) {
    rawProperties.put("status", newStatus.getValue());
  }

  public Map<String, Object> getMap() {
    return Collections.unmodifiableMap(rawProperties);
  }
}
