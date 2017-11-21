package org.folio.inventory.common;

import io.vertx.core.MultiMap;

public class MessagingContext implements Context {
  private final MultiMap headers;

  public MessagingContext(final MultiMap headers) {
    this.headers = headers;
  }

  @Override
  public String getTenantId() {
    return getHeader("tenantId");
  }

  @Override
  public String getToken() {
    return getHeader("token");
  }

  @Override
  public String getOkapiLocation() {
    return getHeader("okapiLocation");
  }

  @Override
  public String getHeader(String header) {
    return headers.get(header);
  }

  @Override
  public String getHeader(String header, String defaultValue) {
    return hasHeader(header) ? getHeader(header) : defaultValue;
  }

  @Override
  public boolean hasHeader(String header) {
    return headers.contains(header);
  }

  public String getJobId() {
    return getHeader("jobId");
  }
}
