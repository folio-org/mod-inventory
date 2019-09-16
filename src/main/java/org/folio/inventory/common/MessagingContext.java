package org.folio.inventory.common;

import io.vertx.core.MultiMap;

public class MessagingContext implements Context {

  public final static String TENANT_ID = "tenantId";
  public final static String TOKEN = "token";
  public final static String OKAPI_LOCATION = "okapiLocation";
  public final static String JOB_ID = "jobId";

  private final MultiMap headers;

  public MessagingContext(final MultiMap headers) {
    this.headers = headers;
  }

  @Override
  public String getTenantId() {
    return getHeader(TENANT_ID);
  }

  @Override
  public String getToken() {
    return getHeader(TOKEN);
  }

  @Override
  public String getOkapiLocation() {
    return getHeader(OKAPI_LOCATION);
  }

  @Override
  public String getUserId() {
    return null;
  }

  @Override
  public String getRequestId() {
    return null;
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
    return getHeader(JOB_ID);
  }
}
