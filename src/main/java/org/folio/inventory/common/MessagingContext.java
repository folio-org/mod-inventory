package org.folio.inventory.common;

import io.vertx.core.MultiMap;

public class MessagingContext implements Context {

  public static final String TENANT_ID = "tenantId";
  public static final String TOKEN = "token";
  public static final String OKAPI_LOCATION = "okapiLocation";
  public static final String JOB_ID = "jobId";
  public static final String USER_ID = "userId";
  public static final String REQUEST_ID = "requestId";

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
    return getHeader(USER_ID);
  }

  @Override
  public String getRequestId() {
    return getHeader(REQUEST_ID);
  }

  private String getHeader(String header) {
    return headers.get(header);
  }

  public String getJobId() {
    return getHeader(JOB_ID);
  }
}
