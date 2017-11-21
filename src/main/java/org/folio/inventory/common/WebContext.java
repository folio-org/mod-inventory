package org.folio.inventory.common;

import io.vertx.ext.web.RoutingContext;

import java.net.MalformedURLException;
import java.net.URL;

public class WebContext implements Context {
  public WebContext(RoutingContext routingContext) {
    this.routingContext = routingContext;
  }

  @Override
  public String getTenantId() {
    return getHeader("X-Okapi-Tenant", "");
  }

  @Override
  public String getToken() {
    return getHeader("X-Okapi-Token", "");
  }

  @Override
  public String getOkapiLocation() {
    return getHeader("X-Okapi-Url", "");
  }

  @Override
  public String getHeader(String header) {
    return routingContext.request().getHeader(header);
  }

  @Override
  public String getHeader(String header, String defaultValue) {
    return hasHeader(header) ? getHeader(header) : defaultValue;
  }

  @Override
  public boolean hasHeader(String header) {
    return routingContext.request().headers().contains(header);
  }

  public URL absoluteUrl(String path) throws MalformedURLException {
    URL currentRequestUrl = new URL(routingContext.request().absoluteURI());

    //It would seem Okapi preserves headers from the original request,
    // so there is no need to use X-Okapi-Url for this?
    return new URL(currentRequestUrl.getProtocol(), currentRequestUrl.getHost(),
      currentRequestUrl.getPort(), path);
  }

  public Integer getIntegerParameter(String name, Integer defaultValue) {
    String value = routingContext.request().getParam(name);

    return value != null ? Integer.parseInt(value) : defaultValue;
  }

  public String getStringParameter(String name, String defaultValue) {
    String value = routingContext.request().getParam(name);

    return value != null ? value : defaultValue;
  }

  private final RoutingContext routingContext;
}
