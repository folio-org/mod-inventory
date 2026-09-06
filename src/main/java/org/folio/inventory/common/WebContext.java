package org.folio.inventory.common;

import io.vertx.ext.web.RoutingContext;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.commons.lang3.StringUtils;
import org.folio.okapi.common.XOkapiHeaders;

public class WebContext implements Context {

  private final RoutingContext routingContext;

  public WebContext(RoutingContext routingContext) {
    this.routingContext = routingContext;
  }

  @Override
  public String getTenantId() {
    return getHeaderOrEmpty(XOkapiHeaders.TENANT);
  }

  @Override
  public String getToken() {
    return getHeaderOrEmpty(XOkapiHeaders.TOKEN);
  }

  @Override
  public String getOkapiLocation() {
    return getHeaderOrEmpty(XOkapiHeaders.URL);
  }

  @Override
  public String getUserId() {
    return getHeaderOrEmpty(XOkapiHeaders.USER_ID);
  }

  @Override
  public String getRequestId() {
    return getHeader(XOkapiHeaders.REQUEST_ID);
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

  private String getHeader(String header) {
    return routingContext.request().getHeader(header);
  }

  private String getHeaderOrEmpty(String header) {
    return hasHeader(header) ? getHeader(header) : StringUtils.EMPTY;
  }

  private boolean hasHeader(String header) {
    return routingContext.request().headers().contains(header);
  }
}
