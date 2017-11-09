package org.folio.inventory.common;

public interface Context {
  String getTenantId();
  String getToken();
  String getOkapiLocation();
  String getHeader(String header);
  String getHeader(String header, String defaultValue);
  boolean hasHeader(String header);
}
