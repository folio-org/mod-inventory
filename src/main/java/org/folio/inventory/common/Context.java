package org.folio.inventory.common;

public interface Context {
  String getTenantId();
  String getToken();
  String getOkapiLocation();
  String getUserId();
  String getRequestId();
}
