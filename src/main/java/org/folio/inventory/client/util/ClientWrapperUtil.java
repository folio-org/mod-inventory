package org.folio.inventory.client.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import org.folio.rest.tools.ClientHelpers;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;

public final class ClientWrapperUtil {
  public static final String CONTENT_TYPE = "Content-type";
  public static final String APPLICATION_JSON = "application/json";
  public static final String ACCEPT = "Accept";
  public static final String APPLICATION_JSON_TEXT_PLAIN = "application/json,text/plain";

  public static void populateOkapiHeaders(HttpRequest<Buffer> request, String okapiUrl, String tenantId, String token, String userId) {
    if (tenantId != null) {
      request.putHeader(OKAPI_TOKEN, token);
      request.putHeader(OKAPI_TENANT, tenantId);
    }

    if (userId != null) {
      request.putHeader(OKAPI_USER_ID, userId);
    }

    if (okapiUrl != null) {
      request.putHeader(OKAPI_URL, okapiUrl);
    }
  }

  public static Buffer getBuffer(Object object) {
    Buffer buffer = Buffer.buffer();
    if (object != null) {
      buffer.appendString(ClientHelpers.pojo2json(object));
    }
    return buffer;
  }
}
