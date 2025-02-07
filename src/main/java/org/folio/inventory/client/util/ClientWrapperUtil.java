package org.folio.inventory.client.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.tools.ClientHelpers;
import io.vertx.core.http.HttpMethod;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;

/**
 * Utility class for handling client wrapper operations.
 */
public final class ClientWrapperUtil {
  public static final String CONTENT_TYPE = "Content-type";
  public static final String APPLICATION_JSON = "application/json";
  public static final String ACCEPT = "Accept";
  public static final String APPLICATION_JSON_TEXT_PLAIN = "application/json,text/plain";

  private ClientWrapperUtil() {
  }

  /**
   * Creates an HTTP request with the specified method and URL, and populates it with Okapi headers.
   *
   * @param method    the HTTP method to use (e.g., GET, POST, PUT)
   * @param url       the URL for the request
   * @param okapiUrl  the Okapi URL
   * @param tenantId  the tenant ID
   * @param token     the authentication token
   * @param userId    the user ID
   * @param webClient the WebClient instance to use for creating the request
   * @return the created HTTP request with populated headers
   */
  public static HttpRequest<Buffer> createRequest(HttpMethod method, String url, String okapiUrl, String tenantId,
                                                  String token, String userId, WebClient webClient) {
    HttpRequest<Buffer> request = webClient.requestAbs(method, url);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);
    return request;
  }

  /**
   * Converts an object to a JSON buffer.
   *
   * @param object the object to convert
   * @return the JSON buffer
   */
  public static Buffer getBuffer(Object object) {
    Buffer buffer = Buffer.buffer();
    if (object != null) {
      buffer.appendString(ClientHelpers.pojo2json(object));
    }
    return buffer;
  }

  /**
   * Populates the Okapi headers for the given HTTP request.
   *
   * @param request  the HTTP request to populate headers for
   * @param okapiUrl the Okapi URL
   * @param tenantId the tenant ID
   * @param token    the authentication token
   * @param userId   the user ID
   */
  private static void populateOkapiHeaders(HttpRequest<Buffer> request, String okapiUrl, String tenantId, String token, String userId) {
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);

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
}
