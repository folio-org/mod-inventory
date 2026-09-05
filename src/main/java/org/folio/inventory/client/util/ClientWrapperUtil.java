package org.folio.inventory.client.util;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.rest.tools.ClientHelpers;

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
   * Creates an HTTP request with the specified method and URL, and populates it with folio headers.
   *
   * @param method       the HTTP method to use (e.g., GET, POST, PUT)
   * @param folioHeaders folio headers
   * @param webClient    the WebClient instance to use for creating the request
   * @return the created HTTP request with populated headers
   */
  public static HttpRequest<Buffer> createRequest(HttpMethod method, String requestPath,
                                                  FolioHeaders folioHeaders, WebClient webClient) {
    var url = folioHeaders.getConnectionUrl().orElse("") + requestPath;
    HttpRequest<Buffer> request = webClient.requestAbs(method, url);
    populateHeaders(request, folioHeaders);
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

  private static void populateHeaders(HttpRequest<Buffer> request, FolioHeaders folioHeaders) {
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);

    folioHeaders.buildMap().forEach(request::putHeader);
  }
}
