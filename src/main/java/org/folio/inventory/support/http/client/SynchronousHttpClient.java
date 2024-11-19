package org.folio.inventory.support.http.client;

import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.http.HttpHeaders.LOCATION;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.exceptions.InternalServerErrorException;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.function.Consumer;


public class SynchronousHttpClient extends AbstractOkapiHttpClient {

  private static final Logger LOGGER = LogManager.getLogger(SynchronousHttpClient.class);

  private final HttpClient httpClient;

  public SynchronousHttpClient(Context context) throws MalformedURLException {
    this(new URL(context.getOkapiLocation()),
      context.getTenantId(), context.getToken(), context.getUserId(),
      null, null);
  }

  public SynchronousHttpClient(URL okapiUrl, String tenantId,
                               String token, String userId, String requestId, Consumer<Throwable> exceptionHandler) {

    super(okapiUrl, tenantId, userId, token, requestId, exceptionHandler);
    this.httpClient = HttpClient.newBuilder().build();
  }

  public Response get(URL url) {
    return get(url.toString());
  }

  public Response get(String url) {
    var request = getRequest(url);
    request.headers().map().entrySet().forEach(entry -> LOGGER.info("key: {}, value: {}", entry.getKey(), entry.getValue()));
    try {
      var httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return mapResponse(httpResponse);
    } catch(Exception ex) {
      throw new ExternalResourceFetchException(
        String.format("Failed to fetch resource by URL - %s : %s", url, ex.getMessage()), null, 0, null);
    }
  }

  public Response put(URL url, JsonObject requestBody) {
    return put(url.toString(), requestBody);
  }

  public Response put(String url, JsonObject requestBody) {
    var request = putRequest(url, requestBody);
    try {
      var httpResponse = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      return mapResponse(httpResponse);
    } catch(Exception ex) {
      throw new InternalServerErrorException(ex);
    }
  }

  private HttpRequest getRequest(String url) {
    var uri = URI.create(url);
    var request = HttpRequest.newBuilder()
      .uri(uri)
      .GET();

    getHeaders().forEach(request::header);

    return request.build();
  }

  private HttpRequest putRequest(String url, JsonObject requestBody) {
    var uri = URI.create(url);
    var request = HttpRequest.newBuilder()
      .uri(uri)
      .PUT(java.net.http.HttpRequest.BodyPublishers.ofString(requestBody.encode()));

    getHeaders().forEach(request::header);

    return request.build();
  }

  private static Response mapResponse(HttpResponse<String> response) {
    return new Response(response.statusCode(), response.body(),
      response.headers().firstValue(CONTENT_TYPE).orElse(null),
      response.headers().firstValue(LOCATION).orElse(null));
  }
}
