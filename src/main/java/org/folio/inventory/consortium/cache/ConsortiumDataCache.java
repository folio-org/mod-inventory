package org.folio.inventory.consortium.cache;

import static io.vertx.core.http.HttpMethod.GET;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.folio.inventory.consortium.util.ConsortiumUtil.DEFAULT_EXPIRATION_TIME_SECONDS;
import static org.folio.inventory.consortium.util.ConsortiumUtil.EXPIRATION_TIME_PARAM;
import static org.folio.okapi.common.XOkapiHeaders.URL;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.jspecify.annotations.NonNull;

public class ConsortiumDataCache {

  private static final Logger LOG = LogManager.getLogger(ConsortiumDataCache.class);

  private static final String USER_TENANTS_PATH = "/user-tenants";
  private static final String USER_TENANTS_FIELD = "userTenants";
  private static final String CENTRAL_TENANT_ID_FIELD = "centralTenantId";
  private static final String CONSORTIUM_ID_FIELD = "consortiumId";

  private static ConsortiumDataCache instance = null;

  private final WebClient webClient;
  private final AsyncCache<String, Optional<ConsortiumConfiguration>> cache;

  public ConsortiumDataCache(Vertx vertx, HttpClient httpClient) {
    int expirationTime = Integer.parseInt(System.getProperty(EXPIRATION_TIME_PARAM, DEFAULT_EXPIRATION_TIME_SECONDS));
    this.webClient = WebClient.wrap(httpClient);
    this.cache = Caffeine.newBuilder()
      .expireAfterWrite(expirationTime, TimeUnit.SECONDS)
      .executor(task -> vertx.runOnContext(v -> task.run()))
      .buildAsync();
  }

  public static ConsortiumDataCache getInstance(Vertx vertx, HttpClient httpClient) {
    return getInstance(vertx, httpClient, false);
  }

  public static synchronized ConsortiumDataCache getInstance(Vertx vertx, HttpClient httpClient, boolean returnNew) {
    if (instance == null || returnNew) {
      instance = new ConsortiumDataCache(vertx, httpClient);
    }
    return instance;
  }

  /**
   * Returns consortium data by specified {@code tenantId}.
   *
   * @param tenantId - tenant id
   * @param headers  - okapi headers
   * @return future of Optional with consortium data for the specified {@code tenantId},
   *   if the specified {@code tenantId} is not included to any consortium, then returns future with empty Optional
   */
  public Future<Optional<ConsortiumConfiguration>> getConsortiumData(String tenantId, Map<String, String> headers) {
    LOG.info("getConsortiumData:: Retrieving consortium data for tenantId: '{}'", tenantId);
    if (tenantId == null) {
      return Future.failedFuture(new NullPointerException("tenantId must not be null"));
    }
    var connectionUrl = headers.get(URL);
    if (connectionUrl == null) {
      var msg = String.format("Error loading consortium data: header '%s' is missing", URL);
      return Future.failedFuture(new ConsortiumException(msg));
    }
    return Future.fromCompletionStage(cache.get(tenantId,
      (key, executor) -> loadConsortiumData(key, connectionUrl, headers)));
  }

  private CompletableFuture<Optional<ConsortiumConfiguration>> loadConsortiumData(String tenantId,
                                                                                  String connectionUrl,
                                                                                  Map<String, String> headers) {
    LOG.info("loadConsortiumData:: Loading consortium data for tenantId: '{}'", tenantId);
    var request = webClient.requestAbs(GET, connectionUrl + USER_TENANTS_PATH + "?limit=1");
    headers.forEach(request::putHeader);

    return request.send()
      .compose(processResponse(tenantId)).toCompletionStage().toCompletableFuture();
  }

  private Function<HttpResponse<Buffer>, Future<Optional<ConsortiumConfiguration>>> processResponse(String tenantId) {
    return response -> {
      if (response.statusCode() != HTTP_OK) {
        var msg = String.format("Error loading consortium data, tenantId: '%s' response status: '%s', body: '%s'",
          tenantId, response.statusCode(), response.bodyAsString());
        LOG.warn("loadConsortiumData:: {}", msg);
        return Future.failedFuture(new ConsortiumException(msg));
      }

      var userTenants = response.bodyAsJsonObject().getJsonArray(USER_TENANTS_FIELD);
      if (userTenants.isEmpty()) {
        return Future.succeededFuture(Optional.empty());
      }

      LOG.info("loadConsortiumData:: Consortium data was loaded, tenantId: '{}'", tenantId);
      var consortiumConfiguration = buildConsortiumConfiguration(userTenants);
      return Future.succeededFuture(Optional.of(consortiumConfiguration));
    };
  }

  private @NonNull ConsortiumConfiguration buildConsortiumConfiguration(JsonArray userTenants) {
    var userTenant = userTenants.getJsonObject(0);
    return new ConsortiumConfiguration(
      userTenant.getString(CENTRAL_TENANT_ID_FIELD),
      userTenant.getString(CONSORTIUM_ID_FIELD)
    );
  }
}
