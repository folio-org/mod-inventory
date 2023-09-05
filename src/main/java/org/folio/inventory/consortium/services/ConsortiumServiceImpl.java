package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.WebClient;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.Status;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.support.http.client.OkapiHttpClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;

public class ConsortiumServiceImpl implements ConsortiumService {
  private static final Logger LOGGER = LogManager.getLogger(ConsortiumServiceImpl.class);
  private static final String USER_TENANTS_ENDPOINT = "/user-tenants?limit=1";
  private static final String SHARE_INSTANCE_ENDPOINT = "/consortia/%s/sharing/instances";
  private static final String CONSORTIA_ENDPOINT = "/consortia";
  private static final String SHARING_INSTANCE_ERROR = "Error during sharing Instance for sourceTenantId: %s, targetTenantId: %s, instanceIdentifier: %s, status code: %s, response message: %s";
  private final HttpClient httpClient;

  public ConsortiumServiceImpl(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public Future<SharingInstance> createShadowInstance(Context context, String instanceId) {
    return getCentralTenantId(context)
      .compose(centralTenantId -> {
        Context centralTenantContext = constructContext(centralTenantId, context.getToken(), context.getOkapiLocation());
        return getConsortiumId(centralTenantContext)
          .compose(consortiumId -> {
            SharingInstance sharingInstance = new SharingInstance();
            sharingInstance.setSourceTenantId(centralTenantId);
            sharingInstance.setInstanceIdentifier(UUID.fromString(instanceId));
            sharingInstance.setTargetTenantId(context.getTenantId());
            return shareInstance(centralTenantContext, consortiumId, sharingInstance);
          });
      });
  }

  public Future<String> getCentralTenantId(Context context) {
    CompletableFuture<String> completableFuture = createOkapiHttpClient(context)
      .thenCompose(client ->
        client.get(context.getOkapiLocation() + USER_TENANTS_ENDPOINT).toCompletableFuture()
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              JsonArray userTenants = httpResponse.getJson().getJsonArray("userTenants");
              if (userTenants.isEmpty()) {
                String message = "Central tenant not found";
                LOGGER.warn(String.format("getCentralTenantId:: %s", message));
                return CompletableFuture.failedFuture(new NotFoundException(message));
              }
              String centralTenantId = userTenants.getJsonObject(0).getString("centralTenantId");
              LOGGER.debug("getCentralTenantId:: Found centralTenantId: {}", centralTenantId);
              return CompletableFuture.completedFuture(centralTenantId);
            } else {
              String message = String.format("Error retrieving centralTenantId by tenant id: %s, status code: %s, response message: %s",
                context.getTenantId(), httpResponse.getStatusCode(), httpResponse.getBody());
              LOGGER.warn(String.format("getCentralTenantId:: %s", message));
              return CompletableFuture.failedFuture(new ConsortiumException(message));
            }
          }));
    return Future.fromCompletionStage(completableFuture);
  }

  public Future<String> getConsortiumId(Context context) {
    CompletableFuture<String> completableFuture = createOkapiHttpClient(context)
      .thenCompose(client ->
        client.get(context.getOkapiLocation() + CONSORTIA_ENDPOINT).toCompletableFuture()
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
              JsonArray consortia = httpResponse.getJson().getJsonArray("consortia");
              if (consortia.isEmpty()) {
                String message = String.format("ConsortiaId for tenant: %s not found", context.getTenantId());
                LOGGER.warn(String.format("getConsortiumId:: %s", message));
                return CompletableFuture.failedFuture(new NotFoundException(message));
              }
              String consortiumId = consortia.getJsonObject(0).getString("id");
              LOGGER.debug("getConsortiumId:: Found consortiumId: {}", consortiumId);
              return CompletableFuture.completedFuture(consortiumId);
            } else {
              String message = String.format("Error retrieving consortiaId by tenant: %s, status code: %s, response message: %s",
                context.getTenantId(), httpResponse.getStatusCode(), httpResponse.getBody());
              LOGGER.warn(String.format("getConsortiumId:: %s", message));
              return CompletableFuture.failedFuture(new ConsortiumException(message));
            }
          }));
    return Future.fromCompletionStage(completableFuture);
  }

  public Future<SharingInstance> shareInstance(Context context, String consortiumId, SharingInstance sharingInstance) {
    CompletableFuture<SharingInstance> completableFuture = createOkapiHttpClient(context)
      .thenCompose(client ->
        client.post(context.getOkapiLocation() + String.format(SHARE_INSTANCE_ENDPOINT, consortiumId), Json.encode(sharingInstance))
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_OK && !httpResponse.getJson().getString("status").equals(Status.ERROR.toString())) {
              SharingInstance response = Json.decodeValue(httpResponse.getBody(), SharingInstance.class);
              LOGGER.debug("shareInstance:: Successfully sharedInstance with id: {}, sharedInstance: {}",
                response.getInstanceIdentifier(), httpResponse.getBody());
              return CompletableFuture.completedFuture(response);
            } else {
              String message = String.format(SHARING_INSTANCE_ERROR, sharingInstance.getSourceTenantId(), sharingInstance.getTargetTenantId(),
                sharingInstance.getInstanceIdentifier(), httpResponse.getStatusCode(), httpResponse.getBody());
              LOGGER.warn(String.format("shareInstance:: %s", message));
              return CompletableFuture.failedFuture(new ConsortiumException(message));
            }
          }));
    return Future.fromCompletionStage(completableFuture);
  }

  private CompletableFuture<OkapiHttpClient> createOkapiHttpClient(Context context) {
    try {
      return CompletableFuture.completedFuture(new OkapiHttpClient(WebClient.wrap(httpClient), new URL(context.getOkapiLocation()),
        context.getTenantId(), context.getToken(), null, null, null));
    } catch (MalformedURLException e) {
      LOGGER.warn("createOkapiHttpClient:: Error during creation of OkapiHttpClient for URL: {}", context.getOkapiLocation(), e);
      return CompletableFuture.failedFuture(e);
    }
  }
}
