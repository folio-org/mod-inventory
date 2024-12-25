package org.folio.inventory.consortium.services;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.Json;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.cache.ConsortiumDataCache;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.exceptions.ConsortiumException;
import org.folio.okapi.common.XOkapiHeaders;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.consortium.util.ConsortiumUtil.createOkapiHttpClient;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.support.http.ContentType.APPLICATION_JSON;

public class ConsortiumServiceImpl implements ConsortiumService {
  private static final Logger LOGGER = LogManager.getLogger(ConsortiumServiceImpl.class);
  private static final String SHARE_INSTANCE_ENDPOINT = "/consortia/%s/sharing/instances";
  private static final String SHARING_INSTANCE_ERROR = "Error during sharing Instance for sourceTenantId: %s, targetTenantId: %s, instanceIdentifier: %s, status code: %s, response message: %s";
  private final HttpClient httpClient;
  private final ConsortiumDataCache consortiumDataCache;

  public ConsortiumServiceImpl(HttpClient httpClient, ConsortiumDataCache consortiumDataCache) {
    this.httpClient = httpClient;
    this.consortiumDataCache = consortiumDataCache;
  }

  @Override
  public Future<SharingInstance> createShadowInstance(Context context, String instanceId, ConsortiumConfiguration consortiumConfiguration) {
    Context centralTenantContext = constructContext(consortiumConfiguration.getCentralTenantId(), context.getToken(), context.getOkapiLocation(), context.getUserId(), context.getRequestId());
    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setSourceTenantId(consortiumConfiguration.getCentralTenantId());
    sharingInstance.setInstanceIdentifier(UUID.fromString(instanceId));
    sharingInstance.setTargetTenantId(context.getTenantId());
    return shareInstance(centralTenantContext, consortiumConfiguration.getConsortiumId(), sharingInstance);
  }

  @Override
  public Future<Optional<ConsortiumConfiguration>> getConsortiumConfiguration(Context context) {
    Map<String, String> headers = Map.of(XOkapiHeaders.URL, context.getOkapiLocation(),
      XOkapiHeaders.TENANT, context.getTenantId(), XOkapiHeaders.TOKEN, context.getToken());
    return consortiumDataCache.getConsortiumData(context.getTenantId(), headers);
  }

  // Returns successful future if the sharing status is "IN_PROGRESS" or "COMPLETE"
  @Override
  public Future<SharingInstance> shareInstance(Context context, String consortiumId, SharingInstance sharingInstance) {
    Map<String, String> headers = Map.of(HttpHeaders.CONTENT_TYPE.toString(), APPLICATION_JSON);
    CompletableFuture<SharingInstance> completableFuture = createOkapiHttpClient(context, httpClient)
      .thenCompose(client ->
        client.post(context.getOkapiLocation() + String.format(SHARE_INSTANCE_ENDPOINT, consortiumId), Json.encode(sharingInstance), headers)
          .thenCompose(httpResponse -> {
            if (httpResponse.getStatusCode() == HttpStatus.SC_CREATED && !SharingStatus.ERROR.toString().equals(httpResponse.getJson().getString("status"))) {
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
}
