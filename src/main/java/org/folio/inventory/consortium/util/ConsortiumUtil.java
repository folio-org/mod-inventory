package org.folio.inventory.consortium.util;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.support.InstanceUtil;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.WebClient;
import java.util.concurrent.CompletableFuture;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Optional;

public class ConsortiumUtil {
  private static final Logger LOGGER = LogManager.getLogger(ConsortiumUtil.class);
  public static final String EXPIRATION_TIME_PARAM = "cache.consortium-data.expiration.time.seconds";
  public static final String DEFAULT_EXPIRATION_TIME_SECONDS = "300";

  private ConsortiumUtil() {}

  public static Future<Optional<SharingInstance>> createShadowInstanceIfNeeded(ConsortiumService consortiumService,
                                                                               InstanceCollection instanceCollection,
                                                                               Context context, String instanceId,
                                                                               ConsortiumConfiguration consortiumConfiguration) {
    return InstanceUtil.findInstanceById(instanceId, instanceCollection).map(Optional.<SharingInstance>empty())
      .recover(throwable -> {
        if (throwable instanceof NotFoundException) {
          LOGGER.info("createShadowInstanceIfNeeded:: Creating shadow instance with instanceId: {}", instanceId);
          return consortiumService.createShadowInstance(context, instanceId, consortiumConfiguration).map(Optional::of);
        }
        return Future.failedFuture(throwable);
      });
  }

  public static CompletableFuture<OkapiHttpClient> createOkapiHttpClient(Context context, HttpClient httpClient) {
    try {
      return CompletableFuture.completedFuture(new OkapiHttpClient(WebClient.wrap(httpClient), new URL(context.getOkapiLocation()),
        context.getTenantId(), context.getToken(), null, null, null));
    } catch (MalformedURLException e) {
      LOGGER.warn("createOkapiHttpClient:: Error during creation of OkapiHttpClient for URL: {}", context.getOkapiLocation(), e);
      return CompletableFuture.failedFuture(e);
    }
  }
}
