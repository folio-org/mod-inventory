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

import java.util.Optional;

public class ConsortiumUtil {
  private static final Logger LOGGER = LogManager.getLogger(ConsortiumUtil.class);

  private ConsortiumUtil() {
  }

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
}
