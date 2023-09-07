package org.folio.inventory.consortium.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.exceptions.NotFoundException;

import java.util.Optional;

import static java.lang.String.format;

public class ConsortiumUtil {
  private static final Logger LOGGER = LogManager.getLogger(ConsortiumUtil.class);

  private ConsortiumUtil() {}

  public static Future<Optional<SharingInstance>> createShadowInstanceIfNeeded(ConsortiumService consortiumService,
                                                                               InstanceCollection instanceCollection,
                                                                               Context context, String instanceId,
                                                                               ConsortiumConfiguration consortiumConfiguration) {
    return findInstanceById(instanceId, instanceCollection).map(Optional.<SharingInstance>empty())
      .recover(throwable -> {
        if (throwable instanceof NotFoundException) {
          LOGGER.info("createShadowInstanceIfNeeded:: Creating shadow instance with instanceId: {}", instanceId);
          return consortiumService.createShadowInstance(context, instanceId, consortiumConfiguration).map(Optional::of);
        }
        return Future.failedFuture(throwable);
      });
  }

  public static Future<Instance> findInstanceById(String instanceId, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.warn("findInstanceById:: Can't find Instance by id: {} ", instanceId);
          promise.fail(new NotFoundException(format("Can't find Instance by id: %s", instanceId)));
        } else {
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.warn(format("findInstanceById:: Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
