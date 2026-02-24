package org.folio.inventory.validation;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;

public final class InstancesValidators {

  private static final Logger LOGGER = LogManager.getLogger(InstancesValidators.class);

  private InstancesValidators() {}

  public static CompletableFuture<Instance> refuseWhenInstanceNotFound(Instance instance) {
    if (instance == null) {
      String message = "Instance not found";
      LOGGER.warn(message);
      return failedFuture(new NotFoundException(message));
    }
    return completedFuture(instance);
  }

  public static CompletableFuture<Instance> refuseWhenHridChanged(
    Instance existingInstance, Instance updatedInstance) {

    if(!Objects.equals(existingInstance.getHrid(), updatedInstance.getHrid())) {
      String message = String.format("HRID change detected: existing=%s, updated=%s", existingInstance.getHrid(), updatedInstance.getHrid());
      LOGGER.error(message);
      return failedFuture(new UnprocessableEntityException(message, "hrid", updatedInstance.getHrid()));
    }
    return completedFuture(existingInstance);
  }
}
