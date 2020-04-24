package org.folio.inventory.validation;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;

public final class InstancesValidators {

  private InstancesValidators() {}

  public static CompletableFuture<Instance> refuseWhenInstanceNotFound(Instance instance) {
    return instance == null
      ? failedFuture(new NotFoundException("Instance not found"))
      : completedFuture(instance);
  }

  public static CompletableFuture<Instance> refuseWhenHridChanged(
    Instance existingInstance, Instance updatedInstance) {

    return Objects.equals(existingInstance.getHrid(), updatedInstance.getHrid())
      ? completedFuture(existingInstance)
      : failedFuture(new UnprocessableEntityException("HRID can not be updated",
      "hrid", updatedInstance.getHrid()));
  }
}
