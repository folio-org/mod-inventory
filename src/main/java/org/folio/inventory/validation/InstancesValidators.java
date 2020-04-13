package org.folio.inventory.validation;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.inventory.validation.exceptions.NotFoundException;
import org.folio.inventory.validation.exceptions.UnprocessableEntityException;

public final class InstancesValidators {

  private InstancesValidators() {}

  public static CompletableFuture<Instance> refuseWhenNoTitleForPrecedingSucceedingUnconnectedTitle(
    Instance instance) {

    final ValidationError succeedingError = new ValidationError(
      "Title is required for unconnected succeeding title", "succeedingTitles.title", null);
    final ValidationError precedingError = new ValidationError(
      "Title is required for unconnected preceding title", "precedingTitles.title", null);

    return refuseWhenNoTitleForUnconnectedTitle(instance, instance.getSucceedingTitles(), succeedingError)
      .thenCompose(prev -> refuseWhenNoTitleForUnconnectedTitle(instance, instance.getPrecedingTitles(), precedingError));

  }

  private static CompletableFuture<Instance> refuseWhenNoTitleForUnconnectedTitle(
    Instance instance, List<PrecedingSucceedingTitle> titles, ValidationError error) {

    return isTitleMissingForUnconnectedPrecedingSucceeding(titles)
      ? failedFuture(new UnprocessableEntityException(error))
      : completedFuture(instance);
  }

  public static boolean isTitleMissingForUnconnectedPrecedingSucceeding(List<PrecedingSucceedingTitle> titles) {
    if (titles == null || titles.isEmpty()) {
      return false;
    }

    return titles.stream()
      .filter(InstancesValidators::isUnconnectedPrecedingSucceedingTitle)
      .anyMatch(title -> StringUtils.isBlank(title.title));
  }

  private static boolean isUnconnectedPrecedingSucceedingTitle(PrecedingSucceedingTitle title) {
    return title.precedingInstanceId == null && title.succeedingInstanceId == null;
  }

  public static CompletionStage<Instance> refuseWhenInstanceNotFound(Instance instance) {
    return instance == null
      ? failedFuture(new NotFoundException("Instance not found"))
      : completedFuture(instance);
  }

  public static CompletionStage<Instance> refuseWhenHridChanged(
    Instance existingInstance, Instance updatedInstance) {

    return Objects.equals(existingInstance.getHrid(), updatedInstance.getHrid())
      ? completedFuture(existingInstance)
      : failedFuture(new UnprocessableEntityException("HRID can not be updated",
      "hrid", updatedInstance.getHrid()));
  }
}
