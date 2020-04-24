package org.folio.inventory.validation;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

public final class InstancePrecedingSucceedingTitleValidators {

  private InstancePrecedingSucceedingTitleValidators() {}

  public static CompletableFuture<Instance> refuseWhenUnconnectedHasNoTitle(
    Instance instance) {

    final ValidationError succeedingError = new ValidationError(
      "Title is required for unconnected succeeding title", "succeedingTitles.title", null);
    final ValidationError precedingError = new ValidationError(
      "Title is required for unconnected preceding title", "precedingTitles.title", null);

    return refuseWhenUnconnectedHasNoTitle(instance, instance.getSucceedingTitles(), succeedingError)
      .thenCompose(prev -> refuseWhenUnconnectedHasNoTitle(instance, instance.getPrecedingTitles(), precedingError));

  }

  private static CompletableFuture<Instance> refuseWhenUnconnectedHasNoTitle(
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
      .filter(InstancePrecedingSucceedingTitleValidators::isUnconnectedPrecedingSucceedingTitle)
      .anyMatch(title -> StringUtils.isBlank(title.title));
  }

  private static boolean isUnconnectedPrecedingSucceedingTitle(PrecedingSucceedingTitle title) {
    return title.precedingInstanceId == null && title.succeedingInstanceId == null;
  }
}
