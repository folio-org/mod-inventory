package org.folio.inventory.validation;

import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.UnprocessableEntityException;
import org.folio.inventory.support.http.server.ValidationError;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.logging.log4j.util.Strings.isNotBlank;
import static org.folio.inventory.support.CompletableFutures.failedFuture;

public final class InstanceSourceTypeValidator {

  private final static String[] INSTANCE_SOURCE_TYPES = {"MARC", "FOLIO", "CONSORTIUM-MARC", "CONSORTIUM-FOLIO"};

  private InstanceSourceTypeValidator() {}

  public static CompletableFuture<Instance> refuseWhenIncorrectSource(Instance instance) {

    final ValidationError incorrectSourceError = new ValidationError(
      "Instance source is incorrect", "source", instance.getSource());

    return isSourceTypeCorrect(instance.getSource())
      ? completedFuture(instance)
      : failedFuture(new UnprocessableEntityException(incorrectSourceError));
  }

  private static boolean isSourceTypeCorrect(String sourceType) {
    return isNotBlank(sourceType) && Arrays.stream(INSTANCE_SOURCE_TYPES).anyMatch(sourceType::equals);
  }

}
