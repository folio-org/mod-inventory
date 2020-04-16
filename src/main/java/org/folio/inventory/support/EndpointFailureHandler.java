package org.folio.inventory.support;

import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;

import java.util.function.Function;

import org.folio.inventory.exceptions.AbstractInventoryException;
import org.folio.inventory.storage.external.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.ForwardResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.validation.exceptions.NotFoundException;
import org.folio.inventory.validation.exceptions.UnprocessableEntityException;

import io.vertx.core.AsyncResult;
import io.vertx.ext.web.RoutingContext;

public final class EndpointFailureHandler {

  public static <T> void handleFailure(AsyncResult<T> result, RoutingContext context) {
    if (result.succeeded()) {
      return;
    }

    handleFailure(result.cause(), context);
  }

  public static void handleFailure(Throwable originFailure, RoutingContext context) {
    final Throwable failure = getKnownException(originFailure);

    if (failure instanceof UnprocessableEntityException) {
      UnprocessableEntityException validationFailure =
        (UnprocessableEntityException) failure;

      unprocessableEntity(context.response(), validationFailure.getValidationError());
    } else if (failure instanceof NotFoundException) {
      ClientErrorResponse.notFound(context.response(), failure.getMessage());
    } else if (failure instanceof ExternalResourceFetchException) {
      final ExternalResourceFetchException externalException =
        (ExternalResourceFetchException) failure;

      ForwardResponse.forward(context.response(), externalException.getFailedResponse());
    } else {
      ServerErrorResponse.internalError(context.response(), failure);
    }
  }

  public static <T> Function<Throwable, T> doExceptionally(
    RoutingContext context) {

    return failure -> {
      handleFailure(failure, context);
      return null;
    };
  }

  public static Throwable getKnownException(Throwable throwable) {
    if (throwable instanceof AbstractInventoryException) {
      return throwable;
    }

    if (throwable.getCause() instanceof AbstractInventoryException) {
      return throwable.getCause();
    }

    return throwable;
  }
}
