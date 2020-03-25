package org.folio.inventory.support;

import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;

import org.folio.inventory.exceptions.AbstractInventoryException;
import org.folio.inventory.exceptions.ExternalResourceFetchException;
import org.folio.inventory.support.http.server.ClientErrorResponse;
import org.folio.inventory.support.http.server.ForwardResponse;
import org.folio.inventory.support.http.server.ServerErrorResponse;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.exceptions.UnprocessableEntityException;

import io.vertx.core.AsyncResult;
import io.vertx.ext.web.RoutingContext;

public final class EndpointFailureHandler {

  public static <T> void handleFailure(AsyncResult<T> result, RoutingContext context) {
    if (result.succeeded()) {
      return;
    }

    handleFailure(result.cause(), context);
  }

  public static void handleFailure(Throwable failure, RoutingContext context) {
    final Throwable failureToHandle = getKnownException(failure);

    if (failureToHandle instanceof UnprocessableEntityException) {
      UnprocessableEntityException validationFailure =
        (UnprocessableEntityException) failureToHandle;

      unprocessableEntity(context.response(), validationFailure.getValidationError());
    } else if (failureToHandle instanceof NotFoundException) {
      ClientErrorResponse.notFound(context.response(), failureToHandle.getMessage());
    } else if (failureToHandle instanceof ExternalResourceFetchException) {
      final ExternalResourceFetchException externalException =
        (ExternalResourceFetchException) failureToHandle;

      ForwardResponse.forward(context.response(), externalException.getFailedResponse());
    } else {
      ServerErrorResponse.internalError(context.response(), failureToHandle);
    }
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
