package org.folio.inventory.resources;

import io.vertx.core.Promise;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HoldingsRecord;
import org.folio.HoldingsUpdateOwnership;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.exceptions.BadRequestException;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.storage.external.CollectionResourceClient;
import org.folio.inventory.storage.external.MultipleRecordsFetchClient;
import org.folio.inventory.support.MoveApiUtil;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.support.EndpointFailureHandler.handleFailure;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsRecordsFetchClient;
import static org.folio.inventory.support.MoveApiUtil.createHoldingsStorageClient;
import static org.folio.inventory.support.MoveApiUtil.createHttpClient;
import static org.folio.inventory.support.MoveApiUtil.respond;
import static org.folio.inventory.support.http.server.JsonResponse.unprocessableEntity;
import static org.folio.inventory.validation.UpdateOwnershipValidator.updateOwnershipHasRequiredFields;

public class UpdateOwnershipApi extends AbstractInventoryResource {
  private static final Logger LOGGER = LogManager.getLogger(MethodHandles.lookup().lookupClass());
  public static final String INSTANCE_NOT_SHARED = "Instance with id: %s is not shared";
  public static final String INSTANCE_NOT_FOUND_AT_SOURCE_TENANT = "Instance with id: %s not found at source tenant, tenant: %s";
  public static final String TENANT_NOT_IN_CONSORTIA = "%s tenant is not in consortia";
  private final ConsortiumService consortiumService;

  public UpdateOwnershipApi(Storage storage, HttpClient client, ConsortiumService consortiumService) {
    super(storage, client);
    this.consortiumService = consortiumService;
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/update-ownership")
      .handler(this::updateItemsOwnership);
    router.post("/inventory/holdings/update-ownership")
      .handler(this::updateHoldingsOwnership);
  }

  private void updateHoldingsOwnership(RoutingContext routingContext) {
    try {
      final var context = new WebContext(routingContext);
      final var updateOwnershipRequest = routingContext.body().asJsonObject();

      final var validationError = updateOwnershipHasRequiredFields(context.getTenantId(), updateOwnershipRequest, HoldingsUpdateOwnership.class);

      if (validationError.isPresent()) {
        unprocessableEntity(routingContext.response(), validationError.get());
        return;
      }
      var holdingsUpdateOwnership = updateOwnershipRequest.mapTo(HoldingsUpdateOwnership.class);

      LOGGER.info("updateHoldingsOwnership:: Started updating ownership of holdings record: {}, to tenant: {}", holdingsUpdateOwnership.getHoldingsRecordIds(),
        holdingsUpdateOwnership.getTargetTenantId());

      consortiumService.getConsortiumConfiguration(context).toCompletionStage().toCompletableFuture()
        .thenCompose(consortiumConfigurationOptional -> {
          if (consortiumConfigurationOptional.isPresent()) {
            return storage.getInstanceCollection(context)
              .findById(holdingsUpdateOwnership.getToInstanceId())
              .thenCompose(instance -> {
                if (instance != null) {
                  if (instance.getSource().equals(CONSORTIUM_MARC.getValue()) || instance.getSource().equals(CONSORTIUM_FOLIO.getValue())) {
                    return updateOwnershipOfHoldingsRecords(holdingsUpdateOwnership, routingContext, context);
                  } else {
                    String instanceNotSharedErrorMessage = String.format(INSTANCE_NOT_SHARED, holdingsUpdateOwnership.getToInstanceId());
                    LOGGER.warn("updateHoldingsOwnership:: " + instanceNotSharedErrorMessage);
                    return CompletableFuture.failedFuture(new BadRequestException(instanceNotSharedErrorMessage));
                  }
                } else {
                  String instanceNotFoundErrorMessage = String.format(INSTANCE_NOT_FOUND_AT_SOURCE_TENANT, holdingsUpdateOwnership.getToInstanceId(), context.getTenantId());
                  LOGGER.warn("updateHoldingsOwnership:: " + instanceNotFoundErrorMessage);
                  return CompletableFuture.failedFuture(new NotFoundException(instanceNotFoundErrorMessage));
                }
              });
          }
          String notInConsortiaErrorMessage = String.format(TENANT_NOT_IN_CONSORTIA, context.getTenantId());
          LOGGER.warn("updateHoldingsOwnership:: " + notInConsortiaErrorMessage, context);
          return CompletableFuture.failedFuture(new BadRequestException(notInConsortiaErrorMessage)); // NEED TEST
        })
        .thenAccept(updateHoldingsRecords -> respond(routingContext, holdingsUpdateOwnership.getHoldingsRecordIds(), updateHoldingsRecords))
        .exceptionally(throwable -> {
          LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of holdings {}, to tenant: {}",
            holdingsUpdateOwnership.getHoldingsRecordIds(), holdingsUpdateOwnership.getTargetTenantId(), throwable);
          handleFailure(throwable, routingContext);
          return null;
        });
    } catch (Exception e) {
      LOGGER.warn("updateHoldingsOwnership:: Error during update ownership of holdings", e);
      handleFailure(e, routingContext);
    }
  }

  private void updateItemsOwnership(RoutingContext routingContext) {
    // should be implemented in MODINV-1031
  }

  private CompletableFuture<List<String>> updateOwnershipOfHoldingsRecords(HoldingsUpdateOwnership holdingsUpdateOwnership, RoutingContext routingContext,
                                                                           WebContext context) {
    try {
      Context targetTenantContext = constructContext(holdingsUpdateOwnership.getTargetTenantId(), context.getToken(), context.getOkapiLocation());

      CollectionResourceClient holdingsStorageClient = createHoldingsStorageClient(createHttpClient(client, routingContext, context),
        context);
      MultipleRecordsFetchClient holdingsRecordFetchClient = createHoldingsRecordsFetchClient(holdingsStorageClient);

      HoldingsRecordCollection sourceTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(context);
      HoldingsRecordCollection targetTenantHoldingsRecordCollection = storage.getHoldingsRecordCollection(targetTenantContext);

      return holdingsRecordFetchClient.find(holdingsUpdateOwnership.getHoldingsRecordIds(), MoveApiUtil::fetchByIdCql)
        .thenCompose(jsons ->
          createHoldings(jsons, holdingsUpdateOwnership.getToInstanceId(), targetTenantHoldingsRecordCollection)
            .thenCompose(createdHoldings -> {
              List<String> createdHoldingsIds = createdHoldings.stream().map(HoldingsRecord::getId).toList();
              return deleteHoldings(createdHoldingsIds, sourceTenantHoldingsRecordCollection);
            }));
    } catch (Exception e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<List<HoldingsRecord>> createHoldings(List<JsonObject> jsons, String instanceId,
                                                                 HoldingsRecordCollection holdingsRecordCollection) {
    List<HoldingsRecord> holdingsRecordsToUpdateOwnership = jsons.stream()
      .map(json -> json.mapTo(HoldingsRecord.class))
      .filter(holdingsRecord -> holdingsRecord.getInstanceId().equals(instanceId))
      .toList();

    List<CompletableFuture<HoldingsRecord>> createFutures = holdingsRecordsToUpdateOwnership.stream()
      .map(holdingsRecordCollection::add)
      .toList();

    return CompletableFuture.allOf(createFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> createFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }

  private CompletableFuture<List<String>> deleteHoldings(List<String> holdingsRecordIds, HoldingsRecordCollection holdingsRecordCollection) {
    List<CompletableFuture<String>> deleteFutures = holdingsRecordIds.stream()
      .map(holdingsRecordId -> {
        Promise<String> promise = Promise.promise();
        holdingsRecordCollection.delete(holdingsRecordId, success -> promise.complete(holdingsRecordId), failure -> promise.fail(failure.getReason()));
        return promise.future().toCompletionStage().toCompletableFuture();
      }).toList();

    return CompletableFuture.allOf(deleteFutures.toArray(new CompletableFuture[0]))
      .handle((vVoid, throwable) -> deleteFutures.stream()
        .filter(future -> !future.isCompletedExceptionally())
        .map(CompletableFuture::join)
        .toList());
  }
}
