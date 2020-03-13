package org.folio.inventory.handlers;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.Holdingsrecord;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.HoldingsRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingEventHandler.class);
  private static final String CREATED_HOLDINGS_RECORD_EVENT_TYPE = "DI_CREATED_HOLDINGS_RECORD";
  private static final String INSTANCE_ID_ERROR_MESSAGE = "Error: 'instanceId' is empty";
  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String PERMANENT_LOCATION_ID_FIELD = "permanentLocationId";

  private Storage storage;
  private CompletableFuture<DataImportEventPayload> future;

  public CreateHoldingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    try {
      future = new CompletableFuture<>();
      prepareEvent(dataImportEventPayload);
      MappingManager.map(dataImportEventPayload);
      JsonObject holdingAsJson = new JsonObject(dataImportEventPayload.getContext().get(HOLDINGS.value()));

      fillInstanceIdIfNeeded(dataImportEventPayload, holdingAsJson);

      if (isEmpty(holdingAsJson.getString(PERMANENT_LOCATION_ID_FIELD))) {
        throwException("Can`t create Holding entity: 'permanentLocationId' is empty");
      } else {
        Holdingsrecord holding = ObjectMapperTool.getMapper().readValue(dataImportEventPayload.getContext().get(HOLDINGS.value()), Holdingsrecord.class);
        if (!isValidHolding(holding)) {
          throwException("Can`t create Holding entity: holding entity isn`t valid");
        } else {
          Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
          HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
          holdingsRecords.add(holding, holdingSuccess -> constructDataImportEventPayload(dataImportEventPayload, holdingSuccess),
            failure -> throwException("Can`t save new holding"));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Can`t create Holding entity", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == ActionProfile.Action.CREATE && actionProfile.getFolioRecord() == ActionProfile.FolioRecord.HOLDINGS;
    }
    return false;
  }

  private void prepareEvent(DataImportEventPayload dataImportEventPayload) {
    dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    dataImportEventPayload.getContext().put(HOLDINGS.value(), new JsonObject().encode());
    dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  private void constructDataImportEventPayload(DataImportEventPayload dataImportEventPayload, Success<Holdingsrecord> holdingSuccess) {
    Holdingsrecord createdHolding = holdingSuccess.getResult();
    dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(createdHolding));
    dataImportEventPayload.setEventType(CREATED_HOLDINGS_RECORD_EVENT_TYPE);
    future.complete(dataImportEventPayload);
  }

  private void fillInstanceIdIfNeeded(DataImportEventPayload dataImportEventPayload, JsonObject holdingAsJson) {
    if (isEmpty(holdingAsJson.getString(INSTANCE_ID_FIELD))) {
      String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
      JsonObject instanceAsJson = new JsonObject(instanceAsString);
      String instanceId = instanceAsJson.getString("id");
      if (isEmpty(instanceId)) {
        throwException(INSTANCE_ID_ERROR_MESSAGE);
      } else {
        holdingAsJson.put(INSTANCE_ID_FIELD, instanceId);
        dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingAsJson.encode());
      }
    }
  }

  private boolean isValidHolding(Holdingsrecord holding) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<Holdingsrecord>> constraintViolations = validator.validate(holding);
    return constraintViolations.isEmpty();
  }

  private void throwException(String errorMessage) {
    LOGGER.error(errorMessage);
    future.completeExceptionally(new EventProcessingException(errorMessage));
  }

  private Context constructContext(String tenantId, String token, String okapiUrl) {

    return new Context() {
      @Override
      public String getTenantId() {
        return tenantId;
      }

      @Override
      public String getToken() {
        return token;
      }

      @Override
      public String getOkapiLocation() {
        return okapiUrl;
      }

      @Override
      public String getUserId() {
        return "";
      }
    };
  }
}
