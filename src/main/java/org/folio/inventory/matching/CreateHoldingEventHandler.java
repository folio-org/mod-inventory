package org.folio.inventory.matching;

import static org.folio.ActionProfile.FolioRecord.HOLDINGS;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

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

public class CreateHoldingEventHandler implements EventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateHoldingEventHandler.class);
  private static final String ERROR_EVENT_TYPE = "DI_ERROR";
  private static final String CREATED_HOLDINGS_RECORD_EVENT_TYPE = "DI_CREATED_HOLDINGS_RECORD";

  private Storage storage;

  public CreateHoldingEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      String instanceAsString = dataImportEventPayload.getContext().get(INSTANCE.value());
      //Check this string (empty)
      JsonObject instanceAsJson = new JsonObject(instanceAsString);
      String instanceId = instanceAsJson.getString("id");
      if (instanceId.isEmpty()){
        LOGGER.error("Error: 'instanceId' is empty");
        future.completeExceptionally(new EventProcessingException("Error: 'instanceId' is empty"));
      }

      JsonObject holdingJsonObject = new JsonObject().put("instanceId", instanceId);
      dataImportEventPayload.getContext().put(HOLDINGS.value(), holdingJsonObject.encode());
      MappingManager.map(dataImportEventPayload);
      String holdingAsJson = dataImportEventPayload.getContext().get(HOLDINGS.value());
      Holdingsrecord holding = ObjectMapperTool.getMapper().readValue(holdingAsJson, Holdingsrecord.class);

      if (!isValidHolding(holding)) {
        LOGGER.error("Holding entity isn`t valid");
        future.completeExceptionally(new EventProcessingException("Holding isn`t valid"));
      }

      Context context = constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      HoldingsRecordCollection holdingsRecords = storage.getHoldingsRecordCollection(context);
      holdingsRecords.add(holding, holdingSuccess -> {
        Holdingsrecord createdHolding = holdingSuccess.getResult();
        dataImportEventPayload.getContext().put(HOLDINGS.value(), Json.encodePrettily(createdHolding));
        dataImportEventPayload.setEventType(CREATED_HOLDINGS_RECORD_EVENT_TYPE);
        future.complete(dataImportEventPayload);
      }, failure -> dataImportEventPayload.setEventType(ERROR_EVENT_TYPE));
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

  private boolean isValidHolding(Holdingsrecord holding) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<Holdingsrecord>> constraintViolations = validator.validate(holding);
    return constraintViolations.isEmpty();
  }
}
