package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.items.CirculationNote;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.ItemCollection;
import org.folio.inventory.domain.items.ItemStatusName;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.CqlHelper;
import org.folio.inventory.support.ItemUtil;
import org.folio.inventory.support.JsonHelper;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.processing.mapping.MappingManager;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.ActionProfile.FolioRecord.ITEM;
import static org.folio.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class CreateItemEventHandler implements EventHandler {

  private static final String PAYLOAD_HAS_NO_DATA_MSG = "Failed to handle event payload, cause event payload context does not contain MARC_BIBLIOGRAPHIC data";
  private static final String PAYLOAD_DATA_HAS_NO_HOLDING_ID_MSG = "Failed to extract holdingsRecordId from holdingsRecord entity or parsed record";
  public static final String HOLDINGS_RECORD_ID_FIELD = "holdingsRecordId";
  public static final String HOLDING_ID_FIELD = "id";
  public static final String ITEM_ID_FIELD = "id";

  private static final Logger LOG = LoggerFactory.getLogger(CreateItemEventHandler.class);


  private final DateTimeFormatter dateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").withZone(ZoneOffset.UTC);

  private final List<String> requiredFields = Arrays.asList("status.name", "materialType.id", "permanentLoanType.id", "holdingsRecordId");

  private Storage storage;

  public CreateItemEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      HashMap<String, String> payloadContext = dataImportEventPayload.getContext();
      if (payloadContext == null || isBlank(payloadContext.get(EntityType.MARC_BIBLIOGRAPHIC.value()))) {
        LOG.error(PAYLOAD_HAS_NO_DATA_MSG);
        future.completeExceptionally(new EventProcessingException(PAYLOAD_HAS_NO_DATA_MSG));
        return future;
      }

      Context context = EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(), dataImportEventPayload.getOkapiUrl());
      dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
      dataImportEventPayload.setCurrentNode(dataImportEventPayload.getCurrentNode().getChildSnapshotWrappers().get(0));
      dataImportEventPayload.getContext().put(ITEM.value(), new JsonObject().encode());

      MappingManager.map(dataImportEventPayload);
      JsonObject itemAsJson = new JsonObject(dataImportEventPayload.getContext().get(ITEM.value()));
      fillHoldingsRecordIdIfNecessary(dataImportEventPayload, itemAsJson);
      itemAsJson.put(ITEM_ID_FIELD, UUID.randomUUID().toString());

      ItemCollection itemCollection = storage.getItemCollection(context);
      List<String> errors = validateItem(itemAsJson, requiredFields);
      if (errors.isEmpty()) {
        Item mappedItem = ItemUtil.jsonToItem(itemAsJson);
        isItemBarcodeUnique(itemAsJson.getString("barcode"), itemCollection)
          .compose(isUnique -> isUnique
            ? addItem(mappedItem, itemCollection)
            : Future.failedFuture(String.format("Barcode must be unique, %s is already assigned to another item", itemAsJson.getString("barcode"))))
          .setHandler(ar -> {
            if (ar.succeeded()) {
              dataImportEventPayload.getContext().put(ITEM.value(), itemAsJson.encode());
              dataImportEventPayload.setEventType(DI_INVENTORY_ITEM_CREATED.value());
              future.complete(dataImportEventPayload);
            } else {
              LOG.error("Error creating inventory Item", ar.cause());
              future.completeExceptionally(ar.cause());
            }
          });
      } else {
        String msg = String.format("Mapped Item is invalid: %s", errors.toString());
        LOG.error(msg);
        future.completeExceptionally(new EventProcessingException(msg));
      }
    } catch (Exception e) {
      LOG.error("Error creating inventory Item", e);
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && ACTION_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      ActionProfile actionProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(ActionProfile.class);
      return actionProfile.getAction() == CREATE && actionProfile.getFolioRecord() == ITEM;
    }
    return false;
  }

  private void fillHoldingsRecordIdIfNecessary(DataImportEventPayload dataImportEventPayload, JsonObject itemAsJson) throws IOException {
    if (isBlank(itemAsJson.getString(HOLDINGS_RECORD_ID_FIELD))) {
      String holdingsId = null;
      String holdingAsString = dataImportEventPayload.getContext().get(EntityType.HOLDINGS.value());

      if (StringUtils.isNotEmpty(holdingAsString)) {
        JsonObject holdingsRecord = new JsonObject(holdingAsString);
        holdingsId = holdingsRecord.getString(HOLDING_ID_FIELD);
      }
      if (isBlank(holdingsId)) {
        String recordAsString = dataImportEventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value());
        Record record = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);
        holdingsId = ParsedRecordUtil.getAdditionalSubfieldValue(record.getParsedRecord(), ParsedRecordUtil.AdditionalSubfields.H);
      }
      if (isBlank(holdingsId)) {
        LOG.error(PAYLOAD_DATA_HAS_NO_HOLDING_ID_MSG);
        throw new EventProcessingException(PAYLOAD_DATA_HAS_NO_HOLDING_ID_MSG);
      }
      itemAsJson.put(HOLDINGS_RECORD_ID_FIELD, holdingsId);
    }
  }

  private void validateStatusName(JsonObject itemAsJson, List<String> errors) {
    String statusName = JsonHelper.getNestedProperty(itemAsJson, "status", "name");
    if (StringUtils.isNotBlank(statusName) && !ItemStatusName.isStatusCorrect(statusName)) {
      errors.add(String.format("Invalid status specified '%s'", statusName));
    }
  }

  private List<String> validateItem(JsonObject itemAsJson, List<String> requiredFields) {
    List<String> errors = EventHandlingUtil.validateJsonByRequiredFields(itemAsJson, requiredFields);
    validateStatusName(itemAsJson, errors);
    return errors;
  }

  private Future<Boolean> isItemBarcodeUnique(String barcode, ItemCollection itemCollection) throws UnsupportedEncodingException {
    Future<Boolean> future = Future.future();
    itemCollection.findByCql(CqlHelper.barcodeIs(barcode), PagingParameters.defaults(),
      findResult -> future.complete(findResult.getResult().records.isEmpty()),
      failure -> future.fail(failure.getReason()));
    return future;
  }

  private Future<Item> addItem(Item item, ItemCollection itemCollection) {
    Future<Item> future = Future.future();
    List<CirculationNote> notes = item.getCirculationNotes()
      .stream()
      .map(note -> note.withId(UUID.randomUUID().toString()))
      .map(note -> note.withSource(null))
      .map(note -> note.withDate(dateTimeFormatter.format(ZonedDateTime.now())))
      .collect(Collectors.toList());

    itemCollection.add(item.withCirculationNotes(notes), success -> future.complete(success.getResult()),
      failure -> {
        LOG.error("Error posting Item cause %s, status code %s", failure.getReason(), failure.getStatusCode());
        future.fail(failure.getReason());
      });
    return future;
  }
}
