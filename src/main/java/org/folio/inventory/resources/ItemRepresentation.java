package org.folio.inventory.resources;

import static org.folio.inventory.support.HoldingsSupport.holdingForItem;
import static org.folio.inventory.support.HoldingsSupport.instanceForHolding;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.items.Item;
import org.folio.inventory.domain.items.Status;

class ItemRepresentation {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String relativeItemsPath;

  ItemRepresentation(String relativeItemsPath) {
    this.relativeItemsPath = relativeItemsPath;
  }

  JsonObject toJson(
    Item item,
    JsonObject holding,
    JsonObject instance,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    JsonObject effectiveLocation,
    WebContext context) {

    JsonObject representation = toJson(item, holding, instance, context);

    if(materialType != null) {
      representation.getJsonObject("materialType")
        .put("id", materialType.getString("id"))
        .put("name", materialType.getString("name"));
    }

    if(permanentLoanType != null) {
      representation.getJsonObject("permanentLoanType")
        .put("id", permanentLoanType.getString("id"))
        .put("name", permanentLoanType.getString("name"));
    }

    if(temporaryLoanType != null) {
      representation.getJsonObject("temporaryLoanType")
        .put("id", temporaryLoanType.getString("id"))
        .put("name", temporaryLoanType.getString("name"));
    }

    if(permanentLocation != null) {
      representation.getJsonObject("permanentLocation")
        .put("id", permanentLocation.getString("id"))
        .put("name", permanentLocation.getString("name"));
    }

    if(temporaryLocation != null) {
      representation.getJsonObject("temporaryLocation")
        .put("id", temporaryLocation.getString("id"))
        .put("name", temporaryLocation.getString("name"));
    }

    if (effectiveLocation != null) {
      if(representation.containsKey("effectiveLocation")) {
        representation.getJsonObject("effectiveLocation")
          .put("id", effectiveLocation.getString("id"))
          .put("name", effectiveLocation.getString("name"));
      }
      representation.put("effectiveLocation", new JsonObject()
        .put("id", effectiveLocation.getString("id"))
        .put("name", effectiveLocation.getString("name")));
    }

    return representation;
  }

  private JsonObject toJson(
    Item item,
    JsonObject holding,
    JsonObject instance,
    WebContext context) {

    JsonObject representation = new JsonObject();
    representation.put("id", item.id);

    if(item.getStatus().getString(Status.NAME_KEY) != null) {
      representation.put(Item.STATUS_KEY, item.status.getJson());
    }

    List<JsonObject> contributorNames = new ArrayList<>();
    instance.getJsonArray("contributors").forEach((contributor) -> {
      JsonObject contributorName = new JsonObject();
      contributorName.put("name", ((JsonObject)contributor).getString("name"));
      contributorNames.add(contributorName);
    });

    includeIfPresent(representation, "title", instance, i -> i.getString("title"));
    includeIfPresent(representation, "callNumber", holding, h -> h.getString("callNumber"));
    includeIfPresent(representation, Item.HRID_KEY, item.getHrid());
    representation.put("contributorNames", contributorNames);
    representation.put(Item.FORMER_IDS_KEY, item.getFormerIds());
    representation.put(Item.DISCOVERY_SUPPRESS_KEY, item.getDiscoverySuppress());
    includeIfPresent(representation, "holdingsRecordId", item.getHoldingId());
    includeIfPresent(representation, "barcode", item.getBarcode());
    includeIfPresent(representation, Item.ITEM_LEVEL_CALL_NUMBER_KEY, item.getItemLevelCallNumber());
    includeIfPresent(representation, Item.ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY, item.getItemLevelCallNumberPrefix());
    includeIfPresent(representation, Item.ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY, item.getItemLevelCallNumberSuffix());
    includeIfPresent(representation, Item.ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY, item.getItemLevelCallNumberTypeId());
    includeIfPresent(representation, Item.VOLUME_KEY, item.getVolume());
    includeIfPresent(representation, "enumeration", item.getEnumeration());
    includeIfPresent(representation, "chronology", item.getChronology());
    includeIfPresent(representation, Item.COPY_NUMBER_KEY, item.getCopyNumber());
    representation.put(Item.NOTES_KEY, item.getNotes());
    representation.put(Item.CIRCULATION_NOTES_KEY, item.getCirculationNotes());
    includeIfPresent(representation, "numberOfPieces", item.getNumberOfPieces());
    includeIfPresent(representation, Item.DESCRIPTION_OF_PIECES_KEY, item.getDescriptionOfPieces());
    includeIfPresent(representation, Item.NUMBER_OF_MISSING_PIECES_KEY, item.getNumberOfMissingPieces());
    includeIfPresent(representation, Item.MISSING_PIECES_KEY, item.getMissingPieces());
    includeIfPresent(representation, Item.MISSING_PIECES_DATE_KEY, item.getMissingPiecesDate());
    includeIfPresent(representation, Item.ITEM_DAMAGED_STATUS_ID_KEY, item.getItemDamagedStatusId());
    includeIfPresent(representation, Item.ITEM_DAMAGED_STATUS_DATE_KEY, item.getItemDamagedStatusDate());
    includeIfPresent(representation, Item.ACCESSION_NUMBER_KEY, item.getAccessionNumber());
    includeIfPresent(representation, Item.ITEM_IDENTIFIER_KEY, item.getItemIdentifier());
    includeIfPresent(representation,Item.TAGS_KEY, new JsonObject().put(Item.TAG_LIST_KEY, new JsonArray(item.getTags())));
    representation.put(Item.YEAR_CAPTION_KEY, item.getYearCaption());
    representation.put(Item.ELECTRONIC_ACCESS_KEY, item.getElectronicAccess());
    representation.put(Item.STATISTICAL_CODE_IDS_KEY, item.getStatisticalCodeIds());
    representation.put(Item.PURCHASE_ORDER_LINE_IDENTIFIER, item.getPurchaseOrderLineidentifier());
    includeReferenceIfPresent(representation, "materialType",
      item.getMaterialTypeId());

    includeReferenceIfPresent(representation, "permanentLoanType",
      item.getPermanentLoanTypeId());

    includeReferenceIfPresent(representation, "temporaryLoanType",
      item.getTemporaryLoanTypeId());

    includeReferenceIfPresent(representation, "permanentLocation",
      item.getPermanentLocationId());

    includeReferenceIfPresent(representation, "temporaryLocation",
      item.getTemporaryLocationId());

    includeIfPresent(representation, "metadata", item.getMetadata());

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        relativeItemsPath, item.id));

      representation.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      log.warn(String.format("Failed to create self link for item: %s", e.toString()));
    }

    if (item.getLastCheckIn() != null) {
      representation.put("lastCheckIn", item.getLastCheckIn().toJson());
    }

    if (item.getEffectiveCallNumberComponents() != null) {
      representation.put(
        "effectiveCallNumberComponents",
        item.getEffectiveCallNumberComponents().toJson());
    }

    return representation;
  }

  JsonObject toJson(
    MultipleRecords<Item> wrappedItems,
    Collection<JsonObject> holdings,
    Collection<JsonObject> instances,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    Map<String, JsonObject> locations,
    WebContext context) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Item> items = wrappedItems.records;

    items.forEach(item -> {
      JsonObject materialType = materialTypes.get(item.getMaterialTypeId());
      JsonObject permanentLoanType = loanTypes.get(item.getPermanentLoanTypeId());
      JsonObject temporaryLoanType = loanTypes.get(item.getTemporaryLoanTypeId());

      JsonObject holding = holdingForItem(item, holdings).orElse(null);

      JsonObject instance = instanceForHolding(holding, instances).orElse(null);

      JsonObject effectiveLocation = locations.get(item.getEffectiveLocationId());
      JsonObject permanentLocation = locations.get(item.getPermanentLocationId());
      JsonObject temporaryLocation = locations.get(item.getTemporaryLocationId());

      results.add(toJson(item, holding, instance, materialType, permanentLoanType,
        temporaryLoanType, permanentLocation, temporaryLocation, effectiveLocation, context));
    });

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.totalRecords);

    return representation;
  }

  private void includeReferenceIfPresent(
    JsonObject representation,
    String referencePropertyName,
    String id) {

    if (id != null) {
      representation.put(referencePropertyName,
        new JsonObject().put("id", id));
    }
  }

  private void includeIfPresent(
    JsonObject representation,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      representation.put(propertyName, propertyValue);
    }
  }

  private void includeIfPresent(
    JsonObject representation,
    String propertyName,
    JsonObject propertyValue) {

    if (propertyValue != null) {
      representation.put(propertyName, propertyValue);
    }
  }

  private void includeIfPresent(
    JsonObject representation,
    String propertyName,
    JsonObject from,
    Function<JsonObject, String> propertyValue) {

    if (from != null) {
      String value = propertyValue.apply(from);

      if(value != null) {
        representation.put(propertyName, value);
      }
    }
  }
}
