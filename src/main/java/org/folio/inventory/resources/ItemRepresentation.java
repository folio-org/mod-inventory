package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.Item;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.folio.inventory.support.HoldingsSupport.*;

class ItemRepresentation {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String relativeItemsPath;

  public ItemRepresentation(String relativeItemsPath) {
    this.relativeItemsPath = relativeItemsPath;
  }

  public JsonObject toJson(
    Item item,
    JsonObject instance,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    WebContext context) {

    JsonObject representation = toJson(item, instance, context);

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

    return representation;
  }

  JsonObject toJson(
    Item item,
    JsonObject instance,
    WebContext context) {

    JsonObject representation = new JsonObject();
    representation.put("id", item.id);

    if(item.status != null) {
      representation.put("status", new JsonObject().put("name", item.status));
    }

    includeIfPresent(representation, "title", instance, i -> i.getString("title"));
    includeIfPresent(representation, "holdingsRecordId", item.holdingId);
    includeIfPresent(representation, "barcode", item.barcode);
    includeIfPresent(representation, "enumeration", item.enumeration);
    includeIfPresent(representation, "chronology", item.chronology);
    representation.put("pieceIdentifiers",item.pieceIdentifiers);
    representation.put("notes", item.notes);
    includeIfPresent(representation, "numberOfPieces", item.numberOfPieces);

    includeReferenceIfPresent(representation, "materialType",
      item.materialTypeId);

    includeReferenceIfPresent(representation, "permanentLoanType",
      item.permanentLoanTypeId);

    includeReferenceIfPresent(representation, "temporaryLoanType",
      item.temporaryLoanTypeId);
    
    includeReferenceIfPresent(representation, "permanentLocation",
      item.permanentLocationId);
    
    includeReferenceIfPresent(representation, "temporaryLocation",
      item.temporaryLocationId);

    includeIfPresent(representation, "metadata", item.metadata);

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        relativeItemsPath, item.id));

      representation.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      log.warn(String.format("Failed to create self link for item: %s", e.toString()));
    }

    return representation;
  }

  public JsonObject toJson(
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

    items.stream().forEach(item -> {
      JsonObject materialType = materialTypes.get(item.materialTypeId);
      JsonObject permanentLoanType = loanTypes.get(item.permanentLoanTypeId);
      JsonObject temporaryLoanType = loanTypes.get(item.temporaryLoanTypeId);

      JsonObject holding = holdingForItem(item, holdings).orElse(null);

      JsonObject instance = instanceForHolding(holding, instances).orElse(null);

      String effectiveLocationId = determineEffectiveLocationIdForItem(
        holding);

      JsonObject permanentLocation = locations.get(item.permanentLocationId);
      JsonObject temporaryLocation = locations.get(item.temporaryLocationId);

      results.add(toJson(item, instance, materialType, permanentLoanType,
        temporaryLoanType, permanentLocation, temporaryLocation, context));
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
