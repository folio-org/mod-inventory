package org.folio.inventory.resources;

import static org.folio.inventory.support.HoldingsSupport.*;

import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.folio.inventory.common.WebContext;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.items.Item;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

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

    if(item.getStatus() != null) {
      representation.put("status", new JsonObject().put("name", item.getStatus()));
    }

    includeIfPresent(representation, "title", instance, i -> i.getString("title"));
    includeIfPresent(representation, "callNumber", holding, h -> h.getString("callNumber"));
    includeIfPresent(representation, "holdingsRecordId", item.getHoldingId());
    includeIfPresent(representation, "barcode", item.getBarcode());
    includeIfPresent(representation, "enumeration", item.getEnumeration());
    includeIfPresent(representation, "chronology", item.getChronology());
    representation.put("copyNumbers",item.getCopyNumbers());
    representation.put("notes", item.getNotes());
    includeIfPresent(representation, "numberOfPieces", item.getNumberOfPieces());

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

    return representation;
  }

  JsonObject toJson(
    MultipleRecords<Item> wrappedItems,
    Collection<JsonObject> holdings,
    Collection<JsonObject> instances,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    Map<String, JsonObject> locations,
    Map<String, JsonObject> effectiveLocations,
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

      String effectiveLocationId = determineEffectiveLocationIdForItem(
        holding, item);
      JsonObject effectiveLocation = effectiveLocations.get(effectiveLocationId);
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
