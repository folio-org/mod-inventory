package org.folio.inventory.resources;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.Item;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

class ItemRepresentation {
  private final String relativeItemsPath;

  public ItemRepresentation(String relativeItemsPath) {
    this.relativeItemsPath = relativeItemsPath;
  }

  public JsonObject toJson(
    Item item,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    WebContext context) {

    JsonObject representation = toJson(item, context);

    if(materialType != null) {
      representation.getJsonObject("materialType")
        .put("name", materialType.getString("name"));
    }

    if(permanentLoanType != null) {
      representation.getJsonObject("permanentLoanType")
        .put("name", permanentLoanType.getString("name"));
    }

    if(temporaryLoanType != null) {
      representation.getJsonObject("temporaryLoanType")
        .put("name", temporaryLoanType.getString("name"));
    }

    if(permanentLocation != null) {
      representation.getJsonObject("permanentLocation")
        .put("name", permanentLocation.getString("name"));
    }

    if(temporaryLocation != null) {
      representation.getJsonObject("temporaryLocation")
        .put("name", temporaryLocation.getString("name"));
    }

    return representation;
  }

  JsonObject toJson(Item item, WebContext context) {

    JsonObject representation = new JsonObject();
    representation.put("id", item.id);
    representation.put("title", item.title);

    if(item.status != null) {
      representation.put("status", new JsonObject().put("name", item.status));
    }

    includeIfPresent(representation, "instanceId", item.instanceId);
    includeIfPresent(representation, "barcode", item.barcode);

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

    try {
      URL selfUrl = context.absoluteUrl(String.format("%s/%s",
        relativeItemsPath, item.id));

      representation.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      System.out.println(String.format("Failed to create self link for item: " + e.toString()));
    }

    return representation;
  }

  public JsonObject toJson(
    Map<String, Object> wrappedItems,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    Map<String, JsonObject> locations,
    WebContext context) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Item> items = (List<Item>)wrappedItems.get("items");

    items.stream().forEach(item -> {
      JsonObject materialType = materialTypes.get(item.materialTypeId);
      JsonObject permanentLoanType = loanTypes.get(item.permanentLoanTypeId);
      JsonObject temporaryLoanType = loanTypes.get(item.temporaryLoanTypeId);
      JsonObject permanentLocation = locations.get(item.permanentLocationId);
      JsonObject temporaryLocation = locations.get(item.temporaryLocationId);
      results.add(toJson(item, materialType, permanentLoanType, temporaryLoanType,
        permanentLocation, temporaryLocation, context));
    });

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.get("totalRecords"));

    return representation;
  }

  public JsonObject toJson(Map wrappedItems,
                    WebContext context) {

    JsonObject representation = new JsonObject();

    JsonArray results = new JsonArray();

    List<Item> items = (List<Item>)wrappedItems.get("items");

    items.forEach(item -> results.add(toJson(item, context)));

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.get("totalRecords"));

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
}
