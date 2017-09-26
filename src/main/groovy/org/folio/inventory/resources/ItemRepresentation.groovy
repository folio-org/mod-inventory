package org.folio.inventory.resources

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.folio.inventory.common.WebContext
import org.folio.inventory.domain.Item

class ItemRepresentation {
  private final String relativeItemsPath

  def ItemRepresentation(String relativeItemsPath) {
    this.relativeItemsPath = relativeItemsPath
  }

  JsonObject toJson(
    Item item,
    JsonObject materialType,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    WebContext context) {

    def representation = toJson(item, context)

    if(materialType != null) {
      representation.getJsonObject("materialType")
        .put("name", materialType.getString("name"))
    }

    if(permanentLoanType != null) {
      representation.getJsonObject("permanentLoanType")
        .put("name", permanentLoanType.getString("name"))
    }

    if(temporaryLoanType != null) {
      representation.getJsonObject("temporaryLoanType")
        .put("name", temporaryLoanType.getString("name"))
    }

    if(permanentLocation != null) {
      representation.getJsonObject("permanentLocation")
        .put("name", permanentLocation.getString("name"))
    }

    if(temporaryLocation != null) {
      representation.getJsonObject("temporaryLocation")
        .put("name", temporaryLocation.getString("name"))
    }

    representation
  }

  JsonObject toJson(Item item, WebContext context) {
    def representation = new JsonObject()
    representation.put("id", item.id)
    representation.put("title", item.title)

    if(item.status != null) {
      representation.put("status", new JsonObject().put("name", item.status))
    }

    includeIfPresent(representation, "instanceId", item.instanceId)
    includeIfPresent(representation, "barcode", item.barcode)

    includeReferenceIfPresent(representation, "materialType",
      item.materialTypeId)

    includeReferenceIfPresent(representation, "permanentLoanType",
      item.permanentLoanTypeId)

    includeReferenceIfPresent(representation, "temporaryLoanType",
      item.temporaryLoanTypeId)

    includeReferenceIfPresent(representation, "permanentLocation",
      item.permanentLocationId)

    includeReferenceIfPresent(representation, "temporaryLocation",
      item.temporaryLocationId)

    representation.put('links',
      ['self': context.absoluteUrl(
        "${relativeItemsPath}/${item.id}").toString()])

    representation
  }

  JsonObject toJson(
    Map wrappedItems,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    Map<String, JsonObject> locations,
    WebContext context) {

    def representation = new JsonObject()

    def results = new JsonArray()


    wrappedItems.items.each { item ->
      def materialType = materialTypes.get(item?.materialTypeId)
      def permanentLoanType = loanTypes.get(item?.permanentLoanTypeId)
      def temporaryLoanType = loanTypes.get(item?.temporaryLoanTypeId)
      def permanentLocation = locations.get(item?.permanentLocationId)
      def temporaryLocation = locations.get(item?.temporaryLocationId)
      results.add(toJson(item, materialType, permanentLoanType, temporaryLoanType, 
        permanentLocation, temporaryLocation, context))
    }

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.totalRecords)

    representation
  }

  JsonObject toJson(Map wrappedItems,
                    WebContext context) {

    def representation = new JsonObject()

    def results = new JsonArray()

    wrappedItems.items.each { item ->
      results.add(toJson(item, context))
    }

    representation
      .put("items", results)
      .put("totalRecords", wrappedItems.totalRecords)

    representation
  }

  private void includeReferenceIfPresent(
    JsonObject representation,
    String referencePropertyName,
    String id) {

    if (id != null) {
      representation.put(referencePropertyName,
        new JsonObject().put("id", id))
    }
  }

  private void includeIfPresent(
    JsonObject representation,
    String propertyName,
    String propertyValue) {

    if (propertyValue != null) {
      representation.put(propertyName, propertyValue)
    }
  }
}
