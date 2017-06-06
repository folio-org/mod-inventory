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

    if(item.location != null) {
      representation.put("location",
        new JsonObject().put("name", item.location))
    }

    representation.put('links',
      ['self': context.absoluteUrl(
        "${relativeItemsPath}/${item.id}").toString()])

    representation
  }

  JsonObject toJson(
    Map wrappedItems,
    Map<String, JsonObject> materialTypes,
    Map<String, JsonObject> loanTypes,
    WebContext context) {

    def representation = new JsonObject()

    def results = new JsonArray()


    wrappedItems.items.each { item ->
      def materialType = materialTypes.get(item?.materialTypeId)
      def permanentLoanType = loanTypes.get(item?.permanentLoanTypeId)
      def temporaryLoanType = loanTypes.get(item?.temporaryLoanTypeId)

      results.add(toJson(item, materialType, permanentLoanType, temporaryLoanType, context))
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
