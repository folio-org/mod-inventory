package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class ItemRequestBuilder implements Builder {

  private static final String AVAILABLE_STATUS = "Available";

  private final UUID id;
  private final UUID holdingId;
  private final UUID instanceId;
  private final String title;
  private final String barcode;
  private final String status;
  private final JsonObject materialType;
  private final JsonObject permanentLocation;
  private final JsonObject temporaryLocation;
  private final JsonObject permanentLoanType;
  private final JsonObject temporaryLoanType;

  public ItemRequestBuilder() {
    this(UUID.randomUUID(), null, null, "Long Way to a Small Angry Planet", "645398607547",
      AVAILABLE_STATUS, bookMaterialType(), mainLibrary(), null, canCirculateLoanType(), null
    );
  }

  private ItemRequestBuilder(
    UUID id,
    UUID holdingId,
    UUID instanceId,
    String title,
    String barcode,
    String status,
    JsonObject materialType,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType) {

    this.id = id;
    this.holdingId = holdingId;
    this.title = title;
    this.barcode = barcode;
    this.status = status;
    this.permanentLocation = permanentLocation;
    this.temporaryLocation = temporaryLocation;
    this.instanceId = instanceId;
    this.permanentLoanType = permanentLoanType;
    this.temporaryLoanType = temporaryLoanType;
    this.materialType = materialType;
  }

  public JsonObject create() {
    JsonObject itemRequest = new JsonObject();

    includeWhenPresent(itemRequest, "id", id);
    includeWhenPresent(itemRequest, "title", title);
    includeWhenPresent(itemRequest, "barcode", barcode);
    includeWhenPresent(itemRequest, "holdingsRecordId", holdingId);
    includeWhenPresent(itemRequest, "instanceId", instanceId);
    includeWhenPresent(itemRequest, "barcode", barcode);

    itemRequest.put("status", new JsonObject().put("name", status));

    includeWhenPresent(itemRequest, "materialType", materialType);
    includeWhenPresent(itemRequest, "permanentLoanType", permanentLoanType);
    includeWhenPresent(itemRequest, "temporaryLoanType", temporaryLoanType);

    includeWhenPresent(itemRequest, "permanentLocation", permanentLocation);
    includeWhenPresent(itemRequest, "temporaryLocation", temporaryLocation);

    return itemRequest;
  }

  public ItemRequestBuilder withId(UUID id) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder forHolding(UUID holdingId) {
    return new ItemRequestBuilder(
      this.id,
      holdingId,
      instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withTitle(String title) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      instanceId,
      title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withNoTitle() {
    return withTitle(null);
  }

  public ItemRequestBuilder withBarcode(String barcode) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      instanceId,
      this.title,
      barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withNoBarcode() {
    return withBarcode(null);
  }

  private ItemRequestBuilder withMaterialType(JsonObject materialType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.instanceId,
      this.title,
      this.barcode,
      this.status,
      materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder book() {
    return withMaterialType(bookMaterialType());
  }

  public ItemRequestBuilder dvd() {
    return withMaterialType(dvdMaterialType());
  }

  public ItemRequestBuilder withNoMaterialType() {
    return withMaterialType(null);
  }

  private ItemRequestBuilder withPermanentLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      location,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder inMainLibrary() {
    return withPermanentLocation(mainLibrary());
  }

  public ItemRequestBuilder withNoPermanentLocation() {
    return withPermanentLocation(null);
  }

  private ItemRequestBuilder withTemporaryLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      location,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder temporarilyInAnnex() {
    return withTemporaryLocation(annex());
  }

  public ItemRequestBuilder withNoTemporaryLocation() {
    return withTemporaryLocation(null);
  }

  private ItemRequestBuilder withPermanentLoanType(JsonObject loanType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      loanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder canCirculate() {
    return withPermanentLoanType(canCirculateLoanType());
  }

  public ItemRequestBuilder courseReserves() {
    return withPermanentLoanType(courseReservesLoanType());
  }

  public ItemRequestBuilder withNoPermanentLoanType() {
    return withPermanentLoanType(null);
  }

  private ItemRequestBuilder withTemporaryLoanType(JsonObject loanType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.instanceId,
      this.title,
      this.barcode,
      this.status,
      this.materialType,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      loanType);
  }

  public ItemRequestBuilder temporarilyCourseReserves() {
    return withTemporaryLoanType(courseReservesLoanType());
  }

  public ItemRequestBuilder withNoTemporaryLoanType() {
    return withTemporaryLoanType(null);
  }

  private static JsonObject bookMaterialType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getBookMaterialType())
      .put("name", "Book");
  }

  private static JsonObject dvdMaterialType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getDvdMaterialType())
      .put("name", "DVD");
  }

  private static JsonObject mainLibrary() {
    return new JsonObject()
      .put("id", ApiTestSuite.getMainLibraryLocation())
      .put("name", "Main Library");
  }

  private static JsonObject annex() {
    return new JsonObject()
      .put("id", ApiTestSuite.getAnnexLocation())
      .put("name", "Annex Library");
  }

  private static JsonObject canCirculateLoanType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getCanCirculateLoanType())
      .put("name", "Can Circulate");
  }

  private static JsonObject courseReservesLoanType() {
    return new JsonObject()
      .put("id", ApiTestSuite.getCourseReserveLoanType())
      .put("name", "Course Reserves");
  }

  private void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    UUID value) {

    if(value != null) {
      itemRequest.put(property, value.toString());
    }
  }

  private void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    String value) {

    if(value != null) {
      itemRequest.put(property, value);
    }
  }

  private void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    JsonObject value) {

    if(value != null) {
      itemRequest.put(property, value);
    }
  }
}
