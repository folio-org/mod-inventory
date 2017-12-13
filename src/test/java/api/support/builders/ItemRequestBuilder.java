package api.support.builders;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public class ItemRequestBuilder implements Builder {

  private static final String AVAILABLE_STATUS = "Available";

  private final UUID id;
  private final UUID holdingId;
  private final String readOnlyTitle;
  private final String barcode;
  private final String status;
  private final JsonObject materialType;
  private final JsonObject readOnlyPermanentLocation;
  private final JsonObject temporaryLocation;
  private final JsonObject permanentLoanType;
  private final JsonObject temporaryLoanType;

  public ItemRequestBuilder() {
    this(UUID.randomUUID(), null, null, "645398607547",
      AVAILABLE_STATUS, bookMaterialType(), null, null,
      canCirculateLoanType(), null
    );
  }

  private ItemRequestBuilder(
    UUID id,
    UUID holdingId,
    String readOnlyTitle,
    String barcode,
    String status,
    JsonObject materialType,
    JsonObject readOnlyPermanentLocation,
    JsonObject temporaryLocation,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType) {

    this.id = id;
    this.holdingId = holdingId;
    this.readOnlyTitle = readOnlyTitle;
    this.barcode = barcode;
    this.status = status;
    this.readOnlyPermanentLocation = readOnlyPermanentLocation;
    this.temporaryLocation = temporaryLocation;
    this.permanentLoanType = permanentLoanType;
    this.temporaryLoanType = temporaryLoanType;
    this.materialType = materialType;
  }

  public JsonObject create() {
    JsonObject itemRequest = new JsonObject();

    includeWhenPresent(itemRequest, "id", id);
    includeWhenPresent(itemRequest, "barcode", barcode);
    includeWhenPresent(itemRequest, "holdingsRecordId", holdingId);
    includeWhenPresent(itemRequest, "barcode", barcode);

    itemRequest.put("status", new JsonObject().put("name", status));

    includeWhenPresent(itemRequest, "materialType", materialType);
    includeWhenPresent(itemRequest, "permanentLoanType", permanentLoanType);
    includeWhenPresent(itemRequest, "temporaryLoanType", temporaryLoanType);
    includeWhenPresent(itemRequest, "temporaryLocation", temporaryLocation);

    //Read only properties
    includeWhenPresent(itemRequest, "title", readOnlyTitle);
    includeWhenPresent(itemRequest, "permanentLocation", readOnlyPermanentLocation);

    return itemRequest;
  }

  public ItemRequestBuilder withId(UUID id) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder forHolding(UUID holdingId) {
    return new ItemRequestBuilder(
      this.id,
      holdingId,
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withReadOnlyTitle(String title) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      title,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withBarcode(String barcode) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
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
      this.readOnlyTitle,
      this.barcode,
      this.status,
      materialType,
      this.readOnlyPermanentLocation,
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

  public ItemRequestBuilder withReadOnlyPermanentLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      location,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  private ItemRequestBuilder withTemporaryLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
      location,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder temporarilyInMainLibrary() {
    return withTemporaryLocation(mainLibrary());
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
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
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
      this.readOnlyTitle,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyPermanentLocation,
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

  public static JsonObject mainLibrary() {
    return new JsonObject()
      .put("id", ApiTestSuite.getMainLibraryLocation())
      .put("name", "Main Library");
  }

  public static JsonObject annex() {
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
