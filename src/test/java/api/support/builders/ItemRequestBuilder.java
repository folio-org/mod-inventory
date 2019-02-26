package api.support.builders;

import java.util.UUID;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;

public class ItemRequestBuilder implements Builder {

  private static final String AVAILABLE_STATUS = "Available";

  private final UUID id;
  private final UUID holdingId;
  private final String readOnlyTitle;
  private final String readOnlyCallNumber;
  private final String barcode;
  private final String status;
  private final JsonObject materialType;
  private final JsonObject readOnlyEffectiveLocation;
  private final JsonObject permanentLocation;
  private final JsonObject temporaryLocation;
  private final JsonObject permanentLoanType;
  private final JsonObject temporaryLoanType;

  public ItemRequestBuilder() {
    this(UUID.randomUUID(), null, null, null, "645398607547",
      AVAILABLE_STATUS, bookMaterialType(), null, null, null,
      canCirculateLoanType(), null
    );
  }

  private ItemRequestBuilder(
    UUID id,
    UUID holdingId,
    String readOnlyTitle,
    String readOnlyCallNumber,
    String barcode,
    String status,
    JsonObject materialType,
    JsonObject readOnlyEffectiveLocation,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType) {

    this.id = id;
    this.holdingId = holdingId;
    this.readOnlyTitle = readOnlyTitle;
    this.readOnlyCallNumber = readOnlyCallNumber;
    this.barcode = barcode;
    this.status = status;
    this.readOnlyEffectiveLocation = readOnlyEffectiveLocation;
    this.permanentLocation = permanentLocation;
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

    if(status != null) {
      itemRequest.put("status", new JsonObject().put("name", status));
    } else {
      itemRequest.put("status", new JsonObject());
    }

    includeWhenPresent(itemRequest, "materialType", materialType);
    includeWhenPresent(itemRequest, "permanentLoanType", permanentLoanType);
    includeWhenPresent(itemRequest, "temporaryLoanType", temporaryLoanType);
    includeWhenPresent(itemRequest, "permanentLocation", permanentLocation);
    includeWhenPresent(itemRequest, "temporaryLocation", temporaryLocation);

    //Read only properties
    includeWhenPresent(itemRequest, "title", readOnlyTitle);
    includeWhenPresent(itemRequest, "callNumber", readOnlyCallNumber);
    includeWhenPresent(itemRequest, "effectiveLocation", readOnlyEffectiveLocation);

    return itemRequest;
  }

  public ItemRequestBuilder withId(UUID id) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder forHolding(UUID holdingId) {
    return new ItemRequestBuilder(
      this.id,
      holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withReadOnlyTitle(String title) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      title,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withReadOnlyCallNumber(String callNumber) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      callNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withBarcode(String barcode) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder withNoBarcode() {
    return withBarcode(null);
  }

  public ItemRequestBuilder withNoStatus() {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      null,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  private ItemRequestBuilder withMaterialType(JsonObject materialType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      materialType,
      this.readOnlyEffectiveLocation,
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

  public ItemRequestBuilder withReadOnlyEffectiveLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      location,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  private ItemRequestBuilder withTemporaryLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      location,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

    private ItemRequestBuilder withPermanentLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      location,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType);
  }

  public ItemRequestBuilder permanentlyInThirdFloor() {
    return withPermanentLocation(thirdFloor());
  }

  public ItemRequestBuilder temporarilyInReadingRoom() {
    return withTemporaryLocation(readingRoom());
  }

  public ItemRequestBuilder withNoPermanentLocation() {
    return withPermanentLocation(null);
  }

  public ItemRequestBuilder withNoTemporaryLocation() {
    return withTemporaryLocation(null);
  }

  private ItemRequestBuilder withPermanentLoanType(JsonObject loanType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
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
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
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

  private static JsonObject thirdFloor() {
    return new JsonObject()
      .put("id", ApiTestSuite.getThirdFloorLocation())
      .put("name", "3rd Floor");
  }

  public static JsonObject readingRoom() {
    return new JsonObject()
      .put("id", ApiTestSuite.getReadingRoomLocation())
      .put("name", "Reading Room");
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
