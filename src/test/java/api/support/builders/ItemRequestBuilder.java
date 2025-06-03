package api.support.builders;

import java.util.Collections;
import java.util.UUID;

import org.folio.inventory.domain.items.Item;

import api.ApiTestSuite;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ItemRequestBuilder extends AbstractBuilder {

  private static final String AVAILABLE_STATUS = "Available";

  private final UUID id;
  private final UUID holdingId;
  private final UUID inTransitDestinationServicePointId;
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
  private final JsonArray circulationNotes;
  private final JsonArray administrativeNotes;
  private final JsonObject tags;
  private final JsonObject lastCheckIn;
  private final String itemLevelCallNumber;
  private final String itemLevelCallNumberPrefix;
  private final String itemLevelCallNumberSuffix;
  private final String itemLevelCallNumberTypeId;
  private final String hrid;
  private final String copyNumber;

  public ItemRequestBuilder() {
    this(UUID.randomUUID(), null, null, null, null, "645398607547",
      AVAILABLE_STATUS, bookMaterialType(), null, null, null,
      canCirculateLoanType(), null, null, null, null,
      null, null, null,
      null, null, null, null
    );
  }

  private ItemRequestBuilder(
    UUID id,
    UUID holdingId,
    UUID inTransitDestinationServicePointId,
    String readOnlyTitle,
    String readOnlyCallNumber,
    String barcode,
    String status,
    JsonObject materialType,
    JsonObject readOnlyEffectiveLocation,
    JsonObject permanentLocation,
    JsonObject temporaryLocation,
    JsonObject permanentLoanType,
    JsonObject temporaryLoanType,
    JsonArray circulationNotes,
    JsonObject tags,
    JsonObject lastCheckIn,
    String itemLevelCallNumber,
    String itemLevelCallNumberPrefix,
    String itemLevelCallNumberSuffix,
    String itemLevelCallNumberTypeId,
    String hrid,
    String copyNumber,
    JsonArray administrativeNotes) {

    this.id = id;
    this.holdingId = holdingId;
    this.inTransitDestinationServicePointId = inTransitDestinationServicePointId;
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
    this.circulationNotes = circulationNotes;
    this.tags = tags;
    this.lastCheckIn = lastCheckIn;
    this.itemLevelCallNumber = itemLevelCallNumber;
    this.itemLevelCallNumberPrefix = itemLevelCallNumberPrefix;
    this.itemLevelCallNumberSuffix = itemLevelCallNumberSuffix;
    this.itemLevelCallNumberTypeId = itemLevelCallNumberTypeId;
    this.hrid = hrid;
    this.copyNumber = copyNumber;
    this.administrativeNotes = administrativeNotes;
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
    includeWhenPresent(itemRequest, "administrativeNotes", administrativeNotes);
    includeWhenPresent(itemRequest, "materialType", materialType);
    includeWhenPresent(itemRequest, "permanentLoanType", permanentLoanType);
    includeWhenPresent(itemRequest, "temporaryLoanType", temporaryLoanType);
    includeWhenPresent(itemRequest, "permanentLocation", permanentLocation);
    includeWhenPresent(itemRequest, "temporaryLocation", temporaryLocation);

    //Read only properties
    includeWhenPresent(itemRequest, "title", readOnlyTitle);
    includeWhenPresent(itemRequest, "callNumber", readOnlyCallNumber);
    includeWhenPresent(itemRequest, "effectiveLocation", readOnlyEffectiveLocation);

    includeWhenPresent(itemRequest, "circulationNotes", circulationNotes);

    includeWhenPresent(itemRequest, "tags", tags);
    includeWhenPresent(itemRequest, "itemLevelCallNumber", itemLevelCallNumber);
    includeWhenPresent(itemRequest, "itemLevelCallNumberSuffix", itemLevelCallNumberSuffix);
    includeWhenPresent(itemRequest, "itemLevelCallNumberPrefix", itemLevelCallNumberPrefix);
    includeWhenPresent(itemRequest, "itemLevelCallNumberTypeId", itemLevelCallNumberTypeId);
    includeWhenPresent(itemRequest, "hrid", hrid);
    includeWhenPresent(itemRequest, "inTransitDestinationServicePointId", inTransitDestinationServicePointId);
    includeWhenPresent(itemRequest, "lastCheckIn", lastCheckIn);
    includeWhenPresent(itemRequest, Item.COPY_NUMBER_KEY, copyNumber);

    return itemRequest;
  }

  public ItemRequestBuilder withId(UUID id) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder forHolding(UUID holdingId) {
    return new ItemRequestBuilder(this.id, holdingId, this.inTransitDestinationServicePointId,
      this.readOnlyTitle, this.readOnlyCallNumber, this.barcode, this.status, this.materialType,
      this.readOnlyEffectiveLocation, this.permanentLocation, this.temporaryLocation,
      this.permanentLoanType, this.temporaryLoanType, this.circulationNotes, this.tags,
      this.lastCheckIn, this.itemLevelCallNumber, this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix, this.itemLevelCallNumberTypeId, this.hrid, this.copyNumber, this.administrativeNotes);
  }

  public ItemRequestBuilder withInTransitDestinationServicePointId(UUID inTransitDestinationServicePointId) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withReadOnlyTitle(String title) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      title,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withReadOnlyCallNumber(String callNumber) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      callNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withBarcode(String barcode) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withNoBarcode() {
    return withBarcode(null);
  }

  private ItemRequestBuilder withMaterialType(JsonObject materialType) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
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
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      location,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withTemporaryLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      location,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

    public ItemRequestBuilder withPermanentLocation(JsonObject location) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      location,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
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
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      loanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
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
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      loanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withTagList(JsonObject tags) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withLastCheckIn(JsonObject lastCheckIn) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder temporarilyCourseReserves() {
    return withTemporaryLoanType(courseReservesLoanType());
  }

  public ItemRequestBuilder withNoTemporaryLoanType() {
    return withTemporaryLoanType(null);
  }

  public ItemRequestBuilder withCheckInNote() {
    JsonObject checkInNote = new JsonObject()
      .put("noteType", "Check in")
      .put("note", "Please read this note before checking in the item")
      .put("staffOnly", false);

    JsonArray circulationNotes = new JsonArray(Collections.singletonList(checkInNote));

    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
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

  public ItemRequestBuilder withItemLevelCallNumber(String itemLevelCallNumber) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withItemLevelCallNumberSuffix(String itemLevelCallNumberSuffix) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withItemLevelCallNumberPrefix(String itemLevelCallNumberPrefix) {
    return new ItemRequestBuilder(
      id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withItemLevelCallNumberTypeId(String itemLevelCallNumberTypeId) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withHrid(String hrid) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withCopyNumber(String copyNumber) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withStatus(String status) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      this.administrativeNotes);
  }

  public ItemRequestBuilder withAdministrativeNotes(JsonArray administrativeNotes) {
    return new ItemRequestBuilder(
      this.id,
      this.holdingId,
      this.inTransitDestinationServicePointId,
      this.readOnlyTitle,
      this.readOnlyCallNumber,
      this.barcode,
      this.status,
      this.materialType,
      this.readOnlyEffectiveLocation,
      this.permanentLocation,
      this.temporaryLocation,
      this.permanentLoanType,
      this.temporaryLoanType,
      this.circulationNotes,
      this.tags,
      this.lastCheckIn,
      this.itemLevelCallNumber,
      this.itemLevelCallNumberPrefix,
      this.itemLevelCallNumberSuffix,
      this.itemLevelCallNumberTypeId,
      this.hrid,
      this.copyNumber,
      administrativeNotes);
  }
}
