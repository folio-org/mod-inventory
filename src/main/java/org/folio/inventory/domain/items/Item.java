package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Item {

  public static final String VERSION_KEY = "_version";
  public static final String HRID_KEY = "hrid";
  public static final String TRANSIT_DESTINATION_SERVICE_POINT_ID_KEY
    = "inTransitDestinationServicePointId";
  public static final String FORMER_IDS_KEY = "formerIds";
  public static final String DISCOVERY_SUPPRESS_KEY = "discoverySuppress";
  public static final String STATUS_KEY = "status";
  public static final String ACCESSION_NUMBER_KEY = "accessionNumber";
  public static final String ITEM_IDENTIFIER_KEY = "itemIdentifier";
  public static final String YEAR_CAPTION_KEY = "yearCaption";
  public static final String ELECTRONIC_ACCESS_KEY = "electronicAccess";
  public static final String STATISTICAL_CODE_IDS_KEY = "statisticalCodeIds";
  public static final String TAGS_KEY = "tags";
  public static final String TAG_LIST_KEY = "tagList";

  public static final String ITEM_LEVEL_CALL_NUMBER_KEY = "itemLevelCallNumber";
  public static final String ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY = "itemLevelCallNumberPrefix";
  public static final String ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY = "itemLevelCallNumberSuffix";
  public static final String ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY = "itemLevelCallNumberTypeId";
  public static final String ADDITIONAL_CALL_NUMBERS_KEY = "additionalCallNumbers";
  public static final String VOLUME_KEY = "volume";

  public static final String DESCRIPTION_OF_PIECES_KEY = "descriptionOfPieces";
  public static final String NUMBER_OF_MISSING_PIECES_KEY = "numberOfMissingPieces";
  public static final String MISSING_PIECES_KEY = "missingPieces";
  public static final String MISSING_PIECES_DATE_KEY = "missingPiecesDate";
  public static final String ITEM_DAMAGED_STATUS_ID_KEY = "itemDamagedStatusId";
  public static final String ITEM_DAMAGED_STATUS_DATE_KEY = "itemDamagedStatusDate";

  public static final String ADMINISTRATIVE_NOTES_KEY = "administrativeNotes";
  public static final String NOTES_KEY = "notes";
  public static final String CIRCULATION_NOTES_KEY = "circulationNotes";
  public static final String PURCHASE_ORDER_LINE_IDENTIFIER = "purchaseOrderLineIdentifier";
  public static final String LAST_CHECK_IN = "lastCheckIn";
  public static final String COPY_NUMBER_KEY = "copyNumber";
  public static final String EFFECTIVE_SHELVING_ORDER_KEY = "effectiveShelvingOrder";
  public static final String BOUND_WITH_TITLES_KEY = "boundWithTitles";

  public final String id;
  private final String version;
  private String hrid;
  private String inTransitDestinationServicePointId;
  private Boolean discoverySuppress;
  private List<String> formerIds = new ArrayList<>();

  private String barcode;
  private String itemLevelCallNumber;
  private String itemLevelCallNumberPrefix;
  private String itemLevelCallNumberSuffix;
  private String itemLevelCallNumberTypeId;
  private List<EffectiveCallNumberComponents> additionalCallNumbers = new ArrayList<>();
  private String volume;
  private String accessionNumber;
  private String itemIdentifier;
  private List<String> yearCaption = new ArrayList<>();
  private String displaySummary;
  private String enumeration;
  private String chronology;
  private String copyNumber;
  private String numberOfPieces;
  private String holdingId;
  private String descriptionOfPieces;
  private String numberOfMissingPieces;
  private String missingPieces;
  private String missingPiecesDate;
  private String itemDamagedStatusId;
  private String itemDamagedStatusDate;
  private List<String> administrativeNotes = new ArrayList<>();
  private List<Note> notes = new ArrayList<>();
  private List<CirculationNote> circulationNotes = new ArrayList<>();
  private final Status status;
  private final String materialTypeId;
  private final String permanentLoanTypeId;
  private String effectiveShelvingOrder;

  private String temporaryLoanTypeId;
  private String permanentLocationId;
  private String temporaryLocationId;
  private String effectiveLocationId;
  private List<ElectronicAccess> electronicAccess = new ArrayList<>();
  private List<String> statisticalCodeIds = new ArrayList<>();
  private String purchaseOrderLineIdentifier;
  private List<String> tags = new ArrayList<>();
  private LastCheckIn lastCheckIn;
  private EffectiveCallNumberComponents effectiveCallNumberComponents;

  private boolean isBoundWith = false;
  private JsonArray boundWithTitles = null;

  private final JsonObject metadata;

  public Item(String id,
              String version,
              String holdingId,
              Status status,
              String materialTypeId,
              String permanentLoanTypeId,
              JsonObject metadata) {

    this.id = id;
    this.version = version;
    this.holdingId = holdingId;
    this.status = Objects.requireNonNull(status, "Status is required");
    this.materialTypeId = materialTypeId;
    this.permanentLoanTypeId = permanentLoanTypeId;
    this.metadata = metadata;
  }

  public Item(String id, String version, String holdingId, String inTransitDestinationServicePointId,
      Status status, String materialTypeId, String permanentLoanTypeId, JsonObject metadata) {

    this.id = id;
    this.version = version;
    this.holdingId = holdingId;
    this.inTransitDestinationServicePointId = inTransitDestinationServicePointId;
    this.status = Objects.requireNonNull(status, "Status is required");
    this.materialTypeId = materialTypeId;
    this.permanentLoanTypeId = permanentLoanTypeId;
    this.metadata = metadata;
  }

  public String getId() {
    return id;
  }

  public String getVersion() {
    return version;
  }

  public String getEffectiveShelvingOrder() {
    return effectiveShelvingOrder;
  }

  public Item withEffectiveShelvingOrder(String effectiveShelvingOrder) {
    this.effectiveShelvingOrder = effectiveShelvingOrder;
    return this;
  }

  public String getHrid() {
    return hrid;
  }

  public Item withHrid(String hrid) {
    this.hrid = hrid;
    return this;
  }

  public String getInTransitDestinationServicePointId() {
    return inTransitDestinationServicePointId;
  }

  public Item withInTransitDestinationServicePointId(String inTransitDestinationServicePointId) {
    this.inTransitDestinationServicePointId = inTransitDestinationServicePointId;
    return this;
  }

  public Boolean getDiscoverySuppress() {
    return discoverySuppress;
  }

  public Item withDiscoverySuppress(Boolean discoverySuppress) {
    this.discoverySuppress = discoverySuppress;
    return this;
  }

  public List<String> getFormerIds() {
    return formerIds;
  }

  public Item withFormerIds(List<String> formerIds) {
    this.formerIds = formerIds;
    return this;
  }

  public Item withBarcode(String barcode) {
    this.barcode = barcode;
    return this;
  }

  public String getBarcode() {
    return barcode;
  }

    public String getItemLevelCallNumber() {
    return itemLevelCallNumber;
  }

  public Item withItemLevelCallNumber(String itemLevelCallNumber) {
    this.itemLevelCallNumber = itemLevelCallNumber;
    return this;
  }

  public String getItemLevelCallNumberPrefix() {
    return itemLevelCallNumberPrefix;
  }

  public Item withItemLevelCallNumberPrefix(String itemLevelCallNumberPrefix) {
    this.itemLevelCallNumberPrefix = itemLevelCallNumberPrefix;
    return this;
  }

  public String getItemLevelCallNumberSuffix() {
    return itemLevelCallNumberSuffix;
  }

  public Item withItemLevelCallNumberSuffix(String itemLevelCallNumberSuffix) {
    this.itemLevelCallNumberSuffix = itemLevelCallNumberSuffix;
    return this;
  }

  public String getItemLevelCallNumberTypeId() {
    return itemLevelCallNumberTypeId;
  }

  public Item withItemLevelCallNumberTypeId(String itemLevelCallNumberTypeId) {
    this.itemLevelCallNumberTypeId = itemLevelCallNumberTypeId;
    return this;
  }

  public Item withAdditionalCallNumbers(List<EffectiveCallNumberComponents> additionalCallNumbers) {
    this.additionalCallNumbers = additionalCallNumbers;
    return this;
  }

  public List<EffectiveCallNumberComponents> getAdditionalCallNumbers() {
    return additionalCallNumbers;
  }

  public String getVolume() {
    return volume;
  }

  public Item withVolume(String volume) {
    this.volume = volume;
    return this;
  }

  public Item withDisplaySummary(String displaySummary) {
    this.displaySummary = displaySummary;
    return this;
  }

  public String getDisplaySummary() {
    return displaySummary;
  }

  public Item withEnumeration(String enumeration) {
    this.enumeration = enumeration;
    return this;
  }

  public String getEnumeration() {
    return enumeration;
  }

  public Item withChronology(String chronology) {
    this.chronology = chronology;
    return this;
  }

  public String getChronology() {
    return chronology;
  }

  public Item withCopyNumber(String copyNumber) {
    this.copyNumber = copyNumber;
    return this;
  }

  public String getCopyNumber() {
    return this.copyNumber;
  }

  public Item withNumberOfPieces(String numberOfPieces) {
    this.numberOfPieces = numberOfPieces;
    return this;
  }

  public String getNumberOfPieces() {
    return this.numberOfPieces;
  }

  public String getDescriptionOfPieces() {
    return descriptionOfPieces;
  }

  public Item withDescriptionOfPieces(String descriptionOfPieces) {
    this.descriptionOfPieces = descriptionOfPieces;
    return this;
  }

  public String getNumberOfMissingPieces() {
    return numberOfMissingPieces;
  }

  public Item withNumberOfMissingPieces(String numberOfMissingPieces) {
    this.numberOfMissingPieces = numberOfMissingPieces;
    return this;
  }

  public String getMissingPieces() {
    return missingPieces;
  }

  public Item withMissingPieces(String missingPieces) {
    this.missingPieces = missingPieces;
    return this;
  }

  public String getMissingPiecesDate() {
    return missingPiecesDate;
  }

  public Item withMissingPiecesDate(String missingPiecesDate) {
    this.missingPiecesDate = missingPiecesDate;
    return this;
  }

  public String getItemDamagedStatusId() {
    return itemDamagedStatusId;
  }

  public Item withItemDamagedStatusId(String itemDamagedStatusId) {
    this.itemDamagedStatusId = itemDamagedStatusId;
    return this;
  }

  public String getItemDamagedStatusDate() {
    return itemDamagedStatusDate;
  }

  public Item withItemDamagedStatusDate(String itemDamagedStatusDate) {
    this.itemDamagedStatusDate = itemDamagedStatusDate;
    return this;
  }

  public Item withAdministrativeNotes(List<String> notes) {
    this.administrativeNotes = notes;
    return this;
  }

  public List<String> getAdministrativeNotes() {
    return this.administrativeNotes;
  }

  public Item withNotes(List<Note> notes) {
    this.notes = notes;
    return this;
  }

  public List<Note> getNotes() {
    return this.notes;
  }

  public Item withCirculationNotes(List<CirculationNote> circulationNotes) {
    this.circulationNotes = circulationNotes;
    return this;
  }

  public List<CirculationNote> getCirculationNotes() {
    return this.circulationNotes;
  }

  public Item withPermanentLocationId(String permanentLocationId) {
    this.permanentLocationId = permanentLocationId;
    return this;
  }

  public String getPermanentLocationId() {
    return permanentLocationId;
  }

  public Item withTemporaryLocationId(String temporaryLocationId) {
    this.temporaryLocationId = temporaryLocationId;
    return this;
  }

  public String getTemporaryLocationId() {
    return temporaryLocationId;
  }

  public Item withEffectiveLocationId(String effectiveLocationId) {
    this.effectiveLocationId = effectiveLocationId;
    return this;
  }

  public String getEffectiveLocationId() {
    return effectiveLocationId;
  }

  public Item withTemporaryLoanTypeId(String temporaryLoanTypeId) {
    this.temporaryLoanTypeId = temporaryLoanTypeId;
    return this;
  }

  public String getTemporaryLoanTypeId() {
    return temporaryLoanTypeId;
  }

  public String getHoldingId() {
    return holdingId;
  }

  public Status getStatus() {
    return status;
  }

  public String getMaterialTypeId() {
    return materialTypeId;
  }

  public String getPermanentLoanTypeId() {
    return permanentLoanTypeId;
  }

  public String getAccessionNumber() {
    return accessionNumber;
  }

  public Item withAccessionNumber(String accessionNumber) {
    this.accessionNumber = accessionNumber;
    return this;
  }

  public String getItemIdentifier() {
    return itemIdentifier;
  }

  public Item withItemIdentifier(String itemIdentifier) {
    this.itemIdentifier = itemIdentifier;
    return this;
  }

  public List<String> getYearCaption() {
    return yearCaption;
  }

  public Item withYearCaption(List<String> yearCaption) {
    this.yearCaption = yearCaption;
    return this;
  }

  public List<ElectronicAccess> getElectronicAccess() {
    return electronicAccess;
  }

  public Item withElectronicAccess(List<ElectronicAccess> electronicAccess) {
    this.electronicAccess = electronicAccess;
    return this;
  }

  public List<String> getStatisticalCodeIds() {
    return statisticalCodeIds;
  }

  public Item withStatisticalCodeIds(List<String> statisticalCodeIds) {
    this.statisticalCodeIds = statisticalCodeIds;
    return this;
  }

  public String getPurchaseOrderLineIdentifier() {
    return purchaseOrderLineIdentifier;
  }

  public Item withPurchaseOrderLineIdentifier(String purchaseOrderLineidentifier) {
    this.purchaseOrderLineIdentifier = purchaseOrderLineidentifier;
    return this;
  }

  public Item withLastCheckIn(LastCheckIn lastCheckIn) {
    if (lastCheckIn != null) {
      this.lastCheckIn = lastCheckIn;
    }

    return this;
  }

  public Item withEffectiveCallNumberComponents(EffectiveCallNumberComponents components) {
    if (components != null) {
      this.effectiveCallNumberComponents = components;
    }

    return this;
  }

  public EffectiveCallNumberComponents getEffectiveCallNumberComponents() {
    return effectiveCallNumberComponents;
  }

  public Item withTags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  public Item withHoldingId(String holdingId) {
    this.holdingId = holdingId;
    return this;
  }

  public boolean getIsBoundWith() {
    return isBoundWith;
  }

  public Item withIsBoundWith(boolean boundWith) {
    this.isBoundWith = boundWith;
    return this;
  }

  public JsonArray getBoundWithTitles () {
    return boundWithTitles;
  }

  public Item withBoundWithTitles (JsonArray titles) {
    this.boundWithTitles = titles;
    return this;
  }

  public List<String> getTags() {
    return tags;
  }

  public JsonObject getMetadata() {
    return metadata;
  }

  public Item copyWithNewId(String newId) {
    return new Item(newId, null, holdingId, inTransitDestinationServicePointId, this.status,
        this.materialTypeId, this.permanentLoanTypeId, this.metadata)
        .withHrid(this.hrid)
        .withFormerIds(this.formerIds)
        .withDiscoverySuppress(this.discoverySuppress)
        .withBarcode(this.barcode)
        .withItemLevelCallNumber(this.itemLevelCallNumber)
        .withItemLevelCallNumberPrefix(this.itemLevelCallNumberPrefix)
        .withItemLevelCallNumberSuffix(this.itemLevelCallNumberSuffix)
        .withItemLevelCallNumberTypeId(this.itemLevelCallNumberTypeId)
        .withAdditionalCallNumbers(this.additionalCallNumbers)
        .withVolume(this.volume)
        .withDisplaySummary(this.displaySummary)
        .withEnumeration(this.enumeration)
        .withChronology(this.chronology)
        .withCopyNumber(this.copyNumber)
        .withNumberOfPieces(this.numberOfPieces)
        .withDescriptionOfPieces(this.descriptionOfPieces)
        .withNumberOfMissingPieces(this.numberOfMissingPieces)
        .withMissingPieces(this.missingPieces)
        .withMissingPiecesDate(this.missingPiecesDate)
        .withItemDamagedStatusId(this.itemDamagedStatusId)
        .withItemDamagedStatusDate(this.itemDamagedStatusDate)
        .withAdministrativeNotes(this.administrativeNotes)
        .withNotes(this.notes)
        .withPermanentLocationId(this.permanentLocationId)
        .withTemporaryLocationId(this.temporaryLocationId)
        .withEffectiveLocationId(this.effectiveLocationId)
        .withTemporaryLoanTypeId(this.temporaryLoanTypeId)
        .withAccessionNumber(this.accessionNumber)
        .withItemIdentifier(this.itemIdentifier)
        .withYearCaption(this.yearCaption)
        .withElectronicAccess(this.electronicAccess)
        .withStatisticalCodeIds(this.statisticalCodeIds)
        .withLastCheckIn(this.lastCheckIn)
        .withPurchaseOrderLineIdentifier(this.purchaseOrderLineIdentifier);
  }

  public Item changeStatus(ItemStatusName newStatus) {
    return new Item(this.id, this.version, holdingId, inTransitDestinationServicePointId,
        new Status(newStatus), this.materialTypeId, this.permanentLoanTypeId, this.metadata)
        .withHrid(this.hrid)
        .withFormerIds(this.formerIds)
        .withDiscoverySuppress(this.discoverySuppress)
        .withBarcode(this.barcode)
        .withItemLevelCallNumber(this.itemLevelCallNumber)
        .withItemLevelCallNumberPrefix(this.itemLevelCallNumberPrefix)
        .withItemLevelCallNumberSuffix(this.itemLevelCallNumberSuffix)
        .withItemLevelCallNumberTypeId(this.itemLevelCallNumberTypeId)
        .withAdditionalCallNumbers(this.additionalCallNumbers)
        .withVolume(this.volume)
        .withDisplaySummary(this.displaySummary)
        .withEnumeration(this.enumeration)
        .withChronology(this.chronology)
        .withCopyNumber(this.copyNumber)
        .withNumberOfPieces(this.numberOfPieces)
        .withDescriptionOfPieces(this.descriptionOfPieces)
        .withNumberOfMissingPieces(this.numberOfMissingPieces)
        .withMissingPieces(this.missingPieces)
        .withMissingPiecesDate(this.missingPiecesDate)
        .withItemDamagedStatusId(this.itemDamagedStatusId)
        .withItemDamagedStatusDate(this.itemDamagedStatusDate)
        .withAdministrativeNotes(this.administrativeNotes)
        .withNotes(this.notes)
        .withPermanentLocationId(this.permanentLocationId)
        .withTemporaryLocationId(this.temporaryLocationId)
        .withEffectiveLocationId(this.effectiveLocationId)
        .withTemporaryLoanTypeId(this.temporaryLoanTypeId)
        .withAccessionNumber(this.accessionNumber)
        .withItemIdentifier(this.itemIdentifier)
        .withYearCaption(this.yearCaption)
        .withElectronicAccess(this.electronicAccess)
        .withStatisticalCodeIds(this.statisticalCodeIds)
        .withPurchaseOrderLineIdentifier(purchaseOrderLineIdentifier)
        .withIsBoundWith(this.isBoundWith)
        .withTags(tags)
        .withCirculationNotes(circulationNotes)
        .withLastCheckIn(this.lastCheckIn);
  }

  @Override
  public String toString() {
    return String.format("Item ID: %s, Barcode: %s", id, barcode);
  }

  public LastCheckIn getLastCheckIn() {
    return lastCheckIn;
  }
}
