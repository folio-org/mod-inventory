package org.folio.inventory.domain.items;

import java.util.ArrayList;
import java.util.List;

import org.folio.inventory.domain.sharedproperties.ElectronicAccess;

import io.vertx.core.json.JsonObject;

public class Item {

  public static final String HRID_KEY = "hrid";
  public static final String FORMER_IDS_KEY = "formerIds";
  public static final String DISCOVERY_SUPPRESS_KEY = "discoverySuppress";

  public static final String ACCESSION_NUMBER_KEY = "accessionNumber";
  public static final String ITEM_IDENTIFIER_KEY = "itemIdentifier";
  public static final String YEAR_CAPTION_KEY = "yearCaption";
  public static final String ELECTRONIC_ACCESS_KEY = "electronicAccess";
  public static final String STATISTICAL_CODE_IDS_KEY = "statisticalCodeIds";

  public static final String ITEM_LEVEL_CALL_NUMBER_KEY = "itemLevelCallNumber";
  public static final String ITEM_LEVEL_CALL_NUMBER_PREFIX_KEY = "itemLevelCallNumberPrefix";
  public static final String ITEM_LEVEL_CALL_NUMBER_SUFFIX_KEY = "itemLevelCallNumberSuffix";
  public static final String ITEM_LEVEL_CALL_NUMBER_TYPE_ID_KEY = "itemLevelCallNumberTypeId";
  public static final String VOLUME_KEY = "volume";

  public static final String DESCRIPTION_OF_PIECES_KEY = "descriptionOfPieces";
  public static final String NUMBER_OF_MISSING_PIECES_KEY = "numberOfMissingPieces";
  public static final String MISSING_PIECES_KEY = "missingPieces";
  public static final String MISSING_PIECES_DATE_KEY = "missingPiecesDate";
  public static final String ITEM_DAMAGED_STATUS_ID_KEY = "itemDamagedStatusId";
  public static final String ITEM_DAMAGED_STATUS_DATE_KEY = "itemDamagedStatusDate";

  public static final String NOTES_KEY = "notes";
  public static final String CIRCULATION_NOTES_KEY = "circulationNotes";

  public final String id;
  private String hrid;
  private Boolean discoverySuppress;
  private List<String> formerIds = new ArrayList();

  private String barcode;
  private String itemLevelCallNumber;
  private String itemLevelCallNumberPrefix;
  private String itemLevelCallNumberSuffix;
  private String itemLevelCallNumberTypeId;
  private String volume;
  private String accessionNumber;
  private String itemIdentifier;
  private List<String> yearCaption = new ArrayList();
  private String enumeration;
  private String chronology;
  private List<String> copyNumbers = new ArrayList();
  private String numberOfPieces;
  private final String holdingId;
  private String descriptionOfPieces;
  private String numberOfMissingPieces;
  private String missingPieces;
  private String missingPiecesDate;
  private String itemDamagedStatusId;
  private String itemDamagedStatusDate;
  private List<Note> notes = new ArrayList();
  private List<CirculationNote> circulationNotes = new ArrayList();
  private final String status;
  private final String materialTypeId;
  private final String permanentLoanTypeId;

  private String temporaryLoanTypeId;
  private String permanentLocationId;
  private String temporaryLocationId;
  private List<ElectronicAccess> electronicAccess = new ArrayList();
  private List<String> statisticalCodeIds = new ArrayList();
  private final JsonObject metadata;

  public Item(String id,
              String holdingId,
              String status,
              String materialTypeId,
              String permanentLoanTypeId,
              JsonObject metadata) {

    this.id = id;
    this.holdingId = holdingId;
    this.status = status;
    this.materialTypeId = materialTypeId;
    this.permanentLoanTypeId = permanentLoanTypeId;
    this.metadata = metadata;
  }

  public String getHrid() {
    return hrid;
  }

  public Item setHrid(String hrid) {
    this.hrid = hrid;
    return this;
  }

  public Boolean getDiscoverySuppress() {
    return discoverySuppress;
  }

  public Item setDiscoverySuppress(Boolean discoverySuppress) {
    this.discoverySuppress = discoverySuppress;
    return this;
  }

  public List<String> getFormerIds() {
    return formerIds;
  }

  public Item setFormerIds(List<String> formerIds) {
    this.formerIds = formerIds;
    return this;
  }

  public Item setBarcode(String barcode) {
    this.barcode = barcode;
    return this;
  }

  public String getBarcode() {
    return barcode;
  }

    public String getItemLevelCallNumber() {
    return itemLevelCallNumber;
  }

  public Item setItemLevelCallNumber(String itemLevelCallNumber) {
    this.itemLevelCallNumber = itemLevelCallNumber;
    return this;
  }

  public String getItemLevelCallNumberPrefix() {
    return itemLevelCallNumberPrefix;
  }

  public Item setItemLevelCallNumberPrefix(String itemLevelCallNumberPrefix) {
    this.itemLevelCallNumberPrefix = itemLevelCallNumberPrefix;
    return this;
  }

  public String getItemLevelCallNumberSuffix() {
    return itemLevelCallNumberSuffix;
  }

  public Item setItemLevelCallNumberSuffix(String itemLevelCallNumberSuffix) {
    this.itemLevelCallNumberSuffix = itemLevelCallNumberSuffix;
    return this;
  }

  public String getItemLevelCallNumberTypeId() {
    return itemLevelCallNumberTypeId;
  }

  public Item setItemLevelCallNumberTypeId(String itemLevelCallNumberTypeId) {
    this.itemLevelCallNumberTypeId = itemLevelCallNumberTypeId;
    return this;
  }

  public String getVolume() {
    return volume;
  }

  public Item setVolume(String volume) {
    this.volume = volume;
    return this;
  }

  public Item setEnumeration(String enumeration) {
    this.enumeration = enumeration;
    return this;
  }

  public String getEnumeration() {
    return enumeration;
  }

  public Item setChronology(String chronology) {
    this.chronology = chronology;
    return this;
  }

  public String getChronology() {
    return chronology;
  }

  public Item setCopyNumbers(List<String> copyNumbers) {
    this.copyNumbers = copyNumbers;
    return this;
  }

  public List<String> getCopyNumbers() {
    return this.copyNumbers;
  }

  public Item setNumberOfPieces(String numberOfPieces) {
    this.numberOfPieces = numberOfPieces;
    return this;
  }

  public String getNumberOfPieces() {
    return this.numberOfPieces;
  }

  public String getDescriptionOfPieces() {
    return descriptionOfPieces;
  }

  public Item setDescriptionOfPieces(String descriptionOfPieces) {
    this.descriptionOfPieces = descriptionOfPieces;
    return this;
  }

  public String getNumberOfMissingPieces() {
    return numberOfMissingPieces;
  }

  public Item setNumberOfMissingPieces(String numberOfMissingPieces) {
    this.numberOfMissingPieces = numberOfMissingPieces;
    return this;
  }

  public String getMissingPieces() {
    return missingPieces;
  }

  public Item setMissingPieces(String missingPieces) {
    this.missingPieces = missingPieces;
    return this;
  }

  public String getMissingPiecesDate() {
    return missingPiecesDate;
  }

  public Item setMissingPiecesDate(String missingPiecesDate) {
    this.missingPiecesDate = missingPiecesDate;
    return this;
  }

  public String getItemDamagedStatusId() {
    return itemDamagedStatusId;
  }

  public Item setItemDamagedStatusId(String itemDamagedStatusId) {
    this.itemDamagedStatusId = itemDamagedStatusId;
    return this;
  }

  public String getItemDamagedStatusDate() {
    return itemDamagedStatusDate;
  }

  public Item setItemDamagedStatusDate(String itemDamagedStatusDate) {
    this.itemDamagedStatusDate = itemDamagedStatusDate;
    return this;
  }

  public Item setNotes(List<Note> notes) {
    this.notes = notes;
    return this;
  };

  public List<Note> getNotes() {
    return this.notes;
  }

  public Item setCirculationNotes(List<CirculationNote> circulationNotes) {
    this.circulationNotes = circulationNotes;
    return this;
  };

  public List<CirculationNote> getCirculationNotes() {
    return this.circulationNotes;
  }

  public Item setPermanentLocationId(String permanentLocationId) {
    this.permanentLocationId = permanentLocationId;
    return this;
  }

  public String getPermanentLocationId() {
    return permanentLocationId;
  }

  public Item setTemporaryLocationId(String temporaryLocationId) {
    this.temporaryLocationId = temporaryLocationId;
    return this;
  }

  public String getTemporaryLocationId() {
    return temporaryLocationId;
  }

  public Item setTemporaryLoanTypeId(String temporaryLoanTypeId) {
    this.temporaryLoanTypeId = temporaryLoanTypeId;
    return this;
  }

  public String getTemporaryLoanTypeId() {
    return temporaryLoanTypeId;
  }

  public String getHoldingId() {
    return holdingId;
  }

  public String getStatus() {
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

  public Item setAccessionNumber(String accessionNumber) {
    this.accessionNumber = accessionNumber;
    return this;
  }

  public String getItemIdentifier() {
    return itemIdentifier;
  }

  public Item setItemIdentifier(String itemIdentifier) {
    this.itemIdentifier = itemIdentifier;
    return this;
  }

  public List<String> getYearCaption() {
    return yearCaption;
  }

  public Item setYearCaption(List<String> yearCaption) {
    this.yearCaption = yearCaption;
    return this;
  }

  public List<ElectronicAccess> getElectronicAccess() {
    return electronicAccess;
  }

  public Item setElectronicAccess(List<ElectronicAccess> electronicAccess) {
    this.electronicAccess = electronicAccess;
    return this;
  }

  public List<String> getStatisticalCodeIds() {
    return statisticalCodeIds;
  }

  public Item setStatisticalCodeIds(List<String> statisticalCodeIds) {
    this.statisticalCodeIds = statisticalCodeIds;
    return this;
  }

  public JsonObject getMetadata() {
    return metadata;
  }

  public Item copyWithNewId(String newId) {
    return new Item(newId,
      holdingId, this.status, this.materialTypeId,
      this.permanentLoanTypeId, this.metadata)
            .setHrid(this.hrid)
            .setFormerIds(this.formerIds)
            .setDiscoverySuppress(this.discoverySuppress)
            .setBarcode(this.barcode)
            .setItemLevelCallNumber(this.itemLevelCallNumber)
            .setItemLevelCallNumberPrefix(this.itemLevelCallNumberPrefix)
            .setItemLevelCallNumberSuffix(this.itemLevelCallNumberSuffix)
            .setItemLevelCallNumberTypeId(this.itemLevelCallNumberTypeId)
            .setVolume(this.volume)
            .setEnumeration(this.enumeration)
            .setChronology(this.chronology)
            .setCopyNumbers(this.copyNumbers)
            .setNumberOfPieces(this.numberOfPieces)
            .setDescriptionOfPieces(this.descriptionOfPieces)
            .setNumberOfMissingPieces(this.numberOfMissingPieces)
            .setMissingPieces(this.missingPieces)
            .setMissingPiecesDate(this.missingPiecesDate)
            .setItemDamagedStatusId(this.itemDamagedStatusId)
            .setItemDamagedStatusDate(this.itemDamagedStatusDate)
            .setNotes(this.notes)
            .setPermanentLocationId(this.permanentLocationId)
            .setTemporaryLocationId(this.temporaryLocationId)
            .setTemporaryLoanTypeId(this.temporaryLoanTypeId)
            .setAccessionNumber(this.accessionNumber)
            .setItemIdentifier(this.itemIdentifier)
            .setYearCaption(this.yearCaption)
            .setElectronicAccess(this.electronicAccess)
            .setStatisticalCodeIds(this.statisticalCodeIds);
  }

  public Item changeStatus(String newStatus) {
    return new Item(this.id,
      holdingId, newStatus, this.materialTypeId,
      this.permanentLoanTypeId, this.metadata)
            .setHrid(this.hrid)
            .setFormerIds(this.formerIds)
            .setDiscoverySuppress(this.discoverySuppress)
            .setBarcode(this.barcode)
            .setItemLevelCallNumber(this.itemLevelCallNumber)
            .setItemLevelCallNumberPrefix(this.itemLevelCallNumberPrefix)
            .setItemLevelCallNumberSuffix(this.itemLevelCallNumberSuffix)
            .setItemLevelCallNumberTypeId(this.itemLevelCallNumberTypeId)
            .setVolume(this.volume)
            .setEnumeration(this.enumeration)
            .setChronology(this.chronology)
            .setCopyNumbers(this.copyNumbers)
            .setNumberOfPieces(this.numberOfPieces)
            .setDescriptionOfPieces(this.descriptionOfPieces)
            .setNumberOfMissingPieces(this.numberOfMissingPieces)
            .setMissingPieces(this.missingPieces)
            .setMissingPiecesDate(this.missingPiecesDate)
            .setItemDamagedStatusId(this.itemDamagedStatusId)
            .setItemDamagedStatusDate(this.itemDamagedStatusDate)
            .setNotes(this.notes)
            .setPermanentLocationId(this.permanentLocationId)
            .setTemporaryLocationId(this.temporaryLocationId)
            .setTemporaryLoanTypeId(this.temporaryLoanTypeId)
            .setAccessionNumber(this.accessionNumber)
            .setItemIdentifier(this.itemIdentifier)
            .setYearCaption(this.yearCaption)
            .setElectronicAccess(this.electronicAccess)
            .setStatisticalCodeIds(this.statisticalCodeIds);
  }

  @Override
  public String toString() {
    return String.format("Item ID: %s, Barcode: %s", id, barcode);
  }
}
