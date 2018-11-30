package org.folio.inventory.domain.items;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonObject;

public class Item {

  public static final String NOTES_KEY = "notes";

  public final String id;
  private String barcode;
  private String enumeration;
  private String chronology;
  private List<String> copyNumbers = new ArrayList();
  private String numberOfPieces;
  private String holdingId;
  private List<Note> notes = new ArrayList();
  private String status;
  private String materialTypeId;
  private String permanentLoanTypeId;

  private String temporaryLoanTypeId;
  private String permanentLocationId;
  private String temporaryLocationId;
  private JsonObject metadata;

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

  public Item setBarcode(String barcode) {
    this.barcode = barcode;
    return this;
  };

  public String getBarcode() {
    return barcode;
  };

  public Item setEnumeration(String enumeration) {
    this.enumeration = enumeration;
    return this;
  };

  public String getEnumeration() {
    return enumeration;
  };

  public Item setChronology(String chronology) {
    this.chronology = chronology;
    return this;
  };

  public String getChronology() {
    return chronology;
  };

  public Item setCopyNumbers(List<String> copyNumbers) {
    this.copyNumbers = copyNumbers;
    return this;
  };

  public List<String> getCopyNumbers() {
    return this.copyNumbers;
  };

  public Item setNumberOfPieces(String numberOfPieces) {
    this.numberOfPieces = numberOfPieces;
    return this;
  };

  public String getNumberOfPieces() {
    return this.numberOfPieces;
  };

  public Item setNotes(List<Note> notes) {
    this.notes = notes;
    return this;
  };

  public List<Note> getNotes() {
    return this.notes;
  };

  public Item setPermanentLocationId(String permanentLocationId) {
    this.permanentLocationId = permanentLocationId;
    return this;
  };

  public String getPermanentLocationId() {
    return permanentLocationId;
  };

  public Item setTemporaryLocationId(String temporaryLocationId) {
    this.temporaryLocationId = temporaryLocationId;
    return this;
  };

  public String getTemporaryLocationId() {
    return temporaryLocationId;
  };

  public Item setTemporaryLoanTypeId(String temporaryLoanTypeId) {
    this.temporaryLoanTypeId = temporaryLoanTypeId;
    return this;
  };

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

  public JsonObject getMetadata() {
    return metadata;
  }

  public Item copyWithNewId(String newId) {
    return new Item(newId,
      holdingId, this.status, this.materialTypeId,
      this.permanentLoanTypeId, this.metadata)
            .setBarcode(this.barcode)
            .setEnumeration(this.enumeration)
            .setChronology(this.chronology)
            .setCopyNumbers(this.copyNumbers)
            .setNumberOfPieces(this.numberOfPieces)
            .setNotes(this.notes)
            .setPermanentLocationId(this.permanentLocationId)
            .setTemporaryLocationId(this.temporaryLocationId)
            .setTemporaryLoanTypeId(this.temporaryLoanTypeId);
  }

  public Item changeStatus(String newStatus) {
    return new Item(this.id,
      holdingId, newStatus, this.materialTypeId,
      this.permanentLoanTypeId, this.metadata)
            .setBarcode(this.barcode)
            .setEnumeration(this.enumeration)
            .setChronology(this.chronology)
            .setCopyNumbers(this.copyNumbers)
            .setNumberOfPieces(this.numberOfPieces)
            .setNotes(this.notes)
            .setPermanentLocationId(this.permanentLocationId)
            .setTemporaryLocationId(this.temporaryLocationId)
            .setTemporaryLoanTypeId(this.temporaryLoanTypeId);
  }

  @Override
  public String toString() {
    return String.format("Item ID: %s, Barcode: %s", id, barcode);
  }
}
