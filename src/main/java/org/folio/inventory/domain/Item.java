package org.folio.inventory.domain;

import java.util.ArrayList;
import java.util.List;

public class Item {
  public final String id;
  public final String barcode;
  public final String enumeration;
  public final String chronology;
  public final List<String> pieceIdentifiers;
  public final String numberOfPieces;
  public final String holdingId;
  public final List<String> notes;
  public final String status;
  public final String materialTypeId;
  public final String permanentLoanTypeId;
  public final String temporaryLoanTypeId;
  public final String temporaryLocationId;

  public Item(String id,
              String barcode,
              String enumeration,
              String chronology,
              List<String> pieceIdentifiers,
              String numberOfPieces,
              String holdingId,
              List<String> notes,
              String status,
              String materialTypeId,
              String temporaryLocationId,
              String permanentLoanTypeId,
              String temporaryLoanTypeId) {

    this.id = id;
    this.barcode = barcode;
    this.enumeration = enumeration;
    this.chronology = chronology;
    this.pieceIdentifiers = new ArrayList<>(pieceIdentifiers);
    this.numberOfPieces = numberOfPieces;
    this.holdingId = holdingId;
    this.notes = new ArrayList<>(notes);
    this.status = status;
    this.materialTypeId = materialTypeId;
    this.temporaryLocationId = temporaryLocationId;
    this.permanentLoanTypeId = permanentLoanTypeId;
    this.temporaryLoanTypeId = temporaryLoanTypeId;
  }

  public Item copyWithNewId(String newId) {
    return new Item(newId, this.barcode, this.enumeration,
      this.chronology, this.pieceIdentifiers, this.numberOfPieces,
      holdingId, this.notes, this.status, this.materialTypeId,
      this.temporaryLocationId, this.permanentLoanTypeId,
      this.temporaryLoanTypeId);
  }

  public Item changeStatus(String newStatus) {
    return new Item(this.id, this.barcode, this.enumeration,
      this.chronology, this.pieceIdentifiers, this.numberOfPieces,
      holdingId, this.notes, newStatus, this.materialTypeId,
      this.temporaryLocationId, this.permanentLoanTypeId,
      this.temporaryLoanTypeId);
  }

  @Override
  public String toString() {
    return String.format("Item ID: %s, Barcode: %s", id, barcode);
  }
}
