package org.folio.inventory.domain;

public class Item {
  public final String id;
  public final String title;
  public final String barcode;
  public final String instanceId;
  public final String status;
  public final String materialTypeId;
  public final String permanentLoanTypeId;
  public final String temporaryLoanTypeId;
  public final String permanentLocationId;
  public final String temporaryLocationId;

  public Item(String id,
       String title,
       String barcode,
       String instanceId,
       String status,
       String materialTypeId,
       String permanentLocationId,
       String temporaryLocationId,
       String permanentLoanTypeId,
       String temporaryLoanTypeId) {

    this.id = id;
    this.title = title;
    this.barcode = barcode;
    this.instanceId = instanceId;
    this.status = status;
    this.materialTypeId = materialTypeId;
    this.permanentLocationId = permanentLocationId;
    this.temporaryLocationId = temporaryLocationId;
    this.permanentLoanTypeId = permanentLoanTypeId;
    this.temporaryLoanTypeId = temporaryLoanTypeId;
  }

  public Item copyWithNewId(String newId) {
    return new Item(newId, this.title, this.barcode,
      this.instanceId, this.status, this.materialTypeId, this.permanentLocationId,
      this.temporaryLocationId, this.permanentLoanTypeId, this.temporaryLoanTypeId);
  }

  public Item changeStatus(String newStatus) {
    return new Item(this.id, this.title, this.barcode,
      this.instanceId, newStatus, this.materialTypeId, this.permanentLocationId,
      this.temporaryLocationId, this.permanentLoanTypeId, this.temporaryLoanTypeId);
  }

  @Override
  public String toString() {
    return String.format("Item ID: %s, Title: %s, Barcode: %s", id, title, barcode);
  }
}
