package org.folio.inventory.domain

class Item {
  final String id
  final String title
  final String barcode
  final String instanceId
  final String status
  final String materialTypeId
  final String permanentLoanTypeId
  final String temporaryLoanTypeId
  final String permanentLocation
  final String temporaryLocation

  Item(String id,
       String title,
       String barcode,
       String instanceId,
       String status,
       String materialTypeId,
       String permanentLocation,
       String temporaryLocation,
       String permanentLoanTypeId,
       String temporaryLoanTypeId) {

    this.id = id
    this.title = title
    this.barcode = barcode
    this.instanceId = instanceId
    this.status = status
    this.materialTypeId = materialTypeId
    this.permanentLocation = permanentLocation
    this.temporaryLocation = temporaryLocation
    this.permanentLoanTypeId = permanentLoanTypeId
    this.temporaryLoanTypeId = temporaryLoanTypeId
  }

  Item copyWithNewId(String newId) {
    new Item(newId, this.title, this.barcode,
      this.instanceId, this.status, this.materialTypeId, this.permanentLocation,
      this.temporaryLocation, this.permanentLoanTypeId, this.temporaryLoanTypeId)
  }

  def changeStatus(String newStatus) {
    new Item(this.id, this.title, this.barcode,
      this.instanceId, newStatus, this.materialTypeId, this.permanentLocation,
      this.temporaryLocation, this.permanentLoanTypeId, this.temporaryLoanTypeId)
  }

  @Override
  String toString() {
    println ("Item ID: ${id}, Title: ${title}, Barcode: ${barcode}")
  }
}
