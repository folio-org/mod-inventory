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
  final String location

  Item(String id,
       String title,
       String barcode,
       String instanceId,
       String status,
       String materialTypeId,
       String location,
       String permanentLoanTypeId,
       String temporaryLoanTypeId) {

    this.id = id
    this.title = title
    this.barcode = barcode
    this.instanceId = instanceId
    this.status = status
    this.materialTypeId = materialTypeId
    this.location = location
    this.permanentLoanTypeId = permanentLoanTypeId
    this.temporaryLoanTypeId = temporaryLoanTypeId
  }

  Item copyWithNewId(String newId) {
    new Item(newId, this.title, this.barcode,
      this.instanceId, this.status, this.materialTypeId, this.location,
      this.permanentLoanTypeId, this.temporaryLoanTypeId)
  }

  def changeStatus(String newStatus) {
    new Item(this.id, this.title, this.barcode,
      this.instanceId, newStatus, this.materialTypeId, this.location,
      this.permanentLoanTypeId, this.temporaryLoanTypeId)
  }

  @Override
  String toString() {
    println ("Item ID: ${id}, Title: ${title}, Barcode: ${barcode}")
  }
}
