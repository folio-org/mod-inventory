package org.folio.inventory.common.domain

class Failure {
  final String reason
  final Integer statusCode

  def Failure(String reason, Integer statusCode) {
    this.reason = reason
    this.statusCode = statusCode
  }
}
