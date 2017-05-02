package org.folio.inventory.common.cql

class CqlFilter {
  Closure filterBy(field, searchTerm) {
    return {
      if (searchTerm == null) {
        true
      } else {
        it."${field}".contains(searchTerm)
      }
    }
  }
}
