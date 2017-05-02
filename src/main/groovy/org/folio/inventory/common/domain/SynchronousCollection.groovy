package org.folio.inventory.common.domain

interface SynchronousCollection<T> {
  void empty()

  T add(T item)

  List<T> findAll()

  T findById(String id)
}
