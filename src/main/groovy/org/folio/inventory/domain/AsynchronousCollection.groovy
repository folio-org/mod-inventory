package org.folio.inventory.domain

import org.folio.inventory.common.api.request.PagingParameters
import org.folio.inventory.common.domain.Failure
import org.folio.inventory.common.domain.Success

import java.util.function.Consumer

interface AsynchronousCollection<T> {
  void empty(Consumer<Success> completionCallback,
             Consumer<Failure> failureCallback)
  void add(T item,
           Consumer<Success<T>> resultCallback,
           Consumer<Failure> failureCallback)
  void findById(String id,
                Consumer<Success<T>> resultCallback,
                Consumer<Failure> failureCallback)
  void findAll(PagingParameters pagingParameters,
               Consumer<Success<List<T>>> resultsCallback,
               Consumer<Failure> failureCallback)
  void delete(String id,
              Consumer<Success> completionCallback,
              Consumer<Failure> failureCallback)
  void update(T item,
              Consumer<Success> completionCallback,
              Consumer<Failure> failureCallback)
}
