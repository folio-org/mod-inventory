package org.folio.inventory.storage.memory

import org.folio.inventory.common.api.request.PagingParameters
import org.folio.inventory.common.domain.Failure
import org.folio.inventory.common.domain.Success

import org.folio.inventory.domain.ingest.IngestJobCollection
import org.folio.inventory.resources.ingest.IngestJob

import java.util.function.Consumer

class InMemoryIngestJobCollection implements IngestJobCollection {
  private final List<IngestJob> items = new ArrayList<IngestJob>()

  @Override
  void empty(Consumer<Success> completionCallback,
             Consumer<Failure> failureCallback) {

    items.clear()
    completionCallback.accept(new Success())
  }

  @Override
  void add(IngestJob item,
           Consumer<Success<IngestJob>> resultCallback,
           Consumer<Failure> failureCallback) {

    items.add(item)
    resultCallback.accept(new Success<IngestJob>(item))
  }

  @Override
  void findById(String id,
                Consumer<Success<IngestJob>> resultCallback,
                Consumer<Failure> failureCallback) {

    resultCallback.accept(new Success(items.find({ it.id == id })))
  }

  @Override
  void findAll(PagingParameters pagingParameters,
               Consumer<Success<Map>> resultCallback,
               Consumer<Failure> failureCallback) {

    def totalRecords = items.size()

    def paged = items.stream()
      .skip(pagingParameters.offset)
      .limit(pagingParameters.limit)
      .collect()

    resultCallback.accept(new Success(
      wrapFindResult("jobs", paged, totalRecords)))
  }

  @Override
  void update(IngestJob ingestJob,
             Consumer<Success> completionCallback,
             Consumer<Failure> failureCallback) {

    items.removeIf({ it.id == ingestJob.id })
    items.add(ingestJob)
    completionCallback.accept(new Success(null))
  }

  @Override
  void delete(String id,
              Consumer<Success> completionCallback,
              Consumer<Failure> failureCallback) {

    items.removeIf({ it.id == id })
    completionCallback.accept(new Success())
  }

  private Map wrapFindResult(
    String collectionName,
    Collection pagedRecords,
    int totalRecords) {

    [
      (collectionName): pagedRecords,
      "totalRecords"  : totalRecords
    ]
  }
}
