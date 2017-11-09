package org.folio.inventory.storage.memory;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.ingest.IngestJobCollection;
import org.folio.inventory.resources.ingest.IngestJob;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class InMemoryIngestJobCollection implements IngestJobCollection {
  private final List<IngestJob> items = new ArrayList<>();

  @Override
  public void empty(
    Consumer<Success> completionCallback,
    Consumer<Failure> failureCallback) {

    items.clear();
    completionCallback.accept(new Success<>(null));
  }

  @Override
  public void add(
    IngestJob item,
    Consumer<Success<IngestJob>> resultCallback,
    Consumer<Failure> failureCallback) {

    if(item.id == null) {
      item = item.copyWithNewId(UUID.randomUUID().toString());
    }

    items.add(item);
    resultCallback.accept(new Success<>(item));
  }

  @Override
  public void findById(
    final String id,
    Consumer<Success<IngestJob>> resultCallback,
    Consumer<Failure> failureCallback) {

    Optional<IngestJob> foundJob = items.stream()
      .filter(it -> it.id.equals(id))
      .findFirst();

    if(foundJob.isPresent()) {
      resultCallback.accept(new Success<>(foundJob.get()));
    }
    else {
      resultCallback.accept(new Success<>(null));
    }
  }

  @Override
  public void findAll(
    PagingParameters pagingParameters,
    Consumer<Success<Map>> resultCallback,
    Consumer<Failure> failureCallback) {

    int totalRecords = items.size();

    Collection paged = items.stream()
      .skip(pagingParameters.offset)
      .limit(pagingParameters.limit)
      .collect(Collectors.toList());

    resultCallback.accept(new Success<>(wrapFindResult(paged, totalRecords)));
  }

  @Override
  public void update(
    final IngestJob ingestJob,
    Consumer<Success> completionCallback,
    Consumer<Failure> failureCallback) {

    items.removeIf(it -> it.id.equals(ingestJob.id));
    items.add(ingestJob);

    completionCallback.accept(new Success<>(null));
  }

  @Override
  public void delete(
    final String id,
    Consumer<Success> completionCallback,
    Consumer<Failure> failureCallback) {

    items.removeIf(it -> it.id.equals(id));
    completionCallback.accept(new Success<>(null));

  }

  private Map wrapFindResult(
    Collection pagedRecords,
    int totalRecords) {

    Map<String, Object> map = new HashMap<>(2);
    map.put("jobs", pagedRecords);
    map.put("totalRecords", totalRecords);
    return map;
  }

}
