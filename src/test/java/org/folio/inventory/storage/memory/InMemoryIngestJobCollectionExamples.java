package org.folio.inventory.storage.memory;

import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.resources.ingest.IngestJob;
import org.folio.inventory.resources.ingest.IngestJobState;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.common.FutureAssistance.complete;
import static org.folio.inventory.common.FutureAssistance.fail;
import static org.folio.inventory.common.FutureAssistance.getOnCompletion;
import static org.folio.inventory.common.FutureAssistance.succeed;
import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class InMemoryIngestJobCollectionExamples {

  private final InMemoryIngestJobCollection collection = new InMemoryIngestJobCollection();

  @Before
  public void before()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Success<Void>> emptied = new CompletableFuture<>();

    collection.empty(complete(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  public void jobsCanBeAdded()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestJob> addFuture = new CompletableFuture<>();

    collection.add(new IngestJob(IngestJobState.REQUESTED),
      succeed(addFuture), fail(addFuture));

    waitForCompletion(addFuture);

    CompletableFuture<MultipleRecords<IngestJob>> findFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findFuture),
      fail(findFuture));

    MultipleRecords<IngestJob> allJobsWrapped = getOnCompletion(findFuture);

    List<IngestJob> allJobs = allJobsWrapped.records;

    assertThat(allJobs.size(), is(1));

    allJobs.stream().forEach(job -> assertThat(job.id, is(notNullValue())));
    allJobs.stream().forEach(job -> assertThat(job.state, is(IngestJobState.REQUESTED)));
  }

  @Test
  public void jobsCanBeFoundById()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestJob> addFuture = new CompletableFuture<>();

    collection.add(new IngestJob(IngestJobState.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestJob added = getOnCompletion(addFuture);

    CompletableFuture<IngestJob> findFuture = new CompletableFuture<>();

    collection.findById(added.id, succeed(findFuture), fail(findFuture));

    IngestJob found = getOnCompletion(findFuture);

    assertThat(found.id, is(added.id));
    assertThat(found.state, is(IngestJobState.REQUESTED));
  }

  @Test
  public void jobStateCanBeUpdated()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestJob> addFuture = new CompletableFuture<>();

    collection.add(new IngestJob(IngestJobState.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestJob added = getOnCompletion(addFuture);

    IngestJob completed = added.complete();

    CompletableFuture<Void> updateFuture = new CompletableFuture<>();

    collection.update(completed, succeed(updateFuture),
      fail(updateFuture));

    waitForCompletion(updateFuture);

    CompletableFuture<IngestJob> findFuture = new CompletableFuture<>();

    collection.findById(added.id, succeed(findFuture), fail(findFuture));

    IngestJob found = getOnCompletion(findFuture);

    assertThat(found.state, is(IngestJobState.COMPLETED));
  }

  @Test
  public void singleJobWithSameIdFollowingUpdate()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<IngestJob> addFuture = new CompletableFuture<>();

    collection.add(new IngestJob(IngestJobState.REQUESTED),
      succeed(addFuture), fail(addFuture));

    IngestJob added = getOnCompletion(addFuture);

    IngestJob completed = added.complete();

    CompletableFuture<Void> updateFuture = new CompletableFuture<>();

    collection.update(completed, succeed(updateFuture), fail(updateFuture));

    waitForCompletion(updateFuture);

    CompletableFuture<MultipleRecords<IngestJob>> findAllFuture = new CompletableFuture<>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture),
      fail(findAllFuture));

    MultipleRecords<IngestJob> allJobsWrapped = getOnCompletion(findAllFuture);

    List<IngestJob> allJobs = allJobsWrapped.records;

    assertThat(allJobs.size(), is(1));
    assertThat(allJobs.stream().findFirst().get().id, is(added.id));
  }
}
