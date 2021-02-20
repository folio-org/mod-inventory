package org.folio.inventory.storage.external;

import api.support.ControlledVocabularyPreparation;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.folio.inventory.storage.external.ReferenceRecordClientExamples.CauseMatcher.causeMatches;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ReferenceRecordClientExamples {

  private ReferenceRecordClient referenceClient;
  private ControlledVocabularyPreparation preparation;

  @Before
  public void before()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    MalformedURLException {

    OkapiHttpClient okapiHttpClient = ExternalStorageSuite.createOkapiHttpClient();

    URL materialTypesUrl = new URL(
      String.format("%s/%s", ExternalStorageSuite.getStorageAddress(),
        "/material-types"));

    CollectionResourceClient collectionResourceClient = new CollectionResourceClient(
      okapiHttpClient, materialTypesUrl);

    referenceClient = new ReferenceRecordClient(collectionResourceClient, "mtypes");

    CompletableFuture<Void> allDeleted = new CompletableFuture<>();

    collectionResourceClient.delete(response -> {
      if(response.getStatusCode() == 204) {
        allDeleted.complete(null);
      }
      else {
        allDeleted.completeExceptionally(new Exception(response.getBody()));
      }
    });

    waitForCompletion(allDeleted);

    preparation = new ControlledVocabularyPreparation(okapiHttpClient,
      materialTypesUrl, "mtypes");
  }

  @Test
  public void canGetSingleReferenceRecord()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    String bookId = preparation.createOrReferenceTerm("Book");

    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    waitForCompletion(recordFuture);

    ReferenceRecord record = recordFuture.join();

    assertThat(record, is(notNullValue()));
    assertThat(record.id, is(bookId));
    assertThat(record.name, is("Book"));
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void cannotGetReferenceRecordWhichDoesNotExist()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    thrown.expect(ExecutionException.class);
    thrown.expectCause(causeMatches(
      ReferenceRecordClient.ReferenceRecordClientException.class,
      "Failed to get reference record: Book"));

    waitForCompletion(recordFuture);
  }

  @Test
  public void doesntGetReferenceRecordBySubstring()
    throws InterruptedException,
    ExecutionException,
    TimeoutException {

    preparation.createOrReferenceTerm("Tactile Book");
    preparation.createOrReferenceTerm("Book in Electronic Form");

    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    thrown.expectMessage("Failed to get reference record: Book");

    ReferenceRecord record = recordFuture.join();
    fail("Got unexpected record: " + record);
  }

  static class CauseMatcher extends TypeSafeMatcher<Throwable> {

    static CauseMatcher causeMatches(Class<? extends Throwable> type, String expectedMessage) {
      return new CauseMatcher(type, expectedMessage);
    }

    private final Class<? extends Throwable> type;
    private final String expectedMessage;

    CauseMatcher(Class<? extends Throwable> type, String expectedMessage) {
      this.type = type;
      this.expectedMessage = expectedMessage;
    }

    @Override
    protected boolean matchesSafely(Throwable item) {
      return item.getClass().isAssignableFrom(type)
        && item.getMessage().contains(expectedMessage);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("expects type ")
        .appendValue(type)
        .appendText(" and a message ")
        .appendValue(expectedMessage);
    }
  }
}
