package org.folio.inventory.storage.external;

import static org.folio.inventory.storage.external.ReferenceRecordClientTest.CauseMatcher.causeMatches;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static support.FutureAssistance.waitForCompletion;

import java.net.URI;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import support.ControlledVocabularyPreparation;

class ReferenceRecordClientTest extends AbstractExternalStorageTest {

  private ReferenceRecordClient referenceClient;
  private ControlledVocabularyPreparation preparation;

  @BeforeEach
  @SneakyThrows
  void before() {
    final var okapiHttpClient = createOkapiHttpClient();

    URL materialTypesUrl = new URI(
      String.format("%s/%s", getStorageAddress(), "/material-types")).toURL();

    CollectionResourceClient collectionResourceClient = new CollectionResourceClient(
      okapiHttpClient, materialTypesUrl);

    referenceClient = new ReferenceRecordClient(collectionResourceClient, "mtypes");

    CompletableFuture<Void> allDeleted = new CompletableFuture<>();

    collectionResourceClient.delete(response -> {
      if (response.statusCode() == 204) {
        allDeleted.complete(null);
      } else {
        allDeleted.completeExceptionally(new Exception(response.body()));
      }
    });

    waitForCompletion(allDeleted);

    preparation = new ControlledVocabularyPreparation(okapiHttpClient,
      materialTypesUrl, "mtypes");
  }

  @Test
  @SneakyThrows
  void canGetSingleReferenceRecord() {
    String bookId = preparation.createOrReferenceTerm("Book");

    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    waitForCompletion(recordFuture);

    ReferenceRecord referenceRecord = recordFuture.join();

    assertThat(referenceRecord, is(notNullValue()));
    assertThat(referenceRecord.id(), is(bookId));
    assertThat(referenceRecord.name(), is("Book"));
  }

  @Test
  void cannotGetReferenceRecordWhichDoesNotExist() {
    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    var ex = assertThrows(ExecutionException.class, () -> waitForCompletion(recordFuture));
    assertTrue(causeMatches(
      ReferenceRecordClient.ReferenceRecordClientException.class,
      "Failed to get reference record: Book").matches(ex.getCause()));
  }

  @Test
  @SneakyThrows
  void doesntGetReferenceRecordBySubstring() {
    preparation.createOrReferenceTerm("Tactile Book");
    preparation.createOrReferenceTerm("Book in Electronic Form");

    CompletableFuture<ReferenceRecord> recordFuture
      = referenceClient.getRecord("Book");

    var ex = assertThrows(Exception.class, recordFuture::join);
    assertTrue(ex.getMessage().contains("Failed to get reference record: Book"));
  }

  static class CauseMatcher extends TypeSafeMatcher<Throwable> {

    private final Class<? extends Throwable> type;
    private final String expectedMessage;

    CauseMatcher(Class<? extends Throwable> type, String expectedMessage) {
      this.type = type;
      this.expectedMessage = expectedMessage;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("expects type ")
        .appendValue(type)
        .appendText(" and a message ")
        .appendValue(expectedMessage);
    }

    @Override
    protected boolean matchesSafely(Throwable item) {
      return item.getClass().isAssignableFrom(type)
             && item.getMessage().contains(expectedMessage);
    }

    static CauseMatcher causeMatches(Class<? extends Throwable> type, String expectedMessage) {
      return new CauseMatcher(type, expectedMessage);
    }
  }
}
