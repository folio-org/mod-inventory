package api.items;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static support.matchers.ItemMatchers.isMissing;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import java.net.MalformedURLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.dto.Request;
import io.vertx.core.json.JsonObject;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class MarkItemMissingApiTests extends ApiTests {
  private IndividualResource holdingsRecord;

  @Before
  public void createInstanceAndHoldingsRecord() throws Exception {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  public void testMarkItemMissingPreservesCirculationNotes() throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withCheckInNote()
      .canCirculate());

    final var itemMissing = markItemMissing(createdItem).getJson();
    final var itemCirculationNotes = itemMissing.getJsonArray(CIRCULATION_NOTES_KEY);
    final var checkInNote = itemCirculationNotes.getJsonObject(0);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
  }

  @Test
  public void canMarkItemMissingWhenInAllowedStatus() throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Withdrawn")
      .canCirculate());

    assertThat(markItemMissing(createdItem).getJson(), isMissing());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());
  }

  @Test
  public void cannotMarkItemMissingWhenNotInAllowedStatus() throws Exception {
    final String initialStatus = "On order";
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemMissing(createdItem), hasValidationError(
      "Item is not allowed to be marked as Missing", "status.name", initialStatus));
  }

  @Test
  public void shouldNotMarkItemMissingThatCannotBeFound() {
    assertThat(markItemFixture.markMissing(UUID.randomUUID()).getStatusCode(),
      is(404));
  }

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldChangeRequestBeingFulfilledBackToNotYetFilled(String requestStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    assertThat(markItemMissing(createdItem).getJson(), isMissing());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      isOpenNotYetFilled());
  }

  @Test
  @Parameters({
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  public void shouldNotReopenExpiredRequests(String requestStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(requestStatus.replace("Open - ", ""))
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).minusHours(1));

    assertThat(markItemMissing(createdItem).getJson(), isMissing());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  @Test
  @Parameters({
    "Closed - Cancelled",
    "Closed - Filled",
    "Closed - Pickup expired",
    "Closed - Unfilled"
  })
  public void shouldNotReopenClosedRequests(String requestStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Awaiting pickup")
      .canCirculate());

    final IndividualResource request = createRequest(createdItem.getId(),
      requestStatus, DateTime.now(DateTimeZone.UTC).plusHours(1));

    assertThat(markItemMissing(createdItem).getJson(), isMissing());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  private Response markItemMissing(IndividualResource item) {
    return markItemFixture.markMissing(item.getId());
  }

  private IndividualResource createRequest(UUID itemId, String status, DateTime expireDateTime)
    throws InterruptedException, ExecutionException, TimeoutException, MalformedURLException {

    return requestStorageClient.create(JsonObject.mapFrom(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expireDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build()));
  }
}
