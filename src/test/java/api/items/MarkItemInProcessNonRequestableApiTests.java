package api.items;

import api.support.ApiTests;
import api.support.builders.HoldingRequestBuilder;
import api.support.builders.ItemRequestBuilder;
import api.support.dto.Request;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static api.support.InstanceSamples.smallAngryPlanet;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static support.matchers.ItemMatchers.isInProcessNonRequestable;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

@RunWith(JUnitParamsRunner.class)
public class MarkItemInProcessNonRequestableApiTests extends ApiTests {
  private IndividualResource holdingsRecord;

  @Before
  public void createInstanceAndHoldingsRecord() throws Exception {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));

  }

  @Test
  public void testMarkItemInProcessNonRequestableCirculationNotes() throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withCheckInNote()
      .canCirculate());

    final var itemMarkedAsTargetStatus = markItemInProcessNonRequestable(createdItem).getJson();
    final var itemCirculationNotes = itemMarkedAsTargetStatus.getJsonArray(CIRCULATION_NOTES_KEY);
    final var checkInNote = itemCirculationNotes.getJsonObject(0);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
    assertFalse(false);
  }
  @Parameters({
    "Available"
    ,"Awaiting delivery"
    ,"In transit"
    ,"Lost and paid"
    ,"Missing"
    ,"Order closed"
    ,"Paged"
    ,"Withdrawn"
  })
  @Test
  public void canMarkItemInProcessNonRequestableWhenInAllowedStatus(String initialStatus) throws Exception {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemInProcessNonRequestable(createdItem).getJson(), isInProcessNonRequestable());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isInProcessNonRequestable());
  }

  @Parameters({
    "Aged to lost"
    ,"Checked out"
    ,"Claimed returned"
    ,"Declared lost"
    ,"In process"
    ,"In process (non-requestable)"
    ,"Intellectual item"
    ,"Long missing"
    ,"On order"
    ,"Restricted"
    ,"Unavailable"
    ,"Unknown"
  })

  @Test()
  public void cannotMarkItemInProcessWhenNotInAllowedStatus(String initialStatus) throws Exception {
        final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
          .forHolding(holdingsRecord.getId())
          .withStatus(initialStatus)
          .withBarcode(""+new Date().getTime())
          .canCirculate());

        assertThat(markItemInProcessNonRequestable(createdItem), hasValidationError(
          "Item is not allowed to be marked as:\"In process (non-requestable)\"", "status.name", initialStatus));
  }


  @Test
  public void shouldNotMarkItemInProcessThatCannotBeFound() {
    assertThat(markInProcessFixture.markInProcess(UUID.randomUUID()).getStatusCode(),
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

    assertThat(markItemInProcessNonRequestable(createdItem).getJson(), isInProcessNonRequestable());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isInProcessNonRequestable());

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

    assertThat(markItemInProcessNonRequestable(createdItem).getJson(), isInProcessNonRequestable());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isInProcessNonRequestable());

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

    assertThat(markItemInProcessNonRequestable(createdItem).getJson(), isInProcessNonRequestable());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isInProcessNonRequestable());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  private Response markItemInProcessNonRequestable(IndividualResource item) {
    return markInProcessNonRequestableFixture.markInProcessNonRequestable(item.getId());
  }

  private IndividualResource createRequest(UUID itemId, String status, DateTime expireDateTime)
    throws InterruptedException, ExecutionException, TimeoutException, MalformedURLException {
    return requestStorageClient.create(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expireDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build());
  }
}
