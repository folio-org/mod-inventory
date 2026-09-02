package api.items;

import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static support.matchers.ItemMatchers.isInProcessNonRequestable;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import support.ApiTests;
import support.builders.HoldingRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.dto.Request;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.UUID;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MarkItemInProcessNonRequestableApiTest extends ApiTests {

  private IndividualResource holdingsRecord;

  @BeforeEach
  void createInstanceAndHoldingsRecord() {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  void testMarkItemInProcessNonRequestableCirculationNotes() {
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

  @Test
  void canMarkItemInProcessNonRequestableWhenInAllowedStatus() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Available")
      .canCirculate());
    final Response response = markItemInProcessNonRequestable(createdItem);

    assertEquals(200, response.getStatusCode());
    assertThat(response.getJson(), isInProcessNonRequestable());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isInProcessNonRequestable());
  }

  @Test
  void cannotMarkItemInProcessWhenNotInAllowedStatus() {
    final String initialStatus = "In process (non-requestable)";
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .withBarcode("" + new Date().getTime())
      .canCirculate());

    assertThat(markItemInProcessNonRequestable(createdItem), hasValidationError(
      "Item is not allowed to be marked as In process (non-requestable)", "status.name", initialStatus));
  }

  @Test
  void shouldNotMarkItemInProcessThatCannotBeFound() {
    assertThat(markItemFixture.markInProcess(UUID.randomUUID()).getStatusCode(),
      is(404));
  }

  @ParameterizedTest
  @ValueSource(strings = {
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  void shouldChangeRequestBeingFulfilledBackToNotYetFilled(String requestStatus) {
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

  @ParameterizedTest
  @ValueSource(strings = {
    "Open - Awaiting delivery",
    "Open - Awaiting pickup",
    "Open - In transit"
  })
  void shouldNotReopenExpiredRequests(String requestStatus) {
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

  @ParameterizedTest
  @ValueSource(strings = {
    "Closed - Cancelled",
    "Closed - Filled",
    "Closed - Pickup expired",
    "Closed - Unfilled"
  })
  void shouldNotReopenClosedRequests(String requestStatus) {
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
    return markItemFixture.markInProcessNonRequestable(item.getId());
  }

  private IndividualResource createRequest(UUID itemId, String status, DateTime expireDateTime) {
    return requestStorageClient.create(JsonObject.mapFrom(Request.builder()
      .status(status)
      .itemId(itemId.toString())
      .holdShelfExpirationDate(expireDateTime.toDate())
      .requesterId(UUID.randomUUID().toString())
      .requestType("Hold")
      .build()));
  }
}
