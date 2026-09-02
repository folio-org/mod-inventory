package api.items;

import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static support.matchers.ItemMatchers.isRestricted;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

import support.ApiTests;
import support.builders.HoldingRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.dto.Request;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class MarkItemRestrictedApiTest extends ApiTests {

  private IndividualResource holdingsRecord;

  @BeforeEach
  void createInstanceAndHoldingsRecord() {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  void testMarkItemLongMissingPreservesCirculationNotes() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withCheckInNote()
      .canCirculate());

    final var item = markItemRestricted(createdItem).getJson();
    final var itemCirculationNotes = item.getJsonArray(CIRCULATION_NOTES_KEY);
    final var checkInNote = itemCirculationNotes.getJsonObject(0);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
  }

  @Test
  void canMarkItemRestrictedWhenInAllowedStatus() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Withdrawn")
      .canCirculate());
    final Response response = markItemRestricted(createdItem);

    assertEquals(200, response.getStatusCode());
    assertThat(response.getJson(), isRestricted());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isRestricted());
  }

  @Test
  void cannotMarkItemRestrictedWhenNotInAllowedStatus() {
    final String initialStatus = "In process";
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());

    assertThat(markItemRestricted(createdItem), hasValidationError(
      "Item is not allowed to be marked as Restricted", "status.name", initialStatus));
  }

  @Test
  void shouldNotMarkItemRestrictedThatCannotBeFound() {
    assertThat(markItemFixture.markRestricted(UUID.randomUUID()).getStatusCode(),
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

    assertThat(markItemRestricted(createdItem).getJson(), isRestricted());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isRestricted());

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

    assertThat(markItemRestricted(createdItem).getJson(), isRestricted());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isRestricted());

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

    assertThat(markItemRestricted(createdItem).getJson(), isRestricted());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isRestricted());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  private Response markItemRestricted(IndividualResource item) {
    return markItemFixture.markRestricted(item.getId());
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
