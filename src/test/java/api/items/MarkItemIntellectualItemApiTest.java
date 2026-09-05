package api.items;

import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static support.fixtures.InstanceFixture.smallAngryPlanet;
import static support.matchers.ItemMatchers.isIntellectualItem;
import static support.matchers.RequestMatchers.hasStatus;
import static support.matchers.RequestMatchers.isOpenNotYetFilled;
import static support.matchers.ResponseMatchers.hasValidationError;

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
import support.ApiTests;
import support.builders.HoldingRequestBuilder;
import support.builders.ItemRequestBuilder;
import support.dto.Request;

public class MarkItemIntellectualItemApiTest extends ApiTests {

  private IndividualResource holdingsRecord;

  @BeforeEach
  void createInstanceAndHoldingsRecord() {
    final IndividualResource instance = instancesClient
      .create(smallAngryPlanet(UUID.randomUUID()));

    holdingsRecord = holdingsStorageClient.create(new HoldingRequestBuilder()
      .forInstance(instance.getId()));
  }

  @Test
  void testMarkItemIntellectualItemPreservesCirculationNotes() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withCheckInNote()
      .canCirculate());

    final var item = markItemIntellectualItem(createdItem).getJson();
    final var itemCirculationNotes = item.getJsonArray(CIRCULATION_NOTES_KEY);
    final var checkInNote = itemCirculationNotes.getJsonObject(0);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
  }

  @Test
  void canMarkItemIntellectualItemWhenInAllowedStatus() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Awaiting delivery")
      .canCirculate());
    final Response response = markItemIntellectualItem(createdItem);

    assertEquals(200, response.statusCode());
    assertThat(response.getJson(), isIntellectualItem());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isIntellectualItem());
  }

  @Test
  void cannotMarkItemIntellectualItemWhenNotInAllowedStatus() {
    final String initialStatus = "Intellectual item";
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus(initialStatus)
      .canCirculate());
    markItemIntellectualItem(createdItem);
    assertThat(markItemIntellectualItem(createdItem), hasValidationError(
      "Item is not allowed to be marked as Intellectual item", "status.name", initialStatus));
  }

  @Test
  void shouldNotMarkItemIntellectualItemThatCannotBeFound() {
    assertThat(markItemFixture.markIntellectualItem(UUID.randomUUID()).statusCode(),
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

    assertThat(markItemIntellectualItem(createdItem).getJson(), isIntellectualItem());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isIntellectualItem());

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

    assertThat(markItemIntellectualItem(createdItem).getJson(), isIntellectualItem());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isIntellectualItem());

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

    assertThat(markItemIntellectualItem(createdItem).getJson(), isIntellectualItem());
    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isIntellectualItem());

    assertThat(requestStorageClient.getById(request.getId()).getJson(),
      hasStatus(requestStatus));
  }

  private Response markItemIntellectualItem(IndividualResource item) {
    return markItemFixture.markIntellectualItem(item.getId());
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
