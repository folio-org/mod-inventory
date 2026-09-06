package api.items;

import static org.folio.inventory.domain.items.CirculationNote.NOTE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.NOTE_TYPE_KEY;
import static org.folio.inventory.domain.items.CirculationNote.STAFF_ONLY_KEY;
import static org.folio.inventory.domain.items.Item.CIRCULATION_NOTES_KEY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static support.matchers.ItemMatchers.isLongMissing;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import support.builders.ItemRequestBuilder;

public class MarkItemLongMissingApiTest extends AbstractMarkItemApiTest {

  @Test
  void testMarkItemLongMissingPreservesCirculationNotes() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withCheckInNote()
      .canCirculate());

    final var item = markItem(createdItem).getJson();
    final var itemCirculationNotes = item.getJsonArray(CIRCULATION_NOTES_KEY);
    final var checkInNote = itemCirculationNotes.getJsonObject(0);

    assertThat(checkInNote.getString(NOTE_TYPE_KEY), is("Check in"));
    assertThat(checkInNote.getString(NOTE_KEY), is("Please read this note before checking in the item"));
    assertThat(checkInNote.getBoolean(STAFF_ONLY_KEY), is(false));
  }

  @Override
  protected Response markItem(IndividualResource item) {
    return markItemFixture.markLongMissing(item.getId());
  }

  @Override
  protected Response markItem(UUID id) {
    return markItemFixture.markLongMissing(id);
  }

  @Override
  protected Matcher<JsonObject> targetStatusMatcher() {
    return isLongMissing();
  }

  @Override
  protected String targetStatusName() {
    return "Long missing";
  }

  @Override
  protected String allowedSourceStatus() {
    return "Awaiting pickup";
  }

  @Override
  protected String disallowedSourceStatus() {
    return "Long missing";
  }
}
