package api.items;

import static org.hamcrest.MatcherAssert.assertThat;
import static support.matchers.ItemMatchers.isMissing;
import static support.matchers.ItemMatchers.isWithdrawn;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.inventory.support.http.client.IndividualResource;
import org.folio.inventory.support.http.client.Response;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import support.builders.ItemRequestBuilder;

@SuppressWarnings("java:S5786")
public class MarkItemWithdrawnApiTest extends AbstractMarkItemApiTest {

  @Test
  void canMarkWithdrawnItemAsMissing() {
    final IndividualResource createdItem = itemsClient.create(new ItemRequestBuilder()
      .forHolding(holdingsRecord.getId())
      .withStatus("Available")
      .canCirculate());

    markItem(createdItem);
    markItemFixture.markMissing(createdItem.getId());

    assertThat(itemsClient.getById(createdItem.getId()).getJson(), isMissing());
  }

  @Override
  protected Response markItem(IndividualResource item) {
    return markItemFixture.markWithdrawn(item.getId());
  }

  @Override
  protected Response markItem(UUID id) {
    return markItemFixture.markWithdrawn(id);
  }

  @Override
  protected Matcher<JsonObject> targetStatusMatcher() {
    return isWithdrawn();
  }

  @Override
  protected String targetStatusName() {
    return "Withdrawn";
  }

  @Override
  protected String allowedSourceStatus() {
    return "Paged";
  }

  @Override
  protected String disallowedSourceStatus() {
    return "Checked out";
  }
}
