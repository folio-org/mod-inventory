package support.matchers;

import static support.matchers.JsonObjectMatchers.hasJsonPath;

import org.hamcrest.Matcher;

import io.vertx.core.json.JsonObject;

public final class ItemMatchers {
  private ItemMatchers() {}

  public static Matcher<JsonObject> isMissing() {
    return hasStatus("Missing");
  }

  public static Matcher<JsonObject> isWithdrawn() {
    return hasStatus("Withdrawn");
  }

  private static Matcher<JsonObject> hasStatus(String status) {
    return hasJsonPath("status.name", status);
  }
}
