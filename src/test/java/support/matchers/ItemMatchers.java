package support.matchers;

import static support.matchers.JsonObjectMatchers.hasJsonPath;

import org.hamcrest.Matcher;

import io.vertx.core.json.JsonObject;

public final class ItemMatchers {
  private ItemMatchers() {}

  public static Matcher<JsonObject> isAvailable() {
    return hasStatus("Available");
  }

  public static Matcher<JsonObject> isInProcess() {
    return hasStatus("In process");
  }

  public static Matcher<JsonObject> isInProcessNonRequestable() {
    return hasStatus("In process (non-requestable)");
  }

  public static Matcher<JsonObject> isIntellectualItem() {
    return hasStatus("Intellectual item");
  }

  public static Matcher<JsonObject> isLongMissing() {
    return hasStatus("Long missing");
  }

  public static Matcher<JsonObject> isMissing() {
    return hasStatus("Missing");
  }

  public static Matcher<JsonObject> isRestricted() {
    return hasStatus("Restricted");
  }
  public static Matcher<JsonObject> isUnknown() {
    return hasStatus("Unknown");
  }


  public static Matcher<JsonObject> isWithdrawn() {
    return hasStatus("Withdrawn");
  }

  private static Matcher<JsonObject> hasStatus(String status) {
    return hasJsonPath("status.name", status);
  }
}
