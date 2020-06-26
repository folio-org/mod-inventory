package support.matchers;

import static support.matchers.JsonObjectMatchers.hasJsonPath;

import org.hamcrest.Matcher;

import io.vertx.core.json.JsonObject;

public final class RequestMatchers {

  private RequestMatchers(){}

  public static Matcher<JsonObject> isOpenNotYetFilled() {
    return hasStatus("Open - Not yet filled");
  }

  public static Matcher<JsonObject> hasStatus(String status) {
    return hasJsonPath("status", status);
  }
}
