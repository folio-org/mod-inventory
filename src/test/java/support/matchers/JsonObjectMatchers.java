package support.matchers;

import static org.hamcrest.Matchers.is;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import com.jayway.jsonpath.matchers.JsonPathMatchers;

import io.vertx.core.json.JsonObject;

public final class JsonObjectMatchers {

  private JsonObjectMatchers() {}

  public static <T> Matcher<JsonObject> hasJsonPath(String path, T value) {
    return encodedJsonMatches(JsonPathMatchers.hasJsonPath(path, is(value)));
  }

  private static <T> Matcher<JsonObject> encodedJsonMatches(Matcher<T> matcher) {
    return new TypeSafeMatcher<JsonObject>() {
      @Override
      protected boolean matchesSafely(JsonObject entries) {
        return matcher.matches(entries.encode());
      }

      @Override
      public void describeTo(Description description) {
        description.appendDescriptionOf(matcher);
      }
    };
  }
}
