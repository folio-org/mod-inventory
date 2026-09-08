package support.matchers;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.folio.inventory.support.http.client.Response;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class ResponseMatchers {

  public static Matcher<Response> hasValidationError(String expectedMessage, String expectedKey, String expectedValue) {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description
          .appendText("Response has 422 status and 'message' - ").appendValue(expectedMessage)
          .appendText(", 'key' - ").appendValue(expectedKey)
          .appendText(" and 'value' - ").appendValue(expectedValue);
      }

      @Override
      protected boolean matchesSafely(Response response) {
        if (response.statusCode() != 422) {
          return false;
        }

        if (!isJsonContent(response)) {
          return false;
        }

        try {
          JsonArray errors = response.getJson().getJsonArray("errors");
          if (errors != null && errors.size() == 1) {
            JsonObject error = errors.getJsonObject(0);
            JsonArray parameters = error.getJsonArray("parameters");

            if (parameters != null && parameters.size() == 1) {
              String message = error.getString("message");
              String key = parameters.getJsonObject(0).getString("key");
              String value = parameters.getJsonObject(0).getString("value");

              return Objects.equals(expectedMessage, message)
                     && Objects.equals(expectedKey, key)
                     && Objects.equals(expectedValue, value);
            }
          }
          return false;
        } catch (DecodeException ex) {
          return false;
        }
      }

      @Override
      protected void describeMismatchSafely(Response response,
                                            Description mismatchDescription) {
        mismatchDescription.appendText("Status: ")
          .appendValue(response.statusCode())
          .appendText(", body: ");

        if (isJsonContent(response)) {
          mismatchDescription.appendValue(response.getJson());
        } else {
          mismatchDescription.appendValue(response.body());
        }
      }
    };
  }

  public static Matcher<Response> hasStatusAndJsonBody(int statusCode) {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description
          .appendText("Response has status - ").appendValue(statusCode)
          .appendText(" and a JSON body");
      }

      @Override
      protected boolean matchesSafely(Response response) {
        return response.statusCode() == statusCode && isJsonContent(response);
      }

      @Override
      protected void describeMismatchSafely(Response response,
                                            Description mismatchDescription) {
        mismatchDescription.appendText("Status: ")
          .appendValue(response.statusCode())
          .appendText(", content type: ")
          .appendValue(response.contentType())
          .appendText(", body: ");

        if (isJsonContent(response)) {
          mismatchDescription.appendValue(response.getJson());
        } else {
          mismatchDescription.appendValue(response.body());
        }
      }
    };
  }

  public static Matcher<Response> hasNotUpdatedEntity(
    String expectedEntityId, String expectedErrorMessageFragment) {

    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description
          .appendText("Response has a single 'notUpdatedEntities' entry with 'entityId' - ")
          .appendValue(expectedEntityId)
          .appendText(" and 'errorMessage' containing - ")
          .appendValue(expectedErrorMessageFragment);
      }

      @Override
      protected boolean matchesSafely(Response response) {
        if (!isJsonContent(response)) {
          return false;
        }

        try {
          JsonArray notUpdatedEntities = response.getJson().getJsonArray("notUpdatedEntities");

          if (notUpdatedEntities == null || notUpdatedEntities.size() != 1) {
            return false;
          }

          JsonObject notUpdatedEntity = notUpdatedEntities.getJsonObject(0);
          String entityId = notUpdatedEntity.getString("entityId");
          String errorMessage = notUpdatedEntity.getString("errorMessage");

          return Objects.equals(expectedEntityId, entityId)
                 && errorMessage != null && errorMessage.contains(expectedErrorMessageFragment);
        } catch (DecodeException ex) {
          return false;
        }
      }

      @Override
      protected void describeMismatchSafely(Response response,
                                            Description mismatchDescription) {
        mismatchDescription.appendText("Status: ")
          .appendValue(response.statusCode())
          .appendText(", body: ");

        if (isJsonContent(response)) {
          mismatchDescription.appendValue(response.getJson());
        } else {
          mismatchDescription.appendValue(response.body());
        }
      }
    };
  }

  private static boolean isJsonContent(Response response) {
    return response.contentType().startsWith(HttpHeaderValues.APPLICATION_JSON.toString());
  }
}
