package support.matchers;

import static support.matchers.JsonObjectMatchers.hasJsonPath;

import api.ApiTestSuite;
import io.vertx.core.json.JsonObject;
import java.util.Objects;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public final class ItemMatchers {
  private ItemMatchers() { }

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

  public static Matcher<JsonObject> isUnavailable() {
    return hasStatus("Unavailable");
  }

  public static Matcher<JsonObject> isUnknown() {
    return hasStatus("Unknown");
  }

  public static Matcher<JsonObject> isWithdrawn() {
    return hasStatus("Withdrawn");
  }

  public static Matcher<JsonObject> hasStatus(String status) {
    return hasJsonPath("status.name", status);
  }

  /**
   * Matches an item whose "materialType" is either consistent with one of the known
   * material types (book or DVD) or is entirely empty (no id and no name).
   */
  public static Matcher<JsonObject> hasConsistentMaterialType() {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("item has a materialType consistent with a known material type");
      }

      @Override
      protected boolean matchesSafely(JsonObject item) {
        JsonObject materialType = item.getJsonObject("materialType");

        String materialTypeId = materialType.getString("id");

        if (materialTypeId.equals(ApiTestSuite.getBookMaterialType())) {
          return Objects.equals(materialType.getString("id"), ApiTestSuite.getBookMaterialType())
                 && Objects.equals(materialType.getString("name"), "Book");
        } else if (materialTypeId.equals(ApiTestSuite.getDvdMaterialType())) {
          return Objects.equals(materialType.getString("id"), ApiTestSuite.getDvdMaterialType())
                 && Objects.equals(materialType.getString("name"), "DVD");
        } else {
          return materialType.getString("id") == null && materialType.getString("name") == null;
        }
      }

      @Override
      protected void describeMismatchSafely(JsonObject item, Description mismatchDescription) {
        mismatchDescription.appendText("materialType was ")
          .appendValue(item.getJsonObject("materialType"));
      }
    };
  }

  public static Matcher<JsonObject> hasConsistentPermanentLoanType() {
    return hasConsistentLoanTypeAt("permanentLoanType");
  }

  public static Matcher<JsonObject> hasConsistentTemporaryLoanType() {
    return hasConsistentLoanTypeAt("temporaryLoanType");
  }

  /**
   * Matches a loan type object (e.g. the value of an item's "permanentLoanType" or
   * "temporaryLoanType" property) that is either consistent with a known loan type, or
   * {@code null}.
   */
  public static Matcher<JsonObject> hasConsistentLoanType() {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("loan type is consistent with a known loan type or is null");
      }

      @Override
      protected boolean matchesSafely(JsonObject loanType) {
        return isConsistentLoanType(loanType);
      }

      @Override
      protected void describeMismatchSafely(JsonObject loanType, Description mismatchDescription) {
        mismatchDescription.appendText("loan type was ").appendValue(loanType);
      }
    };
  }

  public static Matcher<JsonObject> hasConsistentPermanentLocation() {
    return hasConsistentLocationAt("permanentLocation");
  }

  public static Matcher<JsonObject> hasConsistentTemporaryLocation() {
    return hasConsistentLocationAt("temporaryLocation");
  }

  /**
   * Matches a location object (e.g. the value of an item's "permanentLocation" or
   * "temporaryLocation" property) that is either consistent with a known location, or
   * {@code null}.
   */
  public static Matcher<JsonObject> hasConsistentLocation() {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("location is consistent with a known location or is null");
      }

      @Override
      protected boolean matchesSafely(JsonObject location) {
        return isConsistentLocation(location);
      }

      @Override
      protected void describeMismatchSafely(JsonObject location, Description mismatchDescription) {
        mismatchDescription.appendText("location was ").appendValue(location);
      }
    };
  }

  /**
   * Matches an item whose "effectiveCallNumberComponents" match the given expected values.
   */
  public static Matcher<JsonObject> hasCallNumbers(
    String callNumber, String suffix, String prefix, String typeId) {

    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description
          .appendText("item has effectiveCallNumberComponents with callNumber - ")
          .appendValue(callNumber)
          .appendText(", suffix - ").appendValue(suffix)
          .appendText(", prefix - ").appendValue(prefix)
          .appendText(" and typeId - ").appendValue(typeId);
      }

      @Override
      protected boolean matchesSafely(JsonObject item) {
        JsonObject callNumberComponents = item.getJsonObject("effectiveCallNumberComponents");

        if (callNumberComponents == null) {
          return false;
        }

        return Objects.equals(callNumberComponents.getString("callNumber"), callNumber)
               && Objects.equals(callNumberComponents.getString("suffix"), suffix)
               && Objects.equals(callNumberComponents.getString("prefix"), prefix)
               && Objects.equals(callNumberComponents.getString("typeId"), typeId);
      }

      @Override
      protected void describeMismatchSafely(JsonObject item, Description mismatchDescription) {
        mismatchDescription.appendText("effectiveCallNumberComponents was ")
          .appendValue(item.getJsonObject("effectiveCallNumberComponents"));
      }
    };
  }

  private static Matcher<JsonObject> hasConsistentLoanTypeAt(String propertyName) {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(propertyName)
          .appendText(" is consistent with a known loan type or is null");
      }

      @Override
      protected boolean matchesSafely(JsonObject item) {
        return isConsistentLoanType(item.getJsonObject(propertyName));
      }

      @Override
      protected void describeMismatchSafely(JsonObject item, Description mismatchDescription) {
        mismatchDescription.appendText(propertyName).appendText(" was ")
          .appendValue(item.getJsonObject(propertyName));
      }
    };
  }

  private static Matcher<JsonObject> hasConsistentLocationAt(String propertyName) {
    return new TypeSafeMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(propertyName)
          .appendText(" is consistent with a known location or is null");
      }

      @Override
      protected boolean matchesSafely(JsonObject item) {
        return isConsistentLocation(item.getJsonObject(propertyName));
      }

      @Override
      protected void describeMismatchSafely(JsonObject item, Description mismatchDescription) {
        mismatchDescription.appendText(propertyName).appendText(" was ")
          .appendValue(item.getJsonObject(propertyName));
      }
    };
  }

  private static boolean isConsistentLoanType(JsonObject loanType) {
    if (loanType == null) {
      return true;
    }

    String loanTypeId = loanType.getString("id");

    if (loanTypeId.equals(ApiTestSuite.getCanCirculateLoanType())) {
      return Objects.equals(loanType.getString("id"), ApiTestSuite.getCanCirculateLoanType())
             && Objects.equals(loanType.getString("name"), "Can Circulate");
    } else if (loanTypeId.equals(ApiTestSuite.getCourseReserveLoanType())) {
      return Objects.equals(loanType.getString("id"), ApiTestSuite.getCourseReserveLoanType())
             && Objects.equals(loanType.getString("name"), "Course Reserves");
    } else {
      return loanType.getString("id") == null && loanType.getString("name") == null;
    }
  }

  private static boolean isConsistentLocation(JsonObject location) {
    if (location == null) {
      return true;
    }

    String locationId = location.getString("id");

    if (locationId.equals(ApiTestSuite.getThirdFloorLocation())) {
      return Objects.equals(location.getString("id"), ApiTestSuite.getThirdFloorLocation())
             && Objects.equals(location.getString("name"), "3rd Floor");
    } else if (locationId.equals(ApiTestSuite.getMezzanineDisplayCaseLocation())) {
      return Objects.equals(location.getString("id"), ApiTestSuite.getMezzanineDisplayCaseLocation())
             && Objects.equals(location.getString("name"), "Display Case, Mezzanine");
    } else {
      return location.getString("id") == null && location.getString("name") == null;
    }
  }
}
