package support.matchers;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

public class TextDateTimeMatcher {

  public static Matcher<String> withinSecondsAfter(Seconds seconds, DateTime start) {
    return new TypeSafeMatcher<String>() {
      @Override
      public void describeTo(Description description) {
        description.appendText(String.format(
          "a date time within %s seconds after %s",
          seconds.getSeconds(), start.toString()));
      }

      @Override
      protected boolean matchesSafely(String textRepresentation) {
        //response representation might vary from request representation
        DateTime actual = DateTime.parse(textRepresentation);

        return ! actual.isBefore(start) &&   // actual must be equal or after start
          Seconds.secondsBetween(start, actual).isLessThan(seconds);
      }
    };
  }
}
