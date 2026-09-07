package support.matchers;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.joda.time.Seconds;

public class TextDateTimeMatcher {

  public static Matcher<String> withinSecondsAfter(Seconds seconds, DateTime start) {
    return new TypeSafeMatcher<>() {
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

        // actual must be equal or after start
        return !actual.isBefore(start)
               && Seconds.secondsBetween(start, actual).isLessThan(seconds);
      }
    };
  }
}
