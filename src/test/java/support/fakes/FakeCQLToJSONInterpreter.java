package support.fakes;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FakeCQLToJSONInterpreter {
  // " or ) at the left and a-z at right
  private static final String OR_REGEX = "(?<=[\")]) or (?=[a-z])";
  private static final Pattern PATTERN = Pattern.compile(OR_REGEX);
  private final boolean diagnosticsEnabled;

  public FakeCQLToJSONInterpreter(boolean diagnosticsEnabled) {
    this.diagnosticsEnabled = diagnosticsEnabled;
  }

  public List<JsonObject> execute(Collection<JsonObject> records, String query) {
    ImmutablePair<String, String> queryAndSort = splitQueryAndSort(query);

    if(containsSort(queryAndSort)) {
      printDiagnostics(() -> String.format("Search by: %s", queryAndSort.left));
      printDiagnostics(() -> String.format("Sort by: %s", queryAndSort.right));

      return records.stream()
        .filter(filterForQuery(queryAndSort.left))
        .sorted(sortForQuery(queryAndSort.right))
        .collect(Collectors.toList());
    }
    else {
      printDiagnostics(() -> String.format("Search only by: %s", queryAndSort.left));

      return records.stream()
        .filter(filterForQuery(queryAndSort.left))
        .collect(Collectors.toList());
    }
  }

  private Comparator<? super JsonObject> sortForQuery(String sort) {
    if(StringUtils.contains(sort, "/")) {
      String propertyName = StringUtils.substring(sort, 0,
        StringUtils.lastIndexOf(sort, "/")).trim();

      String ordering = StringUtils.substring(sort,
        StringUtils.lastIndexOf(sort, "/") + 1).trim();

      if(StringUtils.containsIgnoreCase(ordering, "descending")) {
        return Comparator.comparing(
          (JsonObject record) -> getPropertyValue(record, propertyName))
          .reversed();
      }
      else {
        return Comparator.comparing(record -> getPropertyValue(record, propertyName));
      }
    }
    else {
      return Comparator.comparing(record -> getPropertyValue(record, sort));
    }
  }

  private boolean containsSort(ImmutablePair<String, String> queryAndSort) {
    return StringUtils.isNotBlank(queryAndSort.right);
  }

  private Predicate<JsonObject> filterForQuery(String query) {
    if(StringUtils.isBlank(query)) {
      return t -> true;
    }

    String splitRegex;
    BinaryOperator<Predicate<JsonObject>> accumulator;

    final Matcher matcher = PATTERN.matcher(query);
    if (matcher.find()) {
      splitRegex = OR_REGEX;
      accumulator = Predicate::or;
    } else {
      splitRegex = " and ";
      accumulator = Predicate::and;
    }

    List<ImmutableTriple<String, String, String>> pairs =
      Arrays.stream(query.split(splitRegex))
        .map(pairText -> {
          String[] split = pairText.split("==|=|<>|<|>");

          printDiagnostics(() -> String.format("Split clause: %s",
            String.join(", ", split)));

          String searchField = split[0]
            .replaceAll("\"", "");

          String searchTerm = split[1]
            .replaceAll("\"", "")
            .replaceAll("\\*", "");

          if(pairText.contains("==")) {
            return new ImmutableTriple<>(searchField, searchTerm, "==");
          }
          else if(pairText.contains("=")) {
            return new ImmutableTriple<>(searchField, searchTerm, "=");
          }
          else if(pairText.contains("<>")) {
            return new ImmutableTriple<>(searchField, searchTerm, "<>");
          }
          else if(pairText.contains("<")) {
            return new ImmutableTriple<>(searchField, searchTerm, "<");
          }
          else if(pairText.contains(">")) {
            return new ImmutableTriple<>(searchField, searchTerm, ">");
          }
          else {
            //Should fail completely
            return new ImmutableTriple<>(searchField, searchTerm, "");
          }
        })
        .collect(Collectors.toList());

    return consolidateToSinglePredicate(
      pairs.stream()
        .map(pair -> filterByField(pair.getLeft(), pair.getMiddle(), pair.getRight()))
        .collect(Collectors.toList()), accumulator);
  }

  private Predicate<JsonObject> filterByField(String field, String term, String operator) {
    return record -> {
      final boolean result;
      final String propertyValue;

      if (term == null || field == null) {
        printDiagnostics(() -> "Either term or field are null, aborting filtering");
        return true;
      }
      else {
        propertyValue = getPropertyValue(record, field);

        String cleanTerm = removeBrackets(term);

        if (cleanTerm.contains("or")) {
          Collection<String> acceptableValues = Arrays.stream(cleanTerm.split(" or "))
            .collect(Collectors.toList());

          Predicate<String> predicate = acceptableValues.stream()
            .map(this::filter)
            .reduce(Predicate::or)
            .orElse(t -> false);

          result = predicate.test(propertyValue);
        } else {
          if(propertyValue == null) {
            return false;
          }
          switch (operator) {
            case "==":
              result = propertyValue.equals(cleanTerm);
              break;
            case "=":
              result = propertyValue.contains(cleanTerm);
              break;
            case "<>":
              result = !propertyValue.contains(cleanTerm);
              break;
            case ">":
              result = propertyValue.compareTo(cleanTerm) > 0;
              break;
            case "<":
              result = propertyValue.compareTo(cleanTerm) < 0;
              break;
            default:
              result = false;
          }
        }

        printDiagnostics(() -> String.format("Filtering %s by %s %s %s: %s (value: %s)",
          record.encodePrettily(), field, operator, term, result, propertyValue));
      }

      return result;
    };
  }

  private String removeBrackets(String term) {
    return term.replace("(", "").replace(")", "");
  }

  private Predicate<String> filter(String term) {
    return v -> v.contains(term);
  }

  private String getPropertyValue(JsonObject record, String field) {
    //TODO: Should bomb if property does not exist
    if(field.contains(".")) {
      String[] fields = field.split("\\.");

      if(!record.containsKey(String.format("%s", fields[0]))) {
        return null;
      }

      return record.getJsonObject(String.format("%s", fields[0]))
        .getString(String.format("%s", fields[1].trim()));
    }
    else {
      return record.getString(String.format("%s", field.trim()));
    }
  }

  private Predicate<JsonObject> consolidateToSinglePredicate(
    Collection<Predicate<JsonObject>> predicates, BinaryOperator<Predicate<JsonObject>> accumulator) {

    return predicates.stream().reduce(accumulator).orElse(t -> false);
  }

  private ImmutablePair<String, String> splitQueryAndSort(String query) {
    if(StringUtils.containsIgnoreCase(query, "sortby")) {
      int sortByIndex = StringUtils.lastIndexOfIgnoreCase(query, "sortby");

      String searchOnly = StringUtils.substring(query, 0, sortByIndex).trim();
      String sortOnly = StringUtils.substring(query, sortByIndex + 6).trim();

      return new ImmutablePair<>(searchOnly, sortOnly);
    }
    else {
      return new ImmutablePair<>(query, "");
    }
  }

  private void printDiagnostics(Supplier<String> diagnosticTextSupplier) {
    if(diagnosticsEnabled) {
      System.out.println(diagnosticTextSupplier.get());
    }
  }
}
