package support.fakes;

import io.vertx.core.json.JsonObject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FakeCqlToJsonInterpreter {

  private static final Logger LOGGER = LogManager.getLogger(FakeCqlToJsonInterpreter.class);

  // " or ) at the left and a-z at right
  private static final String OR_REGEX = "(?<=[\")]) or (?=[a-z])";
  private static final Pattern PATTERN = Pattern.compile(OR_REGEX);
  private final boolean diagnosticsEnabled;

  public FakeCqlToJsonInterpreter(boolean diagnosticsEnabled) {
    this.diagnosticsEnabled = diagnosticsEnabled;
  }

  public List<JsonObject> execute(Collection<JsonObject> records, String encodedQuery) {
    var query = encodedQuery == null ? null : URLDecoder.decode(encodedQuery, StandardCharsets.UTF_8);
    ImmutablePair<String, String> queryAndSort = splitQueryAndSort(query);

    if (containsSort(queryAndSort)) {
      printDiagnostics(() -> String.format("Search by: %s", queryAndSort.left));
      printDiagnostics(() -> String.format("Sort by: %s", queryAndSort.right));

      return records.stream()
        .filter(filterForQuery(queryAndSort.left))
        .sorted(sortForQuery(queryAndSort.right))
        .toList();
    } else {
      printDiagnostics(() -> String.format("Search only by: %s", queryAndSort.left));

      return records.stream()
        .filter(filterForQuery(queryAndSort.left))
        .toList();
    }
  }

  /**
   * Builds a comparator for the sortBy clause, e.g. {@code title}, {@code title/sort.descending} or
   * {@code order/number/sort.descending}. The {@code number} modifier compares values numerically instead
   * of as text. Nulls go last when ascending and first when descending, like in PostgreSQL.
   */
  private Comparator<? super JsonObject> sortForQuery(String sort) {
    String[] parts = sort.split("/");
    String propertyName = parts[0].trim();
    boolean numeric = Arrays.stream(parts).skip(1).anyMatch(part -> Strings.CI.equals(part.trim(), "number"));
    boolean descending = Arrays.stream(parts).skip(1).anyMatch(part -> Strings.CI.contains(part, "descending"));

    Comparator<Comparable<Object>> valueComparator = Comparator.nullsLast(Comparator.naturalOrder());
    Comparator<JsonObject> comparator = Comparator.comparing(
      jsonObject -> sortValue(jsonObject, propertyName, numeric), valueComparator);

    return descending ? comparator.reversed() : comparator;
  }

  @SuppressWarnings("unchecked")
  private Comparable<Object> sortValue(JsonObject jsonObject, String propertyName, boolean numeric) {
    String value = getPropertyValue(jsonObject, propertyName);
    if (value == null) {
      return null;
    }
    return (Comparable<Object>) (numeric ? (Comparable<?>) Double.valueOf(value) : value);
  }

  private boolean containsSort(ImmutablePair<String, String> queryAndSort) {
    return StringUtils.isNotBlank(queryAndSort.right);
  }

  private Predicate<JsonObject> filterForQuery(String query) {
    if (StringUtils.isBlank(query)) {
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
            .replace("\"", "");

          String searchTerm = split[1]
            .replace("\"", "")
            .replaceAll("\\*", "");

          if (pairText.contains("==")) {
            return new ImmutableTriple<>(searchField, searchTerm, "==");
          } else if (pairText.contains("=")) {
            return new ImmutableTriple<>(searchField, searchTerm, "=");
          } else if (pairText.contains("<>")) {
            return new ImmutableTriple<>(searchField, searchTerm, "<>");
          } else if (pairText.contains("<")) {
            return new ImmutableTriple<>(searchField, searchTerm, "<");
          } else if (pairText.contains(">")) {
            return new ImmutableTriple<>(searchField, searchTerm, ">");
          } else {
            //Should fail completely
            return new ImmutableTriple<>(searchField, searchTerm, "");
          }
        })
        .toList();

    return consolidateToSinglePredicate(
      pairs.stream()
        .map(pair -> filterByField(pair.getLeft(), pair.getMiddle(), pair.getRight()))
        .toList(), accumulator);
  }

  private Predicate<JsonObject> filterByField(String field, String term, String operator) {
    return dataEntry -> {
      final boolean result;
      final String propertyValue;

      if (term == null || field == null) {
        printDiagnostics(() -> "Either term or field are null, aborting filtering");
        return true;
      } else {
        propertyValue = getPropertyValue(dataEntry, field);

        String cleanTerm = removeBrackets(term);

        if (cleanTerm.contains("or")) {
          Collection<String> acceptableValues = Arrays.stream(cleanTerm.split(" or "))
            .toList();

          Predicate<String> predicate = acceptableValues.stream()
            .map(this::filter)
            .reduce(Predicate::or)
            .orElse(t -> false);

          result = predicate.test(propertyValue);
        } else {
          if (propertyValue == null) {
            return false;
          }
          result = switch (operator) {
            case "==" -> propertyValue.equals(cleanTerm);
            case "=" -> propertyValue.contains(cleanTerm);
            case "<>" -> !propertyValue.contains(cleanTerm);
            case ">" -> propertyValue.compareTo(cleanTerm) > 0;
            case "<" -> propertyValue.compareTo(cleanTerm) < 0;
            default -> false;
          };
        }

        printDiagnostics(() -> String.format("Filtering %s by %s %s %s: %s (value: %s)",
          dataEntry.encodePrettily(), field, operator, term, result, propertyValue));
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

  private String getPropertyValue(JsonObject jsonRecord, String field) {
    if (field.contains(".")) {
      String[] fields = field.split("\\.");

      if (!jsonRecord.containsKey(String.format("%s", fields[0]))) {
        return null;
      }

      return jsonRecord.getJsonObject(String.format("%s", fields[0]))
        .getString(String.format("%s", fields[1].trim()));
    } else {
      // getValue instead of getString: numeric properties such as order are not strings
      return Objects.toString(jsonRecord.getValue(field.trim()), null);
    }
  }

  private Predicate<JsonObject> consolidateToSinglePredicate(
    Collection<Predicate<JsonObject>> predicates, BinaryOperator<Predicate<JsonObject>> accumulator) {
    return predicates.stream().reduce(accumulator).orElse(t -> false);
  }

  private ImmutablePair<String, String> splitQueryAndSort(String query) {
    if (Strings.CI.contains(query, "sortby")) {
      int sortByIndex = Strings.CI.lastIndexOf(query, "sortby");

      String searchOnly = StringUtils.substring(query, 0, sortByIndex).trim();
      String sortOnly = StringUtils.substring(query, sortByIndex + 6).trim();

      return new ImmutablePair<>(searchOnly, sortOnly);
    } else {
      return new ImmutablePair<>(query, "");
    }
  }

  private void printDiagnostics(Supplier<String> diagnosticTextSupplier) {
    if (diagnosticsEnabled) {
      LOGGER.info(diagnosticTextSupplier.get());
    }
  }
}
