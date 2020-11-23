package org.folio.inventory.support;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Helper for CQL queries.
 */
public class CqlHelper {
  private CqlHelper() { }

  private static final Pattern cqlChar = Pattern.compile("[*?^\"\\\\]");

  public static String multipleRecordsCqlQuery(List<String> recordIds) {
    if(recordIds.isEmpty()) {
      return null;
    }
    else {
      String query = buildQueryByIds(recordIds);
      return URLEncoder.encode(query, StandardCharsets.UTF_8);

    }
  }

  /**
   * Returns non-encoded CQL query with ids of records
   * @param recordIds record's ids
   * @return CQL expression
   */
  public static String buildQueryByIds(List<String> recordIds) {
    return String.format("id==(%s)", recordIds.stream()
        .map(String::toString)
        .distinct()
        .collect(Collectors.joining(" or ")));
  }

  /**
   * Returns a CQL expression with an exact match for barcode.
   * <p>
   * barcodeIs("abc") = "barcode==\"abc\""<br>
   * barcodeIs("1-*?") = "barcode==\"1-\\*\\?\""
   * @param barcode  String to match
   * @return CQL expression
   */
  public static String barcodeIs(String barcode) {
    return "barcode==\"" + cqlMask(barcode) + "\"";
  }

  /**
   * Mask these special CQL characters by prepending a backslash: * ? ^ " \
   *
   * @param s  the String to mask
   * @return s with all special CQL characters masked
   */
  public static String cqlMask(String s) {
    if (s == null) {
      return null;
    }
    return cqlChar.matcher(s).replaceAll("\\\\$0");  // one backslash plus the matching character
  }
}
