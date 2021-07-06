package org.folio.inventory.support;

import java.util.List;
import java.util.stream.Collectors;
import org.folio.util.StringUtil;

/**
 * Helper for CQL queries.
 */
public class CqlHelper {
  private CqlHelper() { }

  public static String multipleRecordsCqlQuery(List<String> recordIds) {
    if(recordIds.isEmpty()) {
      return null;
    }
    return buildQueryByIds(recordIds);
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
    return "barcode==" + StringUtil.cqlEncode(barcode);
  }
}
