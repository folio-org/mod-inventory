package org.folio.inventory.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CqlHelperTest {

  @Test
  void multipleRecordIdsCqlQuery() {
    assertThat(urlDecode(multi("a", "b", "c"))).isEqualTo("id==(a or b or c)");
  }

  @Test
  void oneRecordIdCqlQuery() {
    assertThat(urlDecode(multi("a"))).isEqualTo("id==(a)");
  }

  @Test
  void oneRecordCustomPrefixCqlQuery() {
    assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter=", List.of("a")))
      .isEqualTo("parameter=(a)");
  }

  @Test
  void multipleRecordCustomPrefixCqlQuery() {
    assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter=", List.of("a", "b", "c")))
      .isEqualTo("parameter=(a or b or c)");
  }

  @ParameterizedTest
  @MethodSource("barcodeParams")
  void barcodeQuery(String barcode, String cql) {
    assertThat(CqlHelper.barcodeIs(barcode)).isEqualTo(cql);
  }

  private String multi(String... strings) {
    return CqlHelper.multipleRecordsCqlQuery(Arrays.asList(strings));
  }

  private String urlDecode(String s) {
    return URLDecoder.decode(s, StandardCharsets.UTF_8);
  }

  private static Stream<Arguments> barcodeParams() {
    return Stream.of(
      Arguments.of("", "barcode==\"\""),                                  // barcode==""
      Arguments.of("abc", "barcode==\"abc\""),                            // barcode=="abc"
      Arguments.of("*", "barcode==\"\\*\""),                              // barcode=="\*"
      Arguments.of("?", "barcode==\"\\?\""),                              // barcode=="\?"
      Arguments.of("^", "barcode==\"\\^\""),                              // barcode=="\^"
      Arguments.of("\"", "barcode==\"\\\"\""),                            // barcode=="\""
      Arguments.of("\\", "barcode==\"\\\\\""),                            // barcode=="\\"
      Arguments.of("*?^\"\\*?^\"\\",
        "barcode==\"\\*\\?\\^\\\"\\\\\\*\\?\\^\\\"\\\\\"") // barcode=="\*\?\^\"\\\*\?\^\"\\"
    );
  }
}
