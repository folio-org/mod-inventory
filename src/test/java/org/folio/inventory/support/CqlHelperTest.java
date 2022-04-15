package org.folio.inventory.support;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class CqlHelperTest {
  private String multi(String ...strings) {
    return CqlHelper.multipleRecordsCqlQuery(Arrays.asList(strings));
  }

  private String urlDecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void multipleRecordIdsCqlQuery() {
    Assertions.assertThat(urlDecode(multi("a", "b", "c"))).isEqualTo("id==(a or b or c)");
  }

  @Test
  public void oneRecordIdCqlQuery() {
    Assertions.assertThat(urlDecode(multi("a"))).isEqualTo("id==(a)");
  }

  @Test
  public void oneRecordParameterCqlQuery() {
    Assertions.assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter", List.of("a")))
            .isEqualTo("parameter==(a)");
  }

  @Test
  public void multipleRecordParametersCqlQuery() {
    Assertions.assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter", List.of("a", "b", "c")))
            .isEqualTo("parameter==(a or b or c)");
  }

  @Test
  public void oneRecordCustomComparisonOperatorCqlQuery() {
    Assertions.assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter", "=", List.of("a")))
            .isEqualTo("parameter=(a)");
  }

  @Test
  public void multipleRecordCustomComparisonOperatorCqlQuery() {
    Assertions.assertThat(CqlHelper.buildMultipleValuesCqlQuery("parameter", "=", List.of("a", "b", "c")))
            .isEqualTo("parameter=(a or b or c)");
  }

  @Test
  @Parameters({
    "    | barcode==\"\"",      // barcode==""
    "abc | barcode==\"abc\"",   // barcode=="abc"
    "*   | barcode==\"\\*\"",   // barcode=="\*"
    "?   | barcode==\"\\?\"",   // barcode=="\?"
    "^   | barcode==\"\\^\"",   // barcode=="\^"
    "\"  | barcode==\"\\\"\"",  // barcode=="\""
    "\\  | barcode==\"\\\\\"",  // barcode=="\\"
    "*?^\"\\*?^\"\\ | barcode==\"\\*\\?\\^\\\"\\\\\\*\\?\\^\\\"\\\\\"", // barcode=="\*\?\^\"\\\*\?\^\"\\"
  })
  public void barcode(String barcode, String cql) {
    assertThat(CqlHelper.barcodeIs(barcode), is(cql));
  }
}
