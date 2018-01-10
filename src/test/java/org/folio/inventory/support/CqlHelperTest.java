package org.folio.inventory.support;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

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
  public void multipleRecordsCqlQuery() {
    assertThat(urlDecode(multi("a"          )), is("id==(a)"));
    assertThat(urlDecode(multi("a", "b", "c")), is("id==(a or b or c)"));
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
