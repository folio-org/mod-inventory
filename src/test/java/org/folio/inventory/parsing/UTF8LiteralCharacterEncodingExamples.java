package org.folio.inventory.parsing;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class UTF8LiteralCharacterEncodingExamples {
  private final UTF8LiteralCharacterEncoding encoding = new UTF8LiteralCharacterEncoding();

  @Test
  public void encodedCharactersReplacedByUTF16Equivalents() {

    testDecoding("[Dell\\xE2\\x80\\x99Emulazione e dell\\xE2\\x80\\x99]",
      "\\xE2\\x80\\x99", "\u2019");

    testDecoding("Pavle Nik Nikitovic\\xCC\\x81",
      "\\xCC\\x81", "\u0301");

    testDecoding(
      "Grammaire compar\\xC3\\xA9e du grec et du latin. Phon\\xC3\\xA9tique",
      "\\xC3\\xA9", "\u00E9");
  }

  private void testDecoding(
    String input,
    String toReplace,
    String replaceWith) {

    String decoded = encoding.decode(input);

    assertThat(decoded, containsString(replaceWith));
    assertThat(decoded, not(containsString(toReplace)));
  }
}
