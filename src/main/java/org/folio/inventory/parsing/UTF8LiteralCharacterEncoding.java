package org.folio.inventory.parsing;

public class UTF8LiteralCharacterEncoding implements CharacterEncoding {
  @Override
  public String decode(String input) {
    return input
      .replace("\\xE2\\x80\\x99", "\u2019")
      .replace("\\xC3\\xA9", "\u00E9")
      .replace("\\xCC\\x81", "\u0301");
  }
}
