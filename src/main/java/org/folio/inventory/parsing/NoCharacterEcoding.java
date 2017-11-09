package org.folio.inventory.parsing;

public class NoCharacterEcoding implements CharacterEncoding {
  @Override
  public String decode(String input) {
    return input;
  }
}
