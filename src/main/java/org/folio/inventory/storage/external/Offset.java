package org.folio.inventory.storage.external;

public final class Offset {
  private final int offsetValue;

  private Offset(int offset) {
    this.offsetValue = offset;
  }

  public static Offset offset(int offset) {
    return new Offset(offset);
  }

  public static Offset noOffset() {
    return offset(0);
  }

  public int getOffset() {
    return offsetValue;
  }
}
