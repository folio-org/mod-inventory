package org.folio.inventory.exceptions;

/**
 * When a MARC JSON cannot be parsed.
 */
public class InvalidMarcJsonException extends MarcParseException {
  private static final long serialVersionUID = 2912347291082351863L;

  /**
   * When a MARC JSON cannot be parsed.
   * @param message  reason
   */
  public InvalidMarcJsonException(String message) {
    super(message);
  }
}
