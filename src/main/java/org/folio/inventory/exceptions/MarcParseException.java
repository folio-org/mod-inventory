package org.folio.inventory.exceptions;

/**
 * When parsing the MARC input fails.
 */
public class MarcParseException extends Exception {
  private static final long serialVersionUID = 1298961322759720039L;

  /**
   * When parsing the MARC input fails.
   * @param message  the detail message
   */
  public MarcParseException(String message) {
    super(message);
  }

  /**
   * When parsing the MARC input fails.
   * @param message  the detail message
   * @param exception  the exception causing the failure
   */
  public MarcParseException(String message, Exception exception) {
    super(message, exception);
  }
}
