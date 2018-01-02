package org.folio.inventory.exceptions;

/**
 * When the config file for the MARC parser is invalid.
 */
public class InvalidMarcConfigException extends MarcParseException {
  private static final long serialVersionUID = -6365717828653315048L;

  /**
   * When the config file for the MARC parser is invalid.
   * @param message reason
   */
  public InvalidMarcConfigException(String message) {
    super(message);
  }

  /**
   * When the config file for the MARC parser is invalid.
   * @param message reason
   * @param exception  the exception causing the failure
   */
  public InvalidMarcConfigException(String message, Exception exception) {
    super(message, exception);
  }
}
