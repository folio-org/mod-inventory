package org.folio.inventory.dataimport.util;

/**
 * Signals that a MARC record's content could not be read, parsed, mutated, or written back.
 * Internal "this failed" signal used by the throwing cores in {@link AdditionalFieldsUtil}, which their
 * public catch-log-{@code false} facade methods catch as a plain {@link Exception}.
 */
public class MarcContentException extends RuntimeException {

  public MarcContentException(String message) {
    super(message);
  }

  public MarcContentException(String message, Throwable cause) {
    super(message, cause);
  }
}
