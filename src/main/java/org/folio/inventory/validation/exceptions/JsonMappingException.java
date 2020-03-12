package org.folio.inventory.validation.exceptions;

/**
 * Exception for building entities via json.
 */
public class JsonMappingException extends RuntimeException {

  public JsonMappingException(String message, Throwable cause) {
    super(message, cause);
  }
}
