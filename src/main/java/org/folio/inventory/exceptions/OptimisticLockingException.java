package org.folio.inventory.exceptions;

/**
 * Exception for handling errors regarding Optimistic Locking mechanism.
 */
public class OptimisticLockingException extends RuntimeException {

  public OptimisticLockingException(String message) {
    super(message);
  }
}
