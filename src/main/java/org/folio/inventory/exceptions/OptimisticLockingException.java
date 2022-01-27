package org.folio.inventory.exceptions;

/**
 * Exception for handling errors regarding Optimistic Locking mechanism
 */
public class OptimisticLockingException extends Exception {
  public OptimisticLockingException(String message) {
    super(message);
  }
}
