package org.folio.inventory.domain;

/**
 *
 * @author ne
 */
public class Publication {
  public final String publisher;
  public final String place;
  public final String dateOfPublication;
  
  public Publication(String publisher, String place, String dateOfPublication) {
    this.publisher = publisher;
    this.place = place;
    this.dateOfPublication = dateOfPublication;
  }
}
