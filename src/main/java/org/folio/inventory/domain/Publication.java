package org.folio.inventory.domain;

/**
 *
 * @author ne
 */
public class Publication {
  public static final String PUBLISHER = "publisher";
  public static final String PLACE = "place";
  public static final String DATE_OF_PUBLICATION = "dateOfPublication";

  public final String publisher;
  public final String place;
  public final String dateOfPublication;

  public Publication(String publisher, String place, String dateOfPublication) {
    this.publisher = publisher;
    this.place = place;
    this.dateOfPublication = dateOfPublication;
  }
}
