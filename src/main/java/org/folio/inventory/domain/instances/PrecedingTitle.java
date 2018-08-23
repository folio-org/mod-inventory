package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class PrecedingTitle {
  // JSON property names
  public static final String TITLE_KEY = "title";
  public static final String ISSN_KEY = "issn";
  public static final String ISBN_KEY = "isbn";

  public final String title;
  public final String issn;
  public final String isbn;

  public PrecedingTitle(String title, String issn, String isbn) {
    this.title = title;
    this.issn = issn;
    this.isbn = isbn;
  }

  public PrecedingTitle(JsonObject json) {
    this(json.getString(TITLE_KEY),
         json.getString(ISSN_KEY),
         json.getString(ISBN_KEY));
  }

}
