package org.folio.inventory.domain.user;

import io.vertx.core.json.JsonObject;

public class Personal {

  public static final String LAST_NAME_KEY = "lastName";
  public static final String FIRST_NAME_KEY = "firstName";

  private final String lastName;
  private final String firstName;

  public Personal(String lastName, String firstName) {
    this.lastName = lastName;
    this.firstName = firstName;
  }

  public Personal(JsonObject json) {
    this(json.getString(LAST_NAME_KEY), json.getString(FIRST_NAME_KEY));
  }

  public String getLastName() {
    return lastName;
  }

  public String getFirstName() {
    return firstName;
  }
}
