package org.folio.inventory.domain.user;

public class Personal {

  private String lastName;
  private String firstName;

  public Personal(String lastName, String firstName) {
    this.lastName = lastName;
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getFirstName() {
    return firstName;
  }
}
