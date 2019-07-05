package org.folio.inventory.domain.user;

public class User {

  private String id;
  private Personal personal;

  public User(String id, Personal personal) {
    this.id = id;
    this.personal = personal;
  }

  public String getId() {
    return id;
  }

  public Personal getPersonal() {
    return personal;
  }
}
