package org.folio.inventory.domain;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Instance {
  public final String id;
  public final String title;
  public final List<Identifier> identifiers;

  public Instance(String title, List<Identifier> identifiers) {
    this(null, title, identifiers);
  }

  public Instance(String title) {
    this(null, title, new ArrayList());
  }

  public Instance(String id, String title, List<Identifier> identifiers) {
    this.id = id;
    this.title = title;

    if(identifiers != null) {
      this.identifiers = identifiers.stream().collect(Collectors.toList());
    }
    else {
      this.identifiers = new ArrayList<>();
    }
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.title, this.identifiers);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> identifiers = this.identifiers.stream()
      .collect(Collectors.toList());

    identifiers.add(identifier);

    return new Instance(id, title, identifiers);
  }

  public Instance addIdentifier(String namespace, String value) {
    Identifier identifier = new Identifier(namespace, value);

    return addIdentifier(identifier);
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, Title: %s", id, title);
  }

  public Instance removeIdentifier(final String namespace, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(it.namespace.equals(namespace) && it.value.equals(value)))
      .collect(Collectors.toList());

    return new Instance(id, title, newIdentifiers);
  }

}
