package org.folio.inventory.domain;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Instance {
  /** key of the id property of an Instance JSON */
  public static final String ID_NAME = "id";
  /** key of the title property of an Instance JSON */
  public static final String TITLE_PROPERTY_NAME = "title";
  /** key of the identifiers array (ISBN, etc.) of an Instance JSON */
  public static final String IDENTIFIER_PROPERTY_NAME = "identifiers";
  /** key of the contributors array of an Instance JSON */
  public static final String CONTRIBUTORS_PROPERTY_NAME = "contributors";
  /** key of the type id property of an Instance JSON */
  public static final String INSTANCE_TYPE_ID_NAME = "instanceTypeId";
  /** key of the source property of an Instance JSON */
  public static final String SOURCE_NAME = "source";
  /** key of the Base64 encoded source binary of an Instance JSON */
  public static final String SOURCE_BINARY_BASE64_NAME = "sourceBinaryBase64";
  /** key of the source binary's format of an Instance JSON */
  public static final String SOURCE_BINARY_FORMAT_NAME = "sourceBinaryFormat";

  public final String id;
  public final String title;
  public final String source;
  public final String instanceTypeId;
  public final List<Identifier> identifiers;
  public final List<Contributor> contributors;
  public final String sourceBinaryBase64;
  public final String sourceBinaryFormat;

  public Instance(
      String id,
      String title,
      List<Identifier> identifiers,
      String source,
      String instanceTypeId,
      List<Contributor> contributors,
      String sourceBinaryBase64,
      String sourceBinaryFormat) {

      this.id = id;
      this.title = title;
      this.identifiers = new ArrayList<>(identifiers);
      this.source = source;
      this.instanceTypeId = instanceTypeId;
      this.contributors = new ArrayList<>(contributors);
      this.sourceBinaryBase64 = sourceBinaryBase64;
      this.sourceBinaryFormat = sourceBinaryFormat;
    }

  public Instance(
      String id,
      String title,
      List<Identifier> identifiers,
      String source,
      String instanceTypeId,
      List<Contributor> contributors) {
    this(id, title, identifiers, source, instanceTypeId, contributors, null, null);
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, this.title, this.identifiers, this.source,
      this.instanceTypeId, this.contributors, this.sourceBinaryBase64, this.sourceBinaryFormat);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> newIdentifiers = new ArrayList<>(this.identifiers);

    newIdentifiers.add(identifier);

    return new Instance(this.id, this.title, newIdentifiers, this.source,
      this.instanceTypeId, this.contributors, this.sourceBinaryBase64, this.sourceBinaryFormat);
  }

  public Instance addIdentifier(String identifierTypeId, String value) {
    Identifier identifier = new Identifier(identifierTypeId, value);

    return addIdentifier(identifier);
  }

  public Instance addContributor(String contributorNameTypeId, String name) {
    List<Contributor> newContributors = new ArrayList<>(this.contributors);

    newContributors.add(new Contributor(contributorNameTypeId, name));

    return new Instance(this.id, this.title, new ArrayList<>(this.identifiers),
      this.source, this.instanceTypeId, newContributors,
      this.sourceBinaryBase64, this.sourceBinaryFormat);
  }

  public Instance removeIdentifier(final String identifierTypeId, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value)))
      .collect(Collectors.toList());

    return new Instance(this.id, this.title, newIdentifiers, this.source,
      this.instanceTypeId, this.contributors,
      this.sourceBinaryBase64, this.sourceBinaryFormat);
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, Title: %s", id, title);
  }
}
