package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class Subject extends Authorized {
  // JSON property names
  private static final String VALUE_KEY = "value";
  private static final String SUBJECT_SOURCE_KEY = "subjectSourceId";
  private static final String SUBJECT_TYPE_KEY = "subjectTypeId";

  private final String value;

  private final String subjectSourceId;

  private final String subjectTypeId;

  public Subject(String value, String authorityId, String subjectSourceId, String subjectTypeId) {
    super(authorityId);
    this.value = value;
    this.subjectSourceId = subjectSourceId;
    this.subjectTypeId = subjectTypeId;
  }

  public Subject(JsonObject json) {
    this(
      json.getString(VALUE_KEY),
      json.getString(AUTHORITY_ID_KEY),
      json.getString(SUBJECT_SOURCE_KEY),
      json.getString(SUBJECT_TYPE_KEY)
    );
  }

  public String getAuthorityId() {
    return authorityId;
  }

  public String getValue() {
    return value;
  }

  public String getSubjectSourceId() {
    return subjectSourceId;
  }

  public String getSubjectTypeId() {
    return subjectTypeId;
  }
}
