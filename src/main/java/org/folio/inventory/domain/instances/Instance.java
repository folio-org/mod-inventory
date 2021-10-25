package org.folio.inventory.domain.instances;

import static java.lang.String.format;
import static org.folio.inventory.domain.instances.PublicationPeriod.publicationPeriodFromJson;
import static org.folio.inventory.domain.instances.PublicationPeriod.publicationPeriodToJson;
import static org.folio.inventory.support.JsonArrayHelper.toListOfStrings;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.WebContext;
import org.folio.inventory.domain.Metadata;
import org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle;
import org.folio.inventory.domain.sharedproperties.ElectronicAccess;
import org.folio.inventory.support.JsonArrayHelper;

public class Instance {
  // JSON property names
  public static final String VERSION_KEY = "_version";
  public static final String HRID_KEY = "hrid";
  public static final String MATCH_KEY_KEY = "matchKey";
  public static final String SOURCE_KEY = "source";
  public static final String PARENT_INSTANCES_KEY = "parentInstances";
  public static final String CHILD_INSTANCES_KEY = "childInstances";
  public static final String PRECEDING_TITLES_KEY = "precedingTitles";
  public static final String SUCCEEDING_TITLES_KEY = "succeedingTitles";
  public static final String IS_BOUND_WITH_KEY = "isBoundWith";
  public static final String TITLE_KEY = "title";
  public static final String INDEX_TITLE_KEY = "indexTitle";
  public static final String ALTERNATIVE_TITLES_KEY = "alternativeTitles";
  public static final String EDITIONS_KEY = "editions";
  public static final String SERIES_KEY = "series";
  public static final String IDENTIFIERS_KEY = "identifiers";
  public static final String CONTRIBUTORS_KEY = "contributors";
  public static final String SUBJECTS_KEY = "subjects";
  public static final String CLASSIFICATIONS_KEY = "classifications";
  public static final String PUBLICATION_KEY = "publication";
  public static final String PUBLICATION_FREQUENCY_KEY = "publicationFrequency";
  public static final String PUBLICATION_RANGE_KEY = "publicationRange";
  public static final String ELECTRONIC_ACCESS_KEY = "electronicAccess";
  public static final String INSTANCE_TYPE_ID_KEY = "instanceTypeId";
  public static final String INSTANCE_FORMAT_IDS_KEY = "instanceFormatIds";
  public static final String PHYSICAL_DESCRIPTIONS_KEY = "physicalDescriptions";
  public static final String LANGUAGES_KEY = "languages";
  public static final String NOTES_KEY = "notes";
  public static final String MODE_OF_ISSUANCE_ID_KEY = "modeOfIssuanceId";
  public static final String CATALOGED_DATE_KEY = "catalogedDate";
  public static final String PREVIOUSLY_HELD_KEY = "previouslyHeld";
  public static final String STAFF_SUPPRESS_KEY = "staffSuppress";
  public static final String DISCOVERY_SUPPRESS_KEY = "discoverySuppress";
  public static final String STATISTICAL_CODE_IDS_KEY = "statisticalCodeIds";
  public static final String SOURCE_RECORD_FORMAT_KEY = "sourceRecordFormat";
  public static final String STATUS_ID_KEY = "statusId";
  public static final String STATUS_UPDATED_DATE_KEY = "statusUpdatedDate";
  public static final String METADATA_KEY = "metadata";
  public static final String TAGS_KEY = "tags";
  public static final String TAG_LIST_KEY = "tagList";
  public static final String NATURE_OF_CONTENT_TERM_IDS_KEY = "natureOfContentTermIds";
  public static final String PUBLICATION_PERIOD_KEY = "publicationPeriod";

  private final String id;
  @JsonProperty("_version")
  private String version;
  private final String hrid;
  private String matchKey;
  private final String source;
  private List<InstanceRelationshipToParent> parentInstances = new ArrayList();
  private List<InstanceRelationshipToChild> childInstances = new ArrayList();
  private List<PrecedingSucceedingTitle> precedingTitles = new ArrayList<>();
  private List<PrecedingSucceedingTitle> succeedingTitles = new ArrayList<>();
  private boolean isBoundWith = false;
  private final String title;
  private String indexTitle;
  private List<AlternativeTitle> alternativeTitles = new ArrayList();
  private List<String> editions = new ArrayList();
  private List<String> series = new ArrayList();
  private List<Identifier> identifiers = new ArrayList();
  private List<Contributor> contributors = new ArrayList();
  private List<String> subjects = new ArrayList();
  private List<Classification> classifications = new ArrayList();
  private List<Publication> publication = new ArrayList();
  private List<String> publicationFrequency = new ArrayList();
  private List<String> publicationRange = new ArrayList();
  private List<ElectronicAccess> electronicAccess = new ArrayList();
  private final String instanceTypeId;
  private List<String> instanceFormatIds;
  private List<String> physicalDescriptions = new ArrayList();
  private List<String> languages = new ArrayList();
  private List<Note> notes = new ArrayList();
  private String modeOfIssuanceId;
  private String catalogedDate;
  private Boolean previouslyHeld;
  private Boolean staffSuppress;
  private Boolean discoverySuppress;
  private List<String> statisticalCodeIds = new ArrayList();
  private String sourceRecordFormat;
  private String statusId;
  private String statusUpdatedDate;
  private Metadata metadata = null;
  private List<String> tags;
  private List<String> natureOfContentTermIds = new ArrayList<>();
  private PublicationPeriod publicationPeriod;

  protected static final String INVENTORY_PATH = "/inventory";
  protected static final String INSTANCES_PATH = INVENTORY_PATH + "/instances";
  protected static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  public Instance(
    String id,
    String version,
    String hrid,
    String source,
    String title,
    String instanceTypeId) {

    this.id = id;
    this.version = version;
    this.hrid = hrid;
    this.source = source;
    this.title = title;
    this.instanceTypeId = instanceTypeId;
  }

  /**
   * Creates Instance POJO from JSON.
   * Note: Doesn't set Metadata (since some DI processing seems to fail with it)
   *       Metadata thus have to be added after instantiation where required.
   * @param instanceJson  JSON from client request or storage server response
   * @return Instance object that holds all (known) properties from the JSON
   */
  public static Instance fromJson(JsonObject instanceJson) {

    return new Instance(
      instanceJson.getString("id"),
      instanceJson.getString(VERSION_KEY),
      instanceJson.getString("hrid"),
      instanceJson.getString(SOURCE_KEY),
      instanceJson.getString(TITLE_KEY),
      instanceJson.getString(INSTANCE_TYPE_ID_KEY))
      .setIndexTitle(instanceJson.getString(INDEX_TITLE_KEY))
      .setMatchKey(instanceJson.getString(MATCH_KEY_KEY))
      .setParentInstances(instanceJson.getJsonArray(PARENT_INSTANCES_KEY))
      .setChildInstances(instanceJson.getJsonArray(CHILD_INSTANCES_KEY))
      .setPrecedingTitles(instanceJson.getJsonArray(PRECEDING_TITLES_KEY))
      .setSucceedingTitles(instanceJson.getJsonArray(SUCCEEDING_TITLES_KEY))
      .setIsBoundWith(instanceJson.containsKey(IS_BOUND_WITH_KEY) ? instanceJson.getBoolean(IS_BOUND_WITH_KEY) : false)
      .setAlternativeTitles(instanceJson.getJsonArray(ALTERNATIVE_TITLES_KEY))
      .setEditions(toListOfStrings(instanceJson.getJsonArray(EDITIONS_KEY)))
      .setSeries(toListOfStrings(instanceJson.getJsonArray(SERIES_KEY)))
      .setIdentifiers(instanceJson.getJsonArray(IDENTIFIERS_KEY))
      .setContributors(instanceJson.getJsonArray(CONTRIBUTORS_KEY))
      .setSubjects(toListOfStrings(instanceJson.getJsonArray(SUBJECTS_KEY)))
      .setClassifications(instanceJson.getJsonArray(CLASSIFICATIONS_KEY))
      .setPublication(instanceJson.getJsonArray(PUBLICATION_KEY))
      .setPublicationFrequency(toListOfStrings(instanceJson.getJsonArray(PUBLICATION_FREQUENCY_KEY)))
      .setPublicationRange(toListOfStrings(instanceJson.getJsonArray(PUBLICATION_RANGE_KEY)))
      .setElectronicAccess(instanceJson.getJsonArray(ELECTRONIC_ACCESS_KEY))
      .setInstanceFormatIds(toListOfStrings(instanceJson.getJsonArray(INSTANCE_FORMAT_IDS_KEY)))
      .setPhysicalDescriptions(toListOfStrings(instanceJson.getJsonArray(PHYSICAL_DESCRIPTIONS_KEY)))
      .setLanguages(toListOfStrings(instanceJson.getJsonArray(LANGUAGES_KEY)))
      .setNotes(instanceJson.getJsonArray(NOTES_KEY))
      .setModeOfIssuanceId(instanceJson.getString(MODE_OF_ISSUANCE_ID_KEY))
      .setCatalogedDate(instanceJson.getString(CATALOGED_DATE_KEY))
      .setPreviouslyHeld(instanceJson.getBoolean(PREVIOUSLY_HELD_KEY))
      .setStaffSuppress(instanceJson.getBoolean(STAFF_SUPPRESS_KEY))
      .setDiscoverySuppress(instanceJson.getBoolean(DISCOVERY_SUPPRESS_KEY))
      .setStatisticalCodeIds(toListOfStrings(instanceJson.getJsonArray(STATISTICAL_CODE_IDS_KEY)))
      .setSourceRecordFormat(instanceJson.getString(SOURCE_RECORD_FORMAT_KEY))
      .setStatusId(instanceJson.getString(STATUS_ID_KEY))
      .setStatusUpdatedDate(instanceJson.getString(STATUS_UPDATED_DATE_KEY))
      .setTags(getTags(instanceJson))
      .setNatureOfContentTermIds(toListOfStrings(instanceJson.getJsonArray(NATURE_OF_CONTENT_TERM_IDS_KEY)))
      .setPublicationPeriod(publicationPeriodFromJson(instanceJson.getJsonObject(PUBLICATION_PERIOD_KEY)));
  }

  /**
   *
   * @return  JSON representation of the Instance, compatible with FOLIO's
   * Instance storage API.
   */
  public JsonObject getJsonForStorage() {
    JsonObject json = new JsonObject();
    //TODO: Review if this shouldn't be defaulting here
    json.put("id", getId() != null
      ? getId()
      : UUID.randomUUID().toString());
    putIfNotNull(json, VERSION_KEY, version);
    json.put(HRID_KEY, hrid);
    if (source != null) json.put(SOURCE_KEY, source);
    json.put(MATCH_KEY_KEY, matchKey);
    json.put(TITLE_KEY, title);
    json.put(INDEX_TITLE_KEY, indexTitle);
    json.put(ALTERNATIVE_TITLES_KEY, alternativeTitles);
    json.put(EDITIONS_KEY, editions);
    json.put(SERIES_KEY, series);
    json.put(IDENTIFIERS_KEY, identifiers);
    json.put(CONTRIBUTORS_KEY, contributors);
    json.put(SUBJECTS_KEY, subjects);
    json.put(CLASSIFICATIONS_KEY, classifications);
    json.put(PUBLICATION_KEY, publication);
    json.put(PUBLICATION_FREQUENCY_KEY, publicationFrequency);
    json.put(PUBLICATION_RANGE_KEY, publicationRange);
    json.put(ELECTRONIC_ACCESS_KEY, electronicAccess);
    if (instanceTypeId != null) json.put(INSTANCE_TYPE_ID_KEY, instanceTypeId);
    json.put(INSTANCE_FORMAT_IDS_KEY, instanceFormatIds);
    json.put(PHYSICAL_DESCRIPTIONS_KEY, physicalDescriptions);
    json.put(LANGUAGES_KEY, languages);
    json.put(NOTES_KEY, notes);
    json.put(MODE_OF_ISSUANCE_ID_KEY, modeOfIssuanceId);
    json.put(CATALOGED_DATE_KEY, catalogedDate);
    json.put(PREVIOUSLY_HELD_KEY, previouslyHeld);
    json.put(STAFF_SUPPRESS_KEY, staffSuppress);
    json.put(DISCOVERY_SUPPRESS_KEY, discoverySuppress);
    json.put(STATISTICAL_CODE_IDS_KEY, statisticalCodeIds);
    if (sourceRecordFormat != null) json.put(SOURCE_RECORD_FORMAT_KEY, sourceRecordFormat);
    json.put(STATUS_ID_KEY, statusId);
    json.put(STATUS_UPDATED_DATE_KEY, statusUpdatedDate);
    json.put(TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray(getTags() == null ? Collections.emptyList() : getTags())));
    json.put(NATURE_OF_CONTENT_TERM_IDS_KEY, natureOfContentTermIds);
    putIfNotNull(json, PUBLICATION_PERIOD_KEY, publicationPeriodToJson(publicationPeriod));

    return json;
  }

  /**
   *
   * @param context
   * @return JSON representation of the Instance, compatible with Inventory's
   * Instance schema
   */
  public JsonObject getJsonForResponse(WebContext context) {
    JsonObject json = new JsonObject();

    try {
      json.put("@context", context.absoluteUrl(
        INSTANCES_PATH + "/context").toString());
    } catch (MalformedURLException e) {
      log.warn(
        format("Failed to create context link for instance: %s", e.toString()));
    }

    json.put("id", getId());
    putIfNotNull(json, VERSION_KEY, version);
    json.put("hrid", getHrid());
    json.put(SOURCE_KEY, getSource());
    json.put(TITLE_KEY, getTitle());
    putIfNotNull(json, MATCH_KEY_KEY, getMatchKey());
    putIfNotNull(json, INDEX_TITLE_KEY, getIndexTitle());
    putIfNotNull(json, PARENT_INSTANCES_KEY, parentInstances);
    putIfNotNull(json, CHILD_INSTANCES_KEY, childInstances);
    putIfNotNull(json, IS_BOUND_WITH_KEY, getIsBoundWith());
    putIfNotNull(json, ALTERNATIVE_TITLES_KEY, getAlternativeTitles());
    putIfNotNull(json, EDITIONS_KEY, getEditions());
    putIfNotNull(json, SERIES_KEY, getSeries());
    putIfNotNull(json, IDENTIFIERS_KEY, getIdentifiers());
    putIfNotNull(json, CONTRIBUTORS_KEY, getContributors());
    putIfNotNull(json, SUBJECTS_KEY, getSubjects());
    putIfNotNull(json, CLASSIFICATIONS_KEY, getClassifications());
    putIfNotNull(json, PUBLICATION_KEY, getPublication());
    putIfNotNull(json, PUBLICATION_FREQUENCY_KEY, getPublicationFrequency());
    putIfNotNull(json, PUBLICATION_RANGE_KEY, getPublicationRange());
    putIfNotNull(json, ELECTRONIC_ACCESS_KEY, getElectronicAccess());
    putIfNotNull(json, INSTANCE_TYPE_ID_KEY, getInstanceTypeId());
    putIfNotNull(json, INSTANCE_FORMAT_IDS_KEY, getInstanceFormatIds());
    putIfNotNull(json, PHYSICAL_DESCRIPTIONS_KEY, getPhysicalDescriptions());
    putIfNotNull(json, LANGUAGES_KEY, getLanguages());
    putIfNotNull(json, NOTES_KEY, getNotes());
    putIfNotNull(json, MODE_OF_ISSUANCE_ID_KEY, getModeOfIssuanceId());
    putIfNotNull(json, CATALOGED_DATE_KEY, getCatalogedDate());
    putIfNotNull(json, PREVIOUSLY_HELD_KEY, getPreviouslyHeld());
    putIfNotNull(json, STAFF_SUPPRESS_KEY, getStaffSuppress());
    putIfNotNull(json, DISCOVERY_SUPPRESS_KEY, getDiscoverySuppress());
    putIfNotNull(json, STATISTICAL_CODE_IDS_KEY, getStatisticalCodeIds());
    putIfNotNull(json, SOURCE_RECORD_FORMAT_KEY, getSourceRecordFormat());
    putIfNotNull(json, STATUS_ID_KEY, getStatusId());
    putIfNotNull(json, STATUS_UPDATED_DATE_KEY, getStatusUpdatedDate());
    putIfNotNull(json, METADATA_KEY, getMetadata());
    putIfNotNull(json, TAGS_KEY, new JsonObject().put(TAG_LIST_KEY, new JsonArray(getTags())));
    putIfNotNull(json, NATURE_OF_CONTENT_TERM_IDS_KEY, getNatureOfContentTermIds());
    putIfNotNull(json, PUBLICATION_PERIOD_KEY, publicationPeriodToJson(publicationPeriod));

    if (precedingTitles != null) {
      JsonArray precedingTitlesJsonArray = new JsonArray();
      precedingTitles.forEach(title -> precedingTitlesJsonArray.add(title.toPrecedingTitleJson()));
      json.put(PRECEDING_TITLES_KEY, precedingTitlesJsonArray );
    }

    if (succeedingTitles != null) {
      JsonArray succeedingTitlesJsonArray = new JsonArray();
      succeedingTitles.forEach(title -> succeedingTitlesJsonArray.add(title.toSucceedingTitleJson()));
      json.put(SUCCEEDING_TITLES_KEY, succeedingTitlesJsonArray );
    }

    try {
      URL selfUrl = context.absoluteUrl(format("%s/%s",
        INSTANCES_PATH, getId()));

      json.put("links", new JsonObject().put("self", selfUrl.toString()));
    } catch (MalformedURLException e) {
      log.warn(
        format("Failed to create self link for instance: %s", e.toString()));
    }

    return json;
  }



  public Instance setMatchKey(String matchKey) {
    this.matchKey = matchKey;
    return this;
  }

  public Instance setIndexTitle(String indexTitle) {
    this.indexTitle = indexTitle;
    return this;
  }

  public Instance setParentInstances(List<InstanceRelationshipToParent> parentInstances) {
    this.parentInstances = (parentInstances != null ? parentInstances : this.parentInstances);
    return this;
  }

  public Instance setParentInstances(JsonArray parentInstances) {
    this.parentInstances = parentInstances != null
      ? JsonArrayHelper.toList(parentInstances).stream()
      .map(InstanceRelationshipToParent::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    return this;
  }

  public Instance setChildInstances(List<InstanceRelationshipToChild> childInstances) {
    this.childInstances = (childInstances != null ? childInstances : this.childInstances);
    return this;
  }

  public Instance setChildInstances(JsonArray childInstances) {
    this.childInstances =
      childInstances != null
      ? JsonArrayHelper.toList(childInstances).stream()
              .map(InstanceRelationshipToChild::new)
              .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setPrecedingTitles(List<PrecedingSucceedingTitle> precedingTitles) {
    this.precedingTitles = (precedingTitles != null ? precedingTitles : this.precedingTitles);
    return this;
  }

  public Instance setPrecedingTitles(JsonArray precedingTitles) {
    this.precedingTitles =  precedingTitles != null
      ? JsonArrayHelper.toList(precedingTitles).stream()
      .map(PrecedingSucceedingTitle::from)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setSucceedingTitles(List<PrecedingSucceedingTitle> succeedingTitles) {
    this.succeedingTitles = succeedingTitles != null ? succeedingTitles : this.succeedingTitles;
    return this;
  }

  public Instance setSucceedingTitles(JsonArray succeedingTitles) {
    this.succeedingTitles = succeedingTitles != null
    ? JsonArrayHelper.toList(succeedingTitles).stream()
      .map(PrecedingSucceedingTitle::from)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setIsBoundWith(boolean isBoundWith) {
    this.isBoundWith = isBoundWith;
    return this;
  }

  public Instance setAlternativeTitles(List<AlternativeTitle> alternativeTitles) {
    this.alternativeTitles = alternativeTitles;
    return this;
  }

  public Instance setAlternativeTitles(JsonArray array) {
    this.alternativeTitles = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(AlternativeTitle::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setEditions(List<String> editions) {
    this.editions = editions;
    return this;
  }

  public Instance setSeries(List<String> series) {
    this.series = series;
    return this;
  }

  public Instance setIdentifiers(List<Identifier> identifiers) {
    this.identifiers = identifiers;
    return this;
  }

  public Instance setIdentifiers (JsonArray array) {
    this.identifiers = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(Identifier::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setContributors(List<Contributor> contributors) {
    this.contributors = contributors;
    return this;
  }

  public Instance setContributors (JsonArray array) {
    this.contributors = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(Contributor::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setSubjects(List<String> subjects) {
    this.subjects = subjects;
    return this;
  }

  public Instance setClassifications(List<Classification> classifications) {
    this.classifications = classifications;
    return this;
  }

  public Instance setClassifications(JsonArray array) {
    this.classifications = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(Classification::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setPublication(List<Publication> publication) {
    this.publication = publication;
    return this;
  }

  public Instance setPublication(JsonArray array) {
    this.publication = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(Publication::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setPublicationFrequency(List<String> publicationFrequency) {
    this.publicationFrequency = publicationFrequency;
    return this;
  }

  public Instance setPublicationRange(List<String> publicationRange) {
    this.publicationRange = publicationRange;
    return this;
  }

  public Instance setElectronicAccess(List<ElectronicAccess> electronicAccess) {
    this.electronicAccess = electronicAccess;
    return this;
  }

  public Instance setElectronicAccess (JsonArray array) {
    this.electronicAccess = array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(ElectronicAccess::new)
      .collect(Collectors.toList())
      : new ArrayList<>();
    return this;
  }

  public Instance setInstanceFormatIds(List<String> instanceFormatIds) {
    this.instanceFormatIds = instanceFormatIds;
    return this;
  }

  public Instance setPhysicalDescriptions(List<String> physicalDescriptions) {
    this.physicalDescriptions = physicalDescriptions;
    return this;
  }

  public Instance setLanguages(List<String> languages) {
    this.languages = languages;
    return this;
  }

  public Instance setNotes(List<Note> notes) {
    this.notes = notes;
    return this;
  }

  public Instance setNotes (JsonArray array) {
    this.notes =  array != null
      ? JsonArrayHelper.toList(array).stream()
      .map(Note::new)
      .collect(Collectors.toList())
      : new ArrayList<>();

    return this;
  }

  public Instance setModeOfIssuanceId(String modeOfIssuanceId) {
    this.modeOfIssuanceId = modeOfIssuanceId;
    return this;
  }

  public Instance setCatalogedDate(String catalogedDate) {
    this.catalogedDate = catalogedDate;
    return this;
  }

  public Instance setPreviouslyHeld(Boolean previouslyHeld) {
    this.previouslyHeld = previouslyHeld;
    return this;
  }

  public Instance setStaffSuppress(Boolean staffSuppress) {
    this.staffSuppress = staffSuppress;
    return this;
  }

  public Instance setDiscoverySuppress(Boolean discoverySuppress) {
    this.discoverySuppress = discoverySuppress;
    return this;
  }

  public Instance setStatisticalCodeIds(List<String> statisticalCodeIds) {
    this.statisticalCodeIds = statisticalCodeIds;
    return this;
  }

  public Instance setSourceRecordFormat(String sourceRecordFormat) {
    this.sourceRecordFormat = sourceRecordFormat;
    return this;
  }

  public Instance setStatusId(String statusId) {
    this.statusId = statusId;
    return this;
  }

  public Instance setStatusUpdatedDate(String statusUpdatedDate) {
    this.statusUpdatedDate = statusUpdatedDate;
    return this;
  }

  public Instance setVersion(String version) {
    this.version = version;
    return this;
  }

  public Instance setMetadata(Metadata metadata) {
    this.metadata = metadata;
    return this;
  }

  public Instance setTags(List<String> tags) {
    this.tags = tags;
    return this;
  }

  public Instance setNatureOfContentTermIds(List<String> natureOfContentTermIds) {
    this.natureOfContentTermIds = natureOfContentTermIds;
    return this;
  }

  public boolean hasMetadata () {
    return this.metadata != null;
  }

  public String getId() {
    return id;
  }

  public String getVersion() {
    return version;
  }

  public String getHrid() {
    return hrid;
  }

  public String getMatchKey() {
    return matchKey;
  }

  public String getSource() {
    return source;
  }

  public List<InstanceRelationshipToParent> getParentInstances() {
    return parentInstances;
  }

  public List<InstanceRelationshipToChild> getChildInstances() {
    return childInstances;
  }

  public List<PrecedingSucceedingTitle> getPrecedingTitles() {
    return Collections.unmodifiableList(precedingTitles);
  }

  public List<PrecedingSucceedingTitle> getSucceedingTitles() {
    return Collections.unmodifiableList(succeedingTitles);
  }

  public boolean getIsBoundWith() {
    return isBoundWith;
  }

  public String getTitle() {
    return title;
  }

  public String getIndexTitle() {
    return indexTitle;
  }

  public List<AlternativeTitle> getAlternativeTitles() {
    return alternativeTitles;
  }

  public List<String> getEditions() {
    return editions;
  }

  public List<String> getSeries() {
    return series;
  }

  public List<Identifier> getIdentifiers() {
    return identifiers;
  }

  public List<Contributor> getContributors() {
    return contributors;
  }

  public List<String> getSubjects() {
    return subjects;
  }

  public List<Classification> getClassifications() {
    return classifications;
  }

  public List<Publication> getPublication() {
    return publication;
  }

  public List<String> getPublicationFrequency() {
    return publicationFrequency;
  }

  public List<String> getPublicationRange() {
    return publicationRange;
  }

  public List<ElectronicAccess> getElectronicAccess() {
    return electronicAccess;
  }

  public String getInstanceTypeId() {
    return instanceTypeId;
  }

  public List<String> getInstanceFormatIds() {
    return instanceFormatIds;
  }

  public List<String> getPhysicalDescriptions() {
    return physicalDescriptions;
  }

  public List<String> getLanguages() {
    return languages;
  }

  public List<Note> getNotes() {
    return notes;
  }


  public String getModeOfIssuanceId() {
    return modeOfIssuanceId;
  }

  public String getCatalogedDate() {
    return catalogedDate;
  }

  public Boolean getPreviouslyHeld() {
    return previouslyHeld;
  }

  public Boolean getStaffSuppress() {
    return staffSuppress;
  }

  public Boolean getDiscoverySuppress() {
    return discoverySuppress;
  }

  public List<String> getStatisticalCodeIds() {
    return statisticalCodeIds;
  }

  public String getSourceRecordFormat() {
    return sourceRecordFormat;
  }

  public String getStatusId() {
    return statusId;
  }

  public String getStatusUpdatedDate() {
    return statusUpdatedDate;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public List<String> getTags() {
    return tags;
  }

  public List<String> getNatureOfContentTermIds() {
    return natureOfContentTermIds;
  }

  public Instance copyWithNewId(String newId) {
    return new Instance(newId, null, null, this.source, this.title, this.instanceTypeId)
            .setIndexTitle(indexTitle)
            .setAlternativeTitles(alternativeTitles)
            .setEditions(editions)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPublicationFrequency(publicationFrequency)
            .setPublicationRange(publicationRange)
            .setElectronicAccess(electronicAccess)
            .setInstanceFormatIds(instanceFormatIds)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setModeOfIssuanceId(modeOfIssuanceId)
            .setCatalogedDate(catalogedDate)
            .setPreviouslyHeld(previouslyHeld)
            .setStaffSuppress(staffSuppress)
            .setDiscoverySuppress(discoverySuppress)
            .setStatisticalCodeIds(statisticalCodeIds)
            .setSourceRecordFormat(sourceRecordFormat)
            .setStatusId(statusId)
            .setStatusUpdatedDate(statusUpdatedDate)
            .setMetadata(metadata)
            .setTags(tags)
            .setNatureOfContentTermIds(natureOfContentTermIds)
            .setPublicationPeriod(publicationPeriod);
  }

  public Instance copyInstance() {
    return new Instance(this.id, this.version, this.hrid, this.source, this.title, this.instanceTypeId)
            .setIndexTitle(indexTitle)
            .setAlternativeTitles(alternativeTitles)
            .setEditions(editions)
            .setSeries(series)
            .setIdentifiers(identifiers)
            .setContributors(contributors)
            .setSubjects(subjects)
            .setClassifications(classifications)
            .setPublication(publication)
            .setPublicationFrequency(publicationFrequency)
            .setPublicationRange(publicationRange)
            .setElectronicAccess(electronicAccess)
            .setInstanceFormatIds(instanceFormatIds)
            .setPhysicalDescriptions(physicalDescriptions)
            .setLanguages(languages)
            .setNotes(notes)
            .setModeOfIssuanceId(modeOfIssuanceId)
            .setCatalogedDate(catalogedDate)
            .setPreviouslyHeld(previouslyHeld)
            .setStaffSuppress(staffSuppress)
            .setDiscoverySuppress(discoverySuppress)
            .setStatisticalCodeIds(statisticalCodeIds)
            .setSourceRecordFormat(sourceRecordFormat)
            .setStatusId(statusId)
            .setStatusUpdatedDate(statusUpdatedDate)
            .setMetadata(metadata)
            .setTags(tags)
            .setNatureOfContentTermIds(natureOfContentTermIds)
            .setPublicationPeriod(publicationPeriod);
  }

  public Instance addIdentifier(Identifier identifier) {
    List<Identifier> newIdentifiers = new ArrayList<>(this.identifiers);

    newIdentifiers.add(identifier);

    return copyInstance().setIdentifiers(newIdentifiers);
  }

  public Instance addIdentifier(String identifierTypeId, String value) {
    Identifier identifier = new Identifier(identifierTypeId, value);

    return addIdentifier(identifier);
  }

  public Instance addContributor(String contributorNameTypeId, String name, String contributorTypeId, String contributorTypeText, Boolean primary) {
    List<Contributor> newContributors = new ArrayList<>(this.contributors);

    newContributors.add(new Contributor(contributorNameTypeId, name, contributorTypeId, contributorTypeText, primary));

    return copyInstance().setContributors(newContributors);
  }

  public Instance removeIdentifier(final String identifierTypeId, final String value) {
    List<Identifier> newIdentifiers = this.identifiers.stream()
      .filter(it -> !(StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value)))
      .collect(Collectors.toList());

    return copyInstance().setIdentifiers(newIdentifiers);
  }

  public Instance setPublicationPeriod(PublicationPeriod period) {
    this.publicationPeriod = period;
    return this;
  }

  @Override
  public String toString() {
    return String.format("Instance ID: %s, HRID: %s, Title: %s", id, hrid, title);
  }

  private static List<String> getTags(JsonObject instanceRequest) {
    if (instanceRequest.containsKey(TAGS_KEY)) {
      try {
        final JsonObject tags = instanceRequest.getJsonObject(TAGS_KEY);
        return tags.containsKey(TAG_LIST_KEY) ?
          JsonArrayHelper.toListOfStrings(tags.getJsonArray(TAG_LIST_KEY)) : new ArrayList<>();
      } catch (ClassCastException e) {
        return JsonArrayHelper.toListOfStrings(instanceRequest.getJsonArray(TAGS_KEY));
      }
    } else {
      return new ArrayList<>();
    }
  }

  private static void putIfNotNull(JsonObject target, String propertyName, String value) {
    if (value != null) {
      target.put(propertyName, value);
    }
  }

  private static void putIfNotNull(JsonObject target, String propertyName, List<String> value) {
    if (value != null) {
      target.put(propertyName, value);
    }
  }

  private static void putIfNotNull(JsonObject target, String propertyName, Object value) {
    if (value != null) {
      if (value instanceof List) {
        target.put(propertyName, value);
      } else if (value instanceof Boolean) {
        target.put(propertyName, value);
      } else {
        target.put(propertyName, new JsonObject(Json.encode(value)));
      }
    }
  }

}
