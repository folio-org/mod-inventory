package org.folio.inventory.dataimport.cache;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.Test;

public class DeleteRuleFor999FieldCacheTest {

  private final DeleteRuleFor999FieldCache cache = new DeleteRuleFor999FieldCache(60);

  @Test
  public void shouldReturnFalseForNullProfile() {
    assertFalse(cache.containsDeleteRuleFor999Field(null));
  }

  @Test
  public void shouldReturnFalseWhenNoMappingDetails() {
    MappingProfile p = new MappingProfile().withId(UUID.randomUUID().toString());
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnFalseWhenNoDeleteRuleFor999() {
    MarcMappingDetail addRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField().withField("856")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("u"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(addRule)));
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueWhenDeleteRuleFor999Present() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueWhenMixedRulesIncludeDelete999() {
    MarcMappingDetail addRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField().withField("856")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("u"))));
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Arrays.asList(addRule, deleteRule)));
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnFalseWhenDeleteRuleTargetsOtherField() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("856")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldMemoizeResultByProfileId() {
    String profileId = UUID.randomUUID().toString();
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p1 = new MappingProfile()
      .withId(profileId)
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(cache.containsDeleteRuleFor999Field(p1));

    // Different profile object with the same id but with no delete-999 rule.
    // Cache must return the memoized value (true) - proves that value is keyed by profile id.
    MappingProfile p2 = new MappingProfile()
      .withId(profileId)
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.emptyList()));
    assertTrue("memoized value should be returned for the same profile id",
      cache.containsDeleteRuleFor999Field(p2));
  }

  @Test
  public void shouldNotCacheWhenProfileIdIsBlank() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    // profileId is null -> compute directly, no caching
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueForDelete999WithSubfieldI() {
    MappingProfile p = buildDelete999Profile("i");
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueForDelete999WithSubfieldS() {
    MappingProfile p = buildDelete999Profile("s");
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnFalseForDelete999WhenSubfieldIsOtherThanIsOrWildcard() {
    // e.g. profile deletes only 999$l - has nothing to do with externalIdsHolder
    MappingProfile p = buildDelete999Profile("l");
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnFalseForDelete999WhenSubfieldIsT() {
    MappingProfile p = buildDelete999Profile("t");
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueForDelete999WhenAtLeastOneRelevantSubfieldAmongMany() {
    // Rule targeting $l AND $i - $i triggers sync even though $l is irrelevant
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Arrays.asList(
          new MarcSubfield().withSubfield("l"),
          new MarcSubfield().withSubfield("i"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnFalseWhenAllSubfieldsAreIrrelevant() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Arrays.asList(
          new MarcSubfield().withSubfield("l"),
          new MarcSubfield().withSubfield("t"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertFalse(cache.containsDeleteRuleFor999Field(p));
  }

  @Test
  public void shouldReturnTrueForDelete999WhenSubfieldsListIsNull() {
    // No subfields specified - treated as whole-field wildcard
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999"));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(cache.containsDeleteRuleFor999Field(p));
  }

  private static MappingProfile buildDelete999Profile(String subfieldCode) {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield(subfieldCode))));
    return new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
  }

  @Test
  public void singletonAccessorReturnsSameInstance() {
    assertTrue(DeleteRuleFor999FieldCache.getInstance() == DeleteRuleFor999FieldCache.getInstance());
  }
}
