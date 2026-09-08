package org.folio.inventory.dataimport.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MarcField;
import org.folio.rest.jaxrs.model.MarcMappingDetail;
import org.folio.rest.jaxrs.model.MarcSubfield;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class DeleteRuleFor999FieldCacheTest {

  private static DeleteRuleFor999FieldCache cache;

  @BeforeAll
  static void beforeClass(Vertx vertx) {
    cache = DeleteRuleFor999FieldCache.getInstance(vertx);
  }

  @Test
  void shouldReturnFalseForNullProfile() {
    assertFalse(check(null));
  }

  @Test
  void shouldReturnFalseWhenNoMappingDetails() {
    MappingProfile p = new MappingProfile().withId(UUID.randomUUID().toString());
    assertFalse(check(p));
  }

  @Test
  void shouldReturnFalseWhenNoDeleteRuleFor999() {
    MarcMappingDetail addRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.ADD)
      .withField(new MarcField().withField("856")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("u"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(addRule)));
    assertFalse(check(p));
  }

  @Test
  void shouldReturnTrueWhenDeleteRuleFor999Present() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(check(p));
  }

  @Test
  void shouldReturnTrueWhenMixedRulesIncludeDelete999() {
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
    assertTrue(check(p));
  }

  @Test
  void shouldReturnFalseWhenDeleteRuleTargetsOtherField() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("856")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertFalse(check(p));
  }

  @Test
  void shouldMemoizeResultByProfileId() {
    String profileId = UUID.randomUUID().toString();
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p1 = new MappingProfile()
      .withId(profileId)
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(check(p1));

    // Different profile object with the same id but with no delete-999 rule.
    // Cache must return the memoized value (true) - proves that value is keyed by profile id.
    MappingProfile p2 = new MappingProfile()
      .withId(profileId)
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.emptyList()));
    assertTrue(check(p2), "memoized value should be returned for the same profile id");
  }

  @Test
  void shouldNotCacheWhenProfileIdIsBlank() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Collections.singletonList(new MarcSubfield().withSubfield("*"))));
    MappingProfile p = new MappingProfile()
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    // profileId is null -> compute directly, no caching
    assertTrue(check(p));
  }

  @Test
  void singletonAccessorReturnsSameInstance(Vertx vertx) {
    DeleteRuleFor999FieldCache a = DeleteRuleFor999FieldCache.getInstance(vertx);
    DeleteRuleFor999FieldCache b = DeleteRuleFor999FieldCache.getInstance(vertx);
    assertNotNull(a);
    assertSame(a, b);
  }

  @Test
  void shouldReturnTrueForDelete999WithSubfieldI() {
    assertTrue(check(buildDelete999Profile("i")));
  }

  @Test
  void shouldReturnTrueForDelete999WithSubfieldS() {
    assertTrue(check(buildDelete999Profile("s")));
  }

  @Test
  void shouldReturnFalseForDelete999WhenSubfieldIsOtherThanIsOrWildcard() {
    assertFalse(check(buildDelete999Profile("l")));
  }

  @Test
  void shouldReturnFalseForDelete999WhenSubfieldIsT() {
    assertFalse(check(buildDelete999Profile("t")));
  }

  @Test
  void shouldReturnTrueForDelete999WhenAtLeastOneRelevantSubfieldAmongMany() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Arrays.asList(
          new MarcSubfield().withSubfield("l"),
          new MarcSubfield().withSubfield("i"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(check(p));
  }

  @Test
  void shouldReturnFalseWhenAllSubfieldsAreIrrelevant() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999")
        .withSubfields(Arrays.asList(
          new MarcSubfield().withSubfield("l"),
          new MarcSubfield().withSubfield("t"))));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertFalse(check(p));
  }

  @Test
  void shouldReturnTrueForDelete999WhenSubfieldsListIsNull() {
    MarcMappingDetail deleteRule = new MarcMappingDetail()
      .withAction(MarcMappingDetail.Action.DELETE)
      .withField(new MarcField().withField("999"));
    MappingProfile p = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withMappingDetails(new MappingDetail().withMarcMappingDetails(Collections.singletonList(deleteRule)));
    assertTrue(check(p));
  }

  private static boolean check(MappingProfile profile) {
    return cache.containsDeleteRuleFor999Field(profile);
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
}
