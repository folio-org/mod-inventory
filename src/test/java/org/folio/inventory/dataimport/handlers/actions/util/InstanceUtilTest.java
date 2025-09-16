package org.folio.inventory.dataimport.handlers.actions.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.folio.AlternativeTitle;
import org.folio.Contributor;
import org.folio.Instance;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.support.InstanceUtil;
import org.junit.Test;

public class InstanceUtilTest {

  @Test
  public void shouldMergeInstances() {
    Set<AlternativeTitle> alternativeTitles = new HashSet<>();
    alternativeTitles.add(new AlternativeTitle()
      .withAlternativeTitle("alt1")
      .withAlternativeTitleTypeId("30773a27-b485-4dab-aeb6-b8c04fa3cb19"));

    List<Contributor> contributors = new ArrayList<>();
    contributors.add(new Contributor()
      .withName("contributor1")
      .withContributorNameTypeId("30773a27-b485-4dab-aeb6-b8c04fa3cb19")
      .withContributorTypeId("30773a27-b485-4dab-aeb6-b8c04fa3cb19")
      .withContributorTypeText("text")
      .withPrimary(true));

    Instance mapped = new Instance()
      .withId("30773a27-b485-4dab-aeb6-b8c04fa3cb17")
      .withHrid("in000000001")
      .withModeOfIssuanceId("30773a27-b485-4dab-aeb6-b8c04fa3cb18")
      .withAlternativeTitles(alternativeTitles)
      .withDiscoverySuppress(false)
      .withDeleted(false)
      .withStaffSuppress(false)
      .withContributors(contributors);

    List<String> statisticalCodeIds = new ArrayList<>();
    statisticalCodeIds.add("30773a27-b485-4dab-aeb6-b8c04fa3cb19");
    statisticalCodeIds.add("30773a27-b485-4dab-aeb6-b8c04fa3cb18");

    List<String> natureOfContentTermIds = new ArrayList<>();
    natureOfContentTermIds.add("30773a27-b485-4dab-aeb6-b8c04fa3cb21");
    natureOfContentTermIds.add("30773a27-b485-4dab-aeb6-b8c04fa3cb22");

    List<InstanceRelationshipToParent> parents = new ArrayList<>();
    parents.add(new InstanceRelationshipToParent("30773a27-b485-4dab-aeb6-b8c04fa3cb19", "30773a27-b485-4dab-aeb6-b8c04fa3cb23", "30773a27-b485-4dab-aeb6-b8c04fa3cb24"));

    List<InstanceRelationshipToChild> children = new ArrayList<>();
    children.add(new InstanceRelationshipToChild("30773a27-b485-4dab-aeb6-b8c04fa3cb19", "30773a27-b485-4dab-aeb6-b8c04fa3cb23", "30773a27-b485-4dab-aeb6-b8c04fa3cb24"));

    List<String> tagList = new ArrayList<>();
    tagList.add("Tag1");
    tagList.add("Tag2");

    org.folio.inventory.domain.instances.Instance existing =
      new org.folio.inventory.domain.instances.Instance("30773a27-b485-4dab-aeb6-b8c04fa3cb17", 7, "in000000001",
          "source", "title", "30773a27-b485-4dab-aeb6-b8c04fa3cb19");
    existing.setStatisticalCodeIds(statisticalCodeIds);
    existing.setDiscoverySuppress(true);
    existing.setStaffSuppress(true);
    existing.setDeleted(true);
    existing.setPreviouslyHeld(true);
    existing.setCatalogedDate("");
    existing.setStatusId("30773a27-b485-4dab-aeb6-b8c04fa3cb26");
    existing.setStatusUpdatedDate("2022-05-17T13:48:42.559+0000");
    existing.setNatureOfContentTermIds(natureOfContentTermIds);
    existing.setParentInstances(parents);
    existing.setChildInstances(children);
    existing.setTags(tagList);
    existing.setAdministrativeNotes(Lists.newArrayList("Adm note1", "Adm note2"));

    org.folio.inventory.domain.instances.Instance instance = InstanceUtil.mergeFieldsWhichAreNotControlled(existing, mapped);
    assertEquals("30773a27-b485-4dab-aeb6-b8c04fa3cb17", instance.getId());
    assertEquals("in000000001", instance.getHrid());
    assertEquals("30773a27-b485-4dab-aeb6-b8c04fa3cb18", instance.getModeOfIssuanceId());
    assertEquals(contributors.get(0).getName(), instance.getContributors().get(0).name);


    assertEquals(statisticalCodeIds, instance.getStatisticalCodeIds());
    assertTrue(instance.getDiscoverySuppress());
    assertTrue(instance.getStaffSuppress());
    assertFalse(instance.getDeleted());
    assertTrue(instance.getPreviouslyHeld());
    assertEquals("", instance.getCatalogedDate());
    assertEquals("30773a27-b485-4dab-aeb6-b8c04fa3cb26", instance.getStatusId());
    assertEquals("2022-05-17T13:48:42.559+0000", instance.getStatusUpdatedDate());
    assertEquals(natureOfContentTermIds, instance.getNatureOfContentTermIds());
    assertNotNull(instance.getTags());
    assertEquals(tagList, instance.getTags());
    assertEquals("30773a27-b485-4dab-aeb6-b8c04fa3cb19", instance.getParentInstances().get(0).getId());
    assertEquals("30773a27-b485-4dab-aeb6-b8c04fa3cb19", instance.getChildInstances().get(0).getId());
    assertEquals("Adm note1", instance.getAdministrativeNotes().get(0));
    assertEquals("Adm note2", instance.getAdministrativeNotes().get(1));
  }

  @Test
  public void mergeFieldsWithNullVersion() {
    org.folio.inventory.domain.instances.Instance existing =
      new org.folio.inventory.domain.instances.Instance("id", null, "IN00001", "source", "title", "instanceTypeId");

    org.folio.Instance mapped = new org.folio.Instance().withVersion(3);
    org.folio.inventory.domain.instances.Instance merged = InstanceUtil.mergeFieldsWhichAreNotControlled(existing, mapped);
    assertEquals("id", merged.getId());
    assertEquals(3, (int) merged.getVersion());
  }
}
