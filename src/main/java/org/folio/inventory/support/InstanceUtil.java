package org.folio.inventory.support;

import static java.lang.String.format;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ChildInstance;
import org.folio.ParentInstance;
import org.folio.Tags;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.domain.instances.InstanceRelationshipToChild;
import org.folio.inventory.domain.instances.InstanceRelationshipToParent;
import org.folio.inventory.exceptions.NotFoundException;

public class InstanceUtil {
  private static final Logger LOGGER = LogManager.getLogger(InstanceUtil.class);
  private static final String STATISTICAL_CODE_IDS_PROPERTY = "statisticalCodeIds";
  private static final String NATURE_OF_CONTENT_TERM_IDS_PROPERTY = "natureOfContentTermIds";
  private static final String ADMINISTRATIVE_NOTES_PROPERTY = "administrativeNotes";
  private static final String PARENT_INSTANCES_PROPERTY = "parentInstances";
  private static final String CHILDREN_INSTANCES_PROPERTY = "childInstances";



  private InstanceUtil() {}

  /**
   * Merges fields from Instances which are NOT controlled by the underlying SRS MARC
   * @param existing - Instance in DB
   * @param mapped - Instance after mapping
   * @return - result Instance
   */
  public static Instance mergeFieldsWhichAreNotControlled(Instance existing, org.folio.Instance mapped) {
    mapped.setId(existing.getId());
    mapped.setDiscoverySuppress(existing.getDiscoverySuppress());
    mapped.setStaffSuppress(existing.getStaffSuppress());

    List<ParentInstance> parentInstances = constructParentInstancesList(existing);

    List<ChildInstance> childInstances = constructChildInstancesList(existing);

    //Fields which are not affects by default mapping.
    org.folio.Instance tmp = new org.folio.Instance()
      .withId(existing.getId())
      .withVersion(existing.getVersion())
      .withDiscoverySuppress(existing.getDiscoverySuppress())
      .withStaffSuppress(existing.getStaffSuppress())
      .withDeleted(existing.getDeleted())
      .withPreviouslyHeld(existing.getPreviouslyHeld())
      .withCatalogedDate(existing.getCatalogedDate())
      .withStatusId(existing.getStatusId())
      .withStatusUpdatedDate(existing.getStatusUpdatedDate())
      .withStatisticalCodeIds(existing.getStatisticalCodeIds())
      .withNatureOfContentTermIds(existing.getNatureOfContentTermIds())
      .withTags(new Tags().withTagList(existing.getTags()))
      .withAdministrativeNotes(existing.getAdministrativeNotes())
      .withParentInstances(parentInstances)
      .withChildInstances(childInstances);

    JsonObject existingInstanceAsJson = JsonObject.mapFrom(tmp);
    JsonObject mappedInstanceAsJson = JsonObject.mapFrom(mapped);
    JsonObject mergedInstanceAsJson = InstanceUtil.mergeInstances(existingInstanceAsJson, mappedInstanceAsJson);
    return Instance.fromJson(mergedInstanceAsJson);
  }

  private static List<ParentInstance> constructParentInstancesList(Instance existing) {
    List<ParentInstance> parentInstances = new ArrayList<>();
    for (InstanceRelationshipToParent parent : existing.getParentInstances()) {
      ParentInstance parentInstance = new ParentInstance()
        .withId(parent.getId())
        .withSuperInstanceId(parent.getSuperInstanceId())
        .withInstanceRelationshipTypeId(parent.getInstanceRelationshipTypeId());
      parentInstances.add(parentInstance);
    }
    return parentInstances;
  }

  private static List<ChildInstance> constructChildInstancesList(Instance existing) {
    List<ChildInstance> childInstances = new ArrayList<>();
    for (InstanceRelationshipToChild child : existing.getChildInstances()) {
      ChildInstance childInstance = new ChildInstance()
        .withId(child.getId())
        .withSubInstanceId(child.getSubInstanceId())
        .withInstanceRelationshipTypeId(child.getInstanceRelationshipTypeId());
      childInstances.add(childInstance);
    }
    return childInstances;
  }

  public static JsonObject mergeInstances(JsonObject existing, JsonObject mapped) {
    //Statistical code, nature of content terms, administrative notes, parent/childInstances don`t revealed via mergeIn() because of simple array type.
    JsonArray statisticalCodeIds = existing.getJsonArray(STATISTICAL_CODE_IDS_PROPERTY);
    JsonArray natureOfContentTermIds = existing.getJsonArray(NATURE_OF_CONTENT_TERM_IDS_PROPERTY);
    JsonArray administrativeNotes = existing.getJsonArray(ADMINISTRATIVE_NOTES_PROPERTY);
    JsonArray parents = existing.getJsonArray(PARENT_INSTANCES_PROPERTY);
    JsonArray children = existing.getJsonArray(CHILDREN_INSTANCES_PROPERTY);
    JsonObject mergedInstanceAsJson = existing.mergeIn(mapped);
    mergedInstanceAsJson.put(STATISTICAL_CODE_IDS_PROPERTY, statisticalCodeIds);
    mergedInstanceAsJson.put(NATURE_OF_CONTENT_TERM_IDS_PROPERTY, natureOfContentTermIds);
    mergedInstanceAsJson.put(ADMINISTRATIVE_NOTES_PROPERTY, administrativeNotes);
    mergedInstanceAsJson.put(PARENT_INSTANCES_PROPERTY, parents);
    mergedInstanceAsJson.put(CHILDREN_INSTANCES_PROPERTY, children);
    return mergedInstanceAsJson;
  }

  public static Future<Instance> findInstanceById(String instanceId, InstanceCollection instanceCollection) {
    Promise<Instance> promise = Promise.promise();
    instanceCollection.findById(instanceId, success -> {
        if (success.getResult() == null) {
          LOGGER.warn("findInstanceById:: Can't find Instance by id: {} ", instanceId);
          promise.fail(new NotFoundException(format("Can't find Instance by id: %s", instanceId)));
        } else {
          promise.complete(success.getResult());
        }
      },
      failure -> {
        LOGGER.warn(format("findInstanceById:: Error retrieving Instance by id %s - %s, status code %s", instanceId, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
  }
}
