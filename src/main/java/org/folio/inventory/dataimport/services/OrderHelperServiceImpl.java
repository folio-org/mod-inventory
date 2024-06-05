package org.folio.inventory.dataimport.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.DataImportEventTypes;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ProfileType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.processing.events.EventManager.POST_PROCESSING_INDICATOR;

public class OrderHelperServiceImpl implements OrderHelperService {
  private static final Logger LOGGER = LogManager.getLogger();
  public static final String JOB_PROFILE_SNAPSHOT_ID = "JOB_PROFILE_SNAPSHOT_ID";
  public static final String ORDER_TYPE = "ORDER";
  public static final String FOLIO_RECORD = "folioRecord";
  public static final String ACTION_FIELD = "action";
  public static final String CREATE_ACTION = "CREATE";
  private final ProfileSnapshotCache profileSnapshotCache;

  public OrderHelperServiceImpl(ProfileSnapshotCache profileSnapshotCache) {
    this.profileSnapshotCache = profileSnapshotCache;
  }

  public Future<Void> fillPayloadForOrderPostProcessingIfNeeded(DataImportEventPayload eventPayload, DataImportEventTypes targetEventType, Context context) {
    Promise<Void> promise = Promise.promise();
    String jobProfileSnapshotId = eventPayload.getContext().get(JOB_PROFILE_SNAPSHOT_ID);
    profileSnapshotCache.get(jobProfileSnapshotId, context)
      .toCompletionStage()
      .thenCompose(snapshotOptional -> snapshotOptional
        .map(profileSnapshot -> checkIfOrderLogicExistsAndFillPayloadIfNeeded(eventPayload, targetEventType, profileSnapshot))
        .orElse(CompletableFuture.failedFuture((new EventProcessingException(format("Job profile snapshot with id '%s' does not exist", eventPayload.getContext().get("JOB_PROFILE_SNAPSHOT_ID")))))))
      .whenComplete((processed, throwable) -> {
        if (throwable != null) {
          promise.fail(throwable);
          LOGGER.error(throwable.getMessage());
        } else {
          promise.complete();
          LOGGER.debug(format("Job profile snapshot with id '%s' was retrieved from cache", jobProfileSnapshotId));
        }
      });
    return promise.future();
  }

  private CompletableFuture<Void> checkIfOrderLogicExistsAndFillPayloadIfNeeded(DataImportEventPayload eventPayload, DataImportEventTypes targetEventType, ProfileSnapshotWrapper profileSnapshotWrapper) {
    List<ProfileSnapshotWrapper> actionProfiles = profileSnapshotWrapper
      .getChildSnapshotWrappers()
      .stream()
      .filter(e -> e.getContentType() == ProfileType.ACTION_PROFILE)
      .collect(Collectors.toList());

    if (!actionProfiles.isEmpty() && checkIfOrderActionProfileExists(actionProfiles) && checkIfCurrentProfileIsTheLastOne(eventPayload, actionProfiles)) {
      if (!eventPayload.getEventsChain().contains(targetEventType.value())) {
        eventPayload.getEventsChain().add(targetEventType.value());
      }

      eventPayload.getContext().put(POST_PROCESSING_INDICATOR, Boolean.TRUE.toString());
      eventPayload.setEventType(DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value());
    }
    return CompletableFuture.completedFuture(null);
  }

  private static boolean checkIfCurrentProfileIsTheLastOne(DataImportEventPayload eventPayload, List<ProfileSnapshotWrapper> actionProfiles) {
    String currentMappingProfileId = eventPayload.getCurrentNode().getProfileId();
    ProfileSnapshotWrapper lastActionProfile = actionProfiles.get(actionProfiles.size() - 1);
    List<ProfileSnapshotWrapper> childSnapshotWrappers = lastActionProfile.getChildSnapshotWrappers();
    String mappingProfileId = StringUtils.EMPTY;
    if (childSnapshotWrappers != null && childSnapshotWrappers.get(0) != null && Objects.equals(childSnapshotWrappers.get(0).getContentType().value(), "MAPPING_PROFILE")) {
      mappingProfileId = childSnapshotWrappers.get(0).getProfileId();
    }
    return mappingProfileId.equals(currentMappingProfileId);
  }

  private static boolean checkIfOrderActionProfileExists(List<ProfileSnapshotWrapper> actionProfiles) {
    for (ProfileSnapshotWrapper actionProfile : actionProfiles) {
      LinkedHashMap<String, String> content = new ObjectMapper().convertValue(actionProfile.getContent(), LinkedHashMap.class);
      if (content.get(FOLIO_RECORD).equals(ORDER_TYPE) && content.get(ACTION_FIELD).equals(CREATE_ACTION)) {
        return true;
      }
    }
    return false;
  }
}
