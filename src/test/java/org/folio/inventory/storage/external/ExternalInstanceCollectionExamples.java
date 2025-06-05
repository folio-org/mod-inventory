package org.folio.inventory.storage.external;

import static api.ApiTestSuite.REQUEST_ID;
import static api.ApiTestSuite.USER_ID;
import static org.folio.inventory.common.FutureAssistance.fail;
import static org.folio.inventory.common.FutureAssistance.getOnCompletion;
import static org.folio.inventory.common.FutureAssistance.succeed;
import static org.folio.inventory.common.FutureAssistance.waitForCompletion;
import static org.folio.inventory.domain.instances.InstanceSource.LINKED_DATA;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import lombok.SneakyThrows;

public class ExternalInstanceCollectionExamples extends ExternalStorageTests {
  private static final String BOOKS_INSTANCE_TYPE = UUID.randomUUID().toString();
  private static final String PERSONAL_CONTRIBUTOR_NAME_TYPE = UUID.randomUUID().toString();
  private static final String AUTHOR_CONTRIBUTOR_TYPE = UUID.randomUUID().toString();
  private static final String ISBN_IDENTIFIER_TYPE = UUID.randomUUID().toString();
  private static final String ASIN_IDENTIFIER_TYPE = UUID.randomUUID().toString();

  private final InstanceCollection collection =
    useHttpClient(client -> new ExternalStorageModuleInstanceCollection(
      getStorageAddress(), TENANT_ID, TENANT_TOKEN, USER_ID, REQUEST_ID, client));

  @Before
  @SneakyThrows
  public void before() {
    CompletableFuture<Void> emptied = new CompletableFuture<Void>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  @SneakyThrows
  public void canBeEmptied() {
    addSomeExamples(collection);

    CompletableFuture<Void> emptied = new CompletableFuture<>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);

    CompletableFuture<MultipleRecords<Instance>> findFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findAll(PagingParameters.defaults(),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Instance> allInstancesWrapped = getOnCompletion(findFuture);

    List<Instance> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(0));
    assertThat(allInstancesWrapped.totalRecords, is(0));
  }

  @Test
  public void canBeEmptiedByCql() {
    var instance1 = createInstance("foo");
    var instance2 = createInstance("bar");
    addInstance(instance1);
    addInstance(instance2);
    assertThat(getSize(), is(2));
    empty("id==" + instance1.getId());
    assertThat(getSize(), is(1));
    empty("id==" + instance1.getId());
    assertThat(getSize(), is(1));
    empty("id==" + instance2.getId());
    assertThat(getSize(), is(0));
  }

  @Test
  public void cannotEmptyIfCqlIsMissing() {
    var e = assertThrows(ExecutionException.class, () -> empty(null));
    assertThat(e.getCause().getMessage(), is("query parameter is required"));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeAdded() {
    addSomeExamples(collection);

    CompletableFuture<MultipleRecords<Instance>> findFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findAll(PagingParameters.defaults(), succeed(findFuture), fail(findFuture));

    MultipleRecords<Instance> allInstancesWrapped = getOnCompletion(findFuture);

    List<Instance> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(3));
    assertThat(allInstancesWrapped.totalRecords, is(3));

    Instance createdAngryPlanet = getInstance(allInstances, "Long Way to a Small Angry Planet");
    Instance createdNod = getInstance(allInstances, "Nod");
    Instance createdUprooted = getInstance(allInstances, "Uprooted");

    assertThat(hasIdentifier(createdAngryPlanet, ISBN_IDENTIFIER_TYPE, "9781473619777"), is(true));
    assertThat(hasIdentifier(createdNod, ASIN_IDENTIFIER_TYPE, "B01D1PLMDO"), is(true));
    assertThat(hasIdentifier(createdUprooted, ISBN_IDENTIFIER_TYPE, "1447294149"), is(true));
    assertThat(hasIdentifier(createdUprooted, ISBN_IDENTIFIER_TYPE, "9781447294146"), is(true));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeAddedWithAnId() {
    CompletableFuture<Instance> addFinished = new CompletableFuture<>();

    String instanceId = UUID.randomUUID().toString();

    Instance instanceWithId = smallAngryPlanet().copyWithNewId(instanceId);

    collection.add(instanceWithId, succeed(addFinished), fail(addFinished));

    Instance added = getOnCompletion(addFinished);

    assertThat(added.getId(), is(instanceId));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeFoundById() {
    CompletableFuture<Instance> firstAddFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> secondAddFuture = new CompletableFuture<Instance>();

    collection.add(smallAngryPlanet(), succeed(firstAddFuture), fail(firstAddFuture));
    collection.add(nod(), succeed(secondAddFuture), fail(secondAddFuture));

    Instance addedInstance = getOnCompletion(firstAddFuture);
    Instance otherAddedInstance = getOnCompletion(secondAddFuture);

    CompletableFuture<Instance> findFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> otherFindFuture = new CompletableFuture<Instance>();

    collection.findById(addedInstance.getId(), succeed(findFuture), fail(findFuture));
    collection.findById(otherAddedInstance.getId(), succeed(otherFindFuture), fail(otherFindFuture));

    Instance foundSmallAngry = getOnCompletion(findFuture);
    Instance foundNod = getOnCompletion(otherFindFuture);

    assertThat(foundSmallAngry.getTitle(), is("Long Way to a Small Angry Planet"));

    assertThat(foundNod.getTitle(), is("Nod"));

    assertThat(hasIdentifier(foundSmallAngry, ISBN_IDENTIFIER_TYPE, "9781473619777"), is(true));
    assertThat(hasIdentifier(foundNod, ASIN_IDENTIFIER_TYPE, "B01D1PLMDO"), is(true));
  }

  @Test
  @SneakyThrows
  public void allInstancesCanBePaged() {
    WaitForAllFutures<Instance> allAdded = new WaitForAllFutures<>();

    collection.add(smallAngryPlanet(), allAdded.notifySuccess(), v -> {});
    collection.add(nod(), allAdded.notifySuccess(), v -> {});
    collection.add(uprooted(), allAdded.notifySuccess(), v -> {});
    collection.add(temeraire(), allAdded.notifySuccess(), v -> {});
    collection.add(interestingTimes(), allAdded.notifySuccess(), v -> {});

    allAdded.waitForCompletion();

    CompletableFuture<MultipleRecords<Instance>> firstPageFuture = new CompletableFuture<MultipleRecords<Instance>>();
    CompletableFuture<MultipleRecords<Instance>> secondPageFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findAll(new PagingParameters(3, 0), succeed(firstPageFuture), fail(firstPageFuture));
    collection.findAll(new PagingParameters(3, 3), succeed(secondPageFuture), fail(secondPageFuture));

    MultipleRecords<Instance> firstPage = getOnCompletion(firstPageFuture);
    MultipleRecords<Instance> secondPage = getOnCompletion(secondPageFuture);

    List<Instance> firstPageInstances = firstPage.records;
    List<Instance> secondPageInstances = secondPage.records;

    assertThat(firstPageInstances.size(), is(3));
    assertThat(secondPageInstances.size(), is(2));

    assertThat(firstPage.totalRecords, is(5));
    assertThat(secondPage.totalRecords, is(5));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeDeleted() {
    addSomeExamples(collection);

    CompletableFuture<Instance> instanceToBeDeletedFuture = new CompletableFuture<Instance>();

    collection.add(temeraire(), succeed(instanceToBeDeletedFuture), fail(instanceToBeDeletedFuture));

    Instance instanceToBeDeleted = instanceToBeDeletedFuture.get();

    final var deleted = new CompletableFuture<Void>();

    collection.delete(instanceToBeDeleted.getId(), succeed(deleted), fail(deleted));

    waitForCompletion(deleted);

    CompletableFuture<Instance> findFuture = new CompletableFuture<Instance>();

    collection.findById(instanceToBeDeleted.getId(), succeed(findFuture), fail(findFuture));

    assertThat(findFuture.get(), is(CoreMatchers.nullValue()));

    CompletableFuture<MultipleRecords<Instance>> findAllFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture), fail(findAllFuture));

    MultipleRecords<Instance> allInstancesWrapped = getOnCompletion(findAllFuture);

    List<Instance> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(3));
    assertThat(allInstancesWrapped.totalRecords, is(3));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeUpdated() {
    CompletableFuture<Instance> addFinished = new CompletableFuture<Instance>();

    collection.add(smallAngryPlanet(), succeed(addFinished), fail(addFinished));

    Instance added = getOnCompletion(addFinished);

    CompletableFuture<Void> updateFinished = new CompletableFuture<>();

    Instance changed = added.removeIdentifier(ISBN_IDENTIFIER_TYPE, "9781473619777");

    collection.update(changed, succeed(updateFinished), fail(updateFinished));

    waitForCompletion(updateFinished);

    CompletableFuture<Instance> gotUpdated = new CompletableFuture<Instance>();

    collection.findById(added.getId(), succeed(gotUpdated), fail(gotUpdated));

    Instance updated = getOnCompletion(gotUpdated);

    assertThat(updated.getId(), is(added.getId()));
    assertThat(updated.getTitle(), is(added.getTitle()));
    assertThat(updated.getIdentifiers().size(), is(0));
  }

  @Test
  @SneakyThrows
  public void instancesCanBeFoundByByPartialName() {
    CompletableFuture<Instance> firstAddFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> secondAddFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> thirdAddFuture = new CompletableFuture<Instance>();

    collection.add(smallAngryPlanet(), succeed(firstAddFuture), fail(firstAddFuture));
    collection.add(nod(), succeed(secondAddFuture), fail(secondAddFuture));
    collection.add(uprooted(), succeed(thirdAddFuture), fail(thirdAddFuture));

    CompletableFuture<Void> allAddsFuture = CompletableFuture.allOf(secondAddFuture, thirdAddFuture);

    getOnCompletion(allAddsFuture);

    Instance addedSmallAngryPlanet = getOnCompletion(firstAddFuture);

    CompletableFuture<MultipleRecords<Instance>> findFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findByCql("title=\"*Small Angry*\"", new PagingParameters(10, 0),
      succeed(findFuture), fail(findFuture));

    MultipleRecords<Instance> findByNameResultsWrapped = getOnCompletion(findFuture);

    List<Instance> findByNameResults = findByNameResultsWrapped.records;

    assertThat(findByNameResults.size(), is(1));
    assertThat(findByNameResultsWrapped.totalRecords, is(1));

    assertThat(findByNameResults.get(0).getId(), is(addedSmallAngryPlanet.getId()));
  }

  @Test
  @SneakyThrows
  public void anInstanceCanBeUpdatedByIdRetainingSourceOfExistingInstance() {
    String expectedTitle = "Updated title";
    CompletableFuture<Instance> addFinished = new CompletableFuture<>();

    collection.add(sourceLinkDataInstance(), succeed(addFinished), fail(addFinished));
    Instance addedInstance = getOnCompletion(addFinished);
    assertThat(addedInstance.getSource(), is(LINKED_DATA.getValue()));

    JsonObject changedInstanceJson = addedInstance.getJsonForStorage();
    changedInstanceJson.put(Instance.SOURCE_KEY, MARC.getValue());
    changedInstanceJson.put(Instance.TITLE_KEY, expectedTitle);

    Context context = EventHandlingUtil.constructContext(
      TENANT_ID, TENANT_TOKEN, getStorageAddress(), USER_ID, REQUEST_ID);
    collection.findByIdAndUpdate(addedInstance.getId(), changedInstanceJson, context);

    CompletableFuture<Instance> gotUpdated = new CompletableFuture<>();
    collection.findById(addedInstance.getId(), succeed(gotUpdated), fail(gotUpdated));
    Instance updated = getOnCompletion(gotUpdated);

    assertThat(updated.getId(), is(addedInstance.getId()));
    assertThat(updated.getTitle(), is(expectedTitle));
    assertThat(updated.getSource(), is(addedInstance.getSource()));
  }

  @SneakyThrows
  private static void addSomeExamples(InstanceCollection instanceCollection) {
    WaitForAllFutures<Instance> allAdded = new WaitForAllFutures<>();

    instanceCollection.add(smallAngryPlanet(), allAdded.notifySuccess(), v -> {});
    instanceCollection.add(nod(), allAdded.notifySuccess(), v -> {});
    instanceCollection.add(uprooted(), allAdded.notifySuccess(), v -> {});

    allAdded.waitForCompletion();
  }

  private static Instance nod() {
    return createInstance("Nod")
      .addIdentifier(ASIN_IDENTIFIER_TYPE, "B01D1PLMDO")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Barnes, Adrian", AUTHOR_CONTRIBUTOR_TYPE, "", "", null);
  }

  private static Instance uprooted() {
    return createInstance("Uprooted")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "1447294149")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9781447294146")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Novik, Naomi", AUTHOR_CONTRIBUTOR_TYPE, "", "", null);
  }

  private static Instance smallAngryPlanet() {
    return createInstance("Long Way to a Small Angry Planet")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9781473619777")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Chambers, Becky", AUTHOR_CONTRIBUTOR_TYPE, "", "", null);
  }

  private static Instance temeraire() {
    return createInstance("Temeraire")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "0007258712")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9780007258710")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Novik, Naomi", AUTHOR_CONTRIBUTOR_TYPE, "", "", null);
  }

  private static Instance interestingTimes() {
    return createInstance("Interesting Times")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "0552167541")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9780552167543")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Pratchett, Terry", AUTHOR_CONTRIBUTOR_TYPE, "", "", null);
  }

  private static Instance sourceLinkDataInstance() {
    return new Instance(
      UUID.randomUUID().toString(),
      null,
      null,
      LINKED_DATA.getValue(),
      "Temeraire",
      BOOKS_INSTANCE_TYPE)
      .setAlternativeTitles(new ArrayList<>())
      .setIdentifiers(new ArrayList<>())
      .setContributors(new ArrayList<>());
  }

  private static Instance createInstance(String title) {
    return new Instance(
            UUID.randomUUID().toString(),
            null,
            null,
            "local",
            title,
            BOOKS_INSTANCE_TYPE)
                .setAlternativeTitles(new ArrayList<>())
                .setIdentifiers(new ArrayList<>())
                .setContributors(new ArrayList<>());
  }

  @SneakyThrows
  private void addInstance(Instance instance) {
    waitForCompletion((CompletableFuture<Instance> add) -> collection.add(instance, succeed(add), fail(add)));
  }

  @SneakyThrows
  private void empty(String cqlQuery) {
    waitForCompletion((CompletableFuture<Void> emptied) ->
        collection.empty(cqlQuery, succeed(emptied), fail(emptied)));
  }

  @SneakyThrows
  private int getSize() {
    return getOnCompletion((CompletableFuture<MultipleRecords<Instance>> find) ->
        collection.findAll(PagingParameters.defaults(), succeed(find), fail(find)))
        .records.size();
  }

  private static boolean hasIdentifier(
    Instance instance,
    final String identifierTypeId,
    final String value) {

    return instance.getIdentifiers().stream().anyMatch(it ->
      StringUtils.equals(it.identifierTypeId, identifierTypeId)
        && StringUtils.equals(it.value, value));
  }

  private Instance getInstance(List<Instance> allInstances, final String title) {
    return allInstances.stream()
      .filter(it -> StringUtils.equals(it.getTitle(), title))
      .findFirst().orElse(null);
  }
}
