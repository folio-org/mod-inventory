package org.folio.inventory.storage.external;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.WaitForAllFutures;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.domain.Instance;
import org.folio.inventory.domain.InstanceCollection;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.folio.inventory.common.FutureAssistance.*;
import org.folio.inventory.domain.Identifier;
import static org.folio.inventory.storage.external.ExternalStorageSuite.getStorageAddress;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExternalInstanceCollectionExamples {
  private static final String BOOKS_INSTANCE_TYPE = UUID.randomUUID().toString();
  private static final String PERSONAL_CONTRIBUTOR_NAME_TYPE = UUID.randomUUID().toString();
  private static final String AUTHOR_CONTRIBUTOR_TYPE = UUID.randomUUID().toString();
  private static final String ISBN_IDENTIFIER_TYPE = UUID.randomUUID().toString();
  private static final String ASIN_IDENTIFIER_TYPE = UUID.randomUUID().toString();

  private final InstanceCollection collection =
    ExternalStorageSuite.useVertx(
      it -> new ExternalStorageModuleInstanceCollection(it, getStorageAddress(),
        ExternalStorageSuite.TENANT_ID, ExternalStorageSuite.TENANT_TOKEN));

  @Before
  public void before()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Void> emptied = new CompletableFuture<Void>();

    collection.empty(succeed(emptied), fail(emptied));

    waitForCompletion(emptied);
  }

  @Test
  public void canBeEmptied()
    throws InterruptedException, ExecutionException, TimeoutException {

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
  public void anInstanceCanBeAdded()
    throws InterruptedException, ExecutionException, TimeoutException {

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
  public void anInstanceCanBeAddedWithAnId()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Instance> addFinished = new CompletableFuture<>();

    String instanceId = UUID.randomUUID().toString();

    Instance instanceWithId = smallAngryPlanet().copyWithNewId(instanceId);

    collection.add(instanceWithId, succeed(addFinished), fail(addFinished));

    Instance added = getOnCompletion(addFinished);

    assertThat(added.id, is(instanceId));
  }

  @Test
  public void anInstanceCanBeFoundById()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Instance> firstAddFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> secondAddFuture = new CompletableFuture<Instance>();

    collection.add(smallAngryPlanet(), succeed(firstAddFuture), fail(firstAddFuture));
    collection.add(nod(), succeed(secondAddFuture), fail(secondAddFuture));

    Instance addedInstance = getOnCompletion(firstAddFuture);
    Instance otherAddedInstance = getOnCompletion(secondAddFuture);

    CompletableFuture<Instance> findFuture = new CompletableFuture<Instance>();
    CompletableFuture<Instance> otherFindFuture = new CompletableFuture<Instance>();

    collection.findById(addedInstance.id, succeed(findFuture), fail(findFuture));
    collection.findById(otherAddedInstance.id, succeed(otherFindFuture), fail(otherFindFuture));

    Instance foundSmallAngry = getOnCompletion(findFuture);
    Instance foundNod = getOnCompletion(otherFindFuture);

    assertThat(foundSmallAngry.title, is("Long Way to a Small Angry Planet"));

    assertThat(foundNod.title, is("Nod"));

    assertThat(hasIdentifier(foundSmallAngry, ISBN_IDENTIFIER_TYPE, "9781473619777"), is(true));
    assertThat(hasIdentifier(foundNod, ASIN_IDENTIFIER_TYPE, "B01D1PLMDO"), is(true));
  }

  @Test
  public void allInstancesCanBePaged()
    throws InterruptedException, ExecutionException, TimeoutException {

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
  public void anInstanceCanBeDeleted()
    throws ExecutionException, InterruptedException, TimeoutException {

    addSomeExamples(collection);

    CompletableFuture<Instance> instanceToBeDeletedFuture = new CompletableFuture<Instance>();

    collection.add(temeraire(), succeed(instanceToBeDeletedFuture), fail(instanceToBeDeletedFuture));

    Instance instanceToBeDeleted = instanceToBeDeletedFuture.get();

    CompletableFuture deleted = new CompletableFuture();

    collection.delete(instanceToBeDeleted.id, succeed(deleted), fail(deleted));

    waitForCompletion(deleted);

    CompletableFuture<Instance> findFuture = new CompletableFuture<Instance>();

    collection.findById(instanceToBeDeleted.id, succeed(findFuture), fail(findFuture));

    assertThat(findFuture.get(), is(CoreMatchers.nullValue()));

    CompletableFuture<MultipleRecords<Instance>> findAllFuture = new CompletableFuture<MultipleRecords<Instance>>();

    collection.findAll(PagingParameters.defaults(), succeed(findAllFuture), fail(findAllFuture));

    MultipleRecords<Instance> allInstancesWrapped = getOnCompletion(findAllFuture);

    List<Instance> allInstances = allInstancesWrapped.records;

    assertThat(allInstances.size(), is(3));
    assertThat(allInstancesWrapped.totalRecords, is(3));
  }

  @Test
  public void anInstanceCanBeUpdated()
    throws InterruptedException, ExecutionException, TimeoutException {

    CompletableFuture<Instance> addFinished = new CompletableFuture<Instance>();

    collection.add(smallAngryPlanet(), succeed(addFinished), fail(addFinished));

    Instance added = getOnCompletion(addFinished);

    CompletableFuture<Void> updateFinished = new CompletableFuture<>();

    Instance changed = added.removeIdentifier(ISBN_IDENTIFIER_TYPE, "9781473619777");

    collection.update(changed, succeed(updateFinished), fail(updateFinished));

    waitForCompletion(updateFinished);

    CompletableFuture<Instance> gotUpdated = new CompletableFuture<Instance>();

    collection.findById(added.id, succeed(gotUpdated), fail(gotUpdated));

    Instance updated = getOnCompletion(gotUpdated);

    assertThat(updated.id, is(added.id));
    assertThat(updated.title, is(added.title));
    assertThat(updated.identifiers.size(), is(0));
  }

  @Test
  public void instancesCanBeFoundByByPartialName()
    throws InterruptedException,
    ExecutionException,
    TimeoutException,
    UnsupportedEncodingException {

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

    assertThat(findByNameResults.get(0).id, is(addedSmallAngryPlanet.id));
  }

  private static void addSomeExamples(InstanceCollection instanceCollection)
    throws InterruptedException, ExecutionException, TimeoutException {

    WaitForAllFutures<Instance> allAdded = new WaitForAllFutures<>();

    instanceCollection.add(smallAngryPlanet(), allAdded.notifySuccess(), v -> {});
    instanceCollection.add(nod(), allAdded.notifySuccess(), v -> {});
    instanceCollection.add(uprooted(), allAdded.notifySuccess(), v -> {});

    allAdded.waitForCompletion();
  }

  private static Instance nod() {
    return createInstance("Nod")
      .addIdentifier(ASIN_IDENTIFIER_TYPE, "B01D1PLMDO")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Barnes, Adrian", AUTHOR_CONTRIBUTOR_TYPE, "");
  }

  private static Instance uprooted() {
    return createInstance("Uprooted")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "1447294149")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9781447294146")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Novik, Naomi", AUTHOR_CONTRIBUTOR_TYPE, "");
  }

  private static Instance smallAngryPlanet() {
    return createInstance("Long Way to a Small Angry Planet")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9781473619777")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Chambers, Becky", AUTHOR_CONTRIBUTOR_TYPE, "");
  }

  private static Instance temeraire() {
    return createInstance("Temeraire")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "0007258712")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9780007258710")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Novik, Naomi", AUTHOR_CONTRIBUTOR_TYPE, "");
  }

  private static Instance interestingTimes() {
    return createInstance("Interesting Times")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "0552167541")
      .addIdentifier(ISBN_IDENTIFIER_TYPE, "9780552167543")
      .addContributor(PERSONAL_CONTRIBUTOR_NAME_TYPE, "Pratchett, Terry", AUTHOR_CONTRIBUTOR_TYPE, "");
  }

  private static Instance createInstance(String title) {
    return new Instance(UUID.randomUUID().toString(), "local", title, BOOKS_INSTANCE_TYPE)
                .setAlternativeTitles(new ArrayList<>())
                .setIdentifiers(new ArrayList<>())
                .setContributors(new ArrayList<>());
  }

  private static boolean hasIdentifier(
    Instance instance,
    final String identifierTypeId,
    final String value) {

    return instance.identifiers.stream().anyMatch(it ->
      StringUtils.equals(((Identifier)it).identifierTypeId, identifierTypeId)
        && StringUtils.equals(((Identifier)it).value, value));
  }

  private Instance getInstance(List<Instance> allInstances, final String title) {
    return allInstances.stream()
      .filter(it -> StringUtils.equals(it.title, title))
      .findFirst().orElse(null);
  }
}
