import { query as muQuery, update as muUpdate, uuid } from "mu";

const BATCH_SIZE = 100;
const KANSELARIJ_GRAPH = "http://mu.semte.ch/graphs/organizations/kanselarij";
const DECISION_ACTIVITY_BASE =
  "http://themis.vlaanderen.be/id/beslissingsactiviteit/";
const MAX_RETRIES = 5;
const RETRY_TIMEOUT = 10000;

async function query(theQuery, retryCount = 0) {
  try {
    return await muQuery(theQuery);
  } catch (e) {
    if (retryCount < MAX_RETRIES) {
      await new Promise((resolve) =>
        setTimeout(resolve, retryCount * RETRY_TIMEOUT)
      );
      return await update(theQuery, retryCount + 1);
    }
    throw e;
  }
}

async function update(theQuery, retryCount = 0) {
  try {
    return await muUpdate(theQuery);
  } catch (e) {
    if (retryCount < MAX_RETRIES) {
      await new Promise((resolve) =>
        setTimeout(resolve, retryCount * RETRY_TIMEOUT)
      );
      return await update(theQuery, retryCount + 1);
    }
    throw e;
  }
}

async function fetchDoubledAgendaitemTreatments() {
  const response = await query(`
  PREFIX mu: <http://mu.semte.ch/vocablures/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>
  PREFIX sign: <http://mu.semte.ch/vocabularies/ext/handtekenen/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>

  SELECT

  ?agendaitemTreatment
  ?startDate
  ?created
  ?modified
  ?subcase
  ?piece
  ?newsletterInfo
  ?decisionResultCode
  ?publicationFlow
  ?signFlow

  ?agendaitemTreatment_
  ?startDate_
  ?created_
  ?modified_
  ?subcase_
  ?piece_
  ?newsletterInfo_
  ?decisionResultCode_
  ?publicationFlow_
  ?signFlow_

  WHERE {
    GRAPH <${KANSELARIJ_GRAPH}> {
      ?agendaitem a besluit:Agendapunt .

      ?agendaitemTreatment a besluit:BehandelingVanAgendapunt .
      ?agendaitemTreatment besluitvorming:heeftOnderwerp ?agendaitem .
      OPTIONAL { ?agendaitemTreatment dossier:Activiteit.startdatum ?startDate . }
      OPTIONAL { ?agendaitemTreatment dct:created ?created . }
      OPTIONAL { ?agendaitemTreatment dct:modified ?modified . }
      OPTIONAL { ?agendaitemTreatment ext:beslissingVindtPlaatsTijdens ?subcase . }
      OPTIONAL { ?agendaitemTreatment besluitvorming:genereertVerslag ?piece . }
      OPTIONAL { ?agendaitemTreatment prov:generated ?newsletterInfo . }
      OPTIONAL { ?agendaitemTreatment besluitvorming:resultaat ?decisionResultCode . }
      OPTIONAL { ?publicationFlow dct:subject ?agendaitemTreatment . }
      OPTIONAL { ?signFlow sign:heeftBeslissing ?agendaitemTreatment . }

      ?agendaitemTreatment_ a besluit:BehandelingVanAgendapunt .
      ?agendaitemTreatment_ besluitvorming:heeftOnderwerp ?agendaitem .
      OPTIONAL { ?agendaitemTreatment_ dossier:Activiteit.startdatum ?startDate_ . }
      OPTIONAL { ?agendaitemTreatment_ dct:created ?created_ . }
      OPTIONAL { ?agendaitemTreatment_ dct:modified ?modified_ . }
      OPTIONAL { ?agendaitemTreatment_ ext:beslissingVindtPlaatsTijdens ?subcase_ . }
      OPTIONAL { ?agendaitemTreatment_ besluitvorming:genereertVerslag ?piece_ . }
      OPTIONAL { ?agendaitemTreatment_ prov:generated ?newsletterInfo_ . }
      OPTIONAL { ?agendaitemTreatment_ besluitvorming:resultaat ?decisionResultCode_ . }
      OPTIONAL { ?publicationFlow_ dct:subject ?agendaitemTreatment_ . }
      OPTIONAL { ?signFlow_ sign:heeftBeslissing ?agendaitemTreatment_ . }

      FILTER (?agendaitemTreatment != ?agendaitemTreatment_)
      FILTER (?agendaitemTreatment > ?agendaitemTreatment_)
      FILTER NOT EXISTS {
        ?agendaitemTreatment besluitvorming:heeftBeslissing ?decisionActivity .
      }
    }
  }
  LIMIT ${BATCH_SIZE}
  `);
  return response.results.bindings.map((binding) => ({
    first: {
      agendaitemTreatment: binding.agendaitemTreatment.value,
      startDate: binding.startDate?.value,
      created: binding.created?.value,
      modified: binding.modified?.value,
      subcase: binding.subcase?.value,
      piece: binding.piece?.value,
      newsletterInfo: binding.newsletterInfo?.value,
      decisionResultCode: binding.decisionResultCode?.value,
      publicationFlow: binding.publicationFlow?.value,
      signFlow: binding.signFlow?.value,
    },
    second: {
      agendaitemTreatment_: binding.agendaitemTreatment_?.value,
      startDate_: binding.startDate_?.value,
      created_: binding.created_?.value,
      modified_: binding.modified_?.value,
      subcase_: binding.subcase_?.value,
      piece_: binding.piece_?.value,
      newsletterInfo_: binding.newsletterInfo_?.value,
      decisionResultCode_: binding.decisionResultCode_?.value,
      publicationFlow_: binding.publicationFlow_?.value,
      signFlow_: binding.signFlow_?.value,
    },
  }));
}

async function fetchAgendaitemTreatments() {
  const response = await query(`
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

  SELECT ?agendaitemTreatment
  WHERE {
    GRAPH <${KANSELARIJ_GRAPH}> {
      ?agendaitem a besluit:Agendapunt .
      ?agendaitemTreatment a besluit:BehandelingVanAgendapunt .
      ?agendaitemTreatment besluitvorming:heeftOnderwerp ?agendaitem .
  return response.results.bindings.map((binding) => ({
    agendaitemTreatment: binding.agendaitemTreatment.value,
    agendaitemTreatment_: binding.agendaitemTreatment_.value,
  }));
        ?agendaitemTreatment_ besluitvorming:heeftOnderwerp ?agendaitem .
        FILTER (?agendaitemTreatment != ?agendaitemTreatment_)
      }
    }
  }
  LIMIT ${BATCH_SIZE}
  `);
  return response.results.bindings.map(
    (binding) => binding.agendaitemTreatment.value
  );
}

async function migrateAgendaitemTreatments(uris) {
  const items = [];
  for (const uri of uris) {
    const uuid_ = uuid();
    items.push({
      agendaitemTreatment: uri,
      decisionActivity: DECISION_ACTIVITY_BASE + uuid_,
      uuid: uuid_,
    });
  }
  let theQuery = `
  PREFIX mu: <http://mu.semte.ch/vocablures/core/>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
  PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
  PREFIX besluitvorming: <http://data.vlaanderen.be/ns/besluitvorming#>

  DELETE {
    GRAPH <${KANSELARIJ_GRAPH}> {
      ?agendaitemTreatment dossier:Activiteit.startdatum ?startDate ;
                           ext:beslissingVindtPlaatsTijdens ?subcase ;
                           besluitvorming:resultaat ?decisionResultCode ;
                           besluitvorming:genereertVerslag ?piece .
    }
  }
  INSERT {
    GRAPH <${KANSELARIJ_GRAPH}> {
      ?agendaitemTreatment besluitvorming:heeftBeslissing ?decisionActivity .
      ?decisionActivity dossier:Activiteit.startdatum ?startDate ;
                        ext:beslissingVindtPlaatsTijdens ?subcase ;
                        besluitvorming:resultaat ?decisionResultCode ;
                        a besluitvorming:Beslissingsactiviteit ;
                        mu:uuid ?uuid .
      ?piece besluitvorming:beschrijft ?decisionActivity .
    }
  }
  WHERE {
    GRAPH <${KANSELARIJ_GRAPH}> {
      ?agendaitemTreatment a besluit:BehandelingVanAgendapunt .
      OPTIONAL { ?agendaitemTreatment dossier:Activiteit.startdatum ?startDate . }
      OPTIONAL { ?agendaitemTreatment ext:beslissingVindtPlaatsTijdens ?subcase . }
      OPTIONAL { ?agendaitemTreatment besluitvorming:resultaat ?decisionResultCode . }
      OPTIONAL { ?agendaitemTreatment besluitvorming:genereertVerslag ?piece . }
    }
    VALUES (?agendaitemTreatment ?uuid ?decisionActivity) {`;
  for (const { agendaitemTreatment, uuid, decisionActivity } of items) {
    theQuery += `
      (<${agendaitemTreatment}> "${uuid}" <${decisionActivity}>)`;
  }
  theQuery += `
    }
  }`;

  await update(theQuery);
}

(async function () {
  console.log(
    "Extracting decision-activity data from agenda-item-treatments on agendaitem"
  );
  let i = 1;
  while (true) {
    const agendaitemTreatmentUris = await fetchAgendaitemTreatments();
    if (agendaitemTreatmentUris.length == 0) {
      console.log("Finished migrating.");
      return;
    }
    await migrateAgendaitemTreatments(agendaitemTreatmentUris);
    console.log(`Batch ${i}`);
    i += 1;
  }
})();
