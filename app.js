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
