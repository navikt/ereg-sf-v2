# EREG Salesforce v2

En Kotlin-tjeneste som importerer daglige full snapshots fra Brønnøysundregistrenes Enhetsregister, sammenligner dagens snapshot med gårsdagens og publiserer nye, oppdaterte og fjernede enheter til Salesforce.

Snapshots og prosessfremdrift lagres i PostgreSQL. Tjenesten tilbyr også interne endepunkter for status og database diagnostics, samt et enkelt GUI for å inspisere snapshot-data og endringer.

- [GUI – oversikt over siste kjøring](https://ereg-sf-v2.intern.nav.no/internal/gui)

## Dette gjør tjenesten

### 1. Importerer data fra Enhetsregisteret

Tjenesten laster ned gzip-komprimerte JSON-filer fra Brønnøysundregistrene:

- ENHET: `https://data.brreg.no/enhetsregisteret/api/enheter/lastned`
- UNDERENHET: `https://data.brreg.no/enhetsregisteret/api/underenheter/lastned`

Dataene prosesseres som streams og lagres i batches på 2 000 rader. Hver rad inneholder blant annet organisasjonsnummer, metadata, payload og SHA-256-hash. Snapshotet settes til `READY` når begge importene er fullført, eller `FAILED` ved feil.

### 2. Beregner daglige endringer

Dagens snapshot sammenlignes med gårsdagens, forutsatt at begge har status `READY`. Tjenesten kjører fire diff-faser for ENHET og UNDERENHET, og identifiserer `NEW`, `UPDATED` og `REMOVED` basert på organisasjonsnummer og payload-hash.

Prosessering og fremdrift lagres i PostgreSQL, slik at kjøringen kan gjenopptas og GUI-et kan vise et utvalg av endringene.

### 3. Publiserer endringer til Salesforce

Endringene sendes via Salesforce Composite sObject REST API i batches på maksimalt 100 poster. Både HTTP-responsen og resultatet for hver post kontrolleres.

- `NEW` og `UPDATED` publiseres til `public-ereg-cache-org-json`, med organisasjonsnummer, type og payload-hash som nøkkel.
- `REMOVED` publiseres til `public-ereg-cache-org-tombstones`, med organisasjonsnummeret som både nøkkel og verdi.

Opprydding kjøres først når publiseringen er fullført uten feil.

### 4. Beholder snapshots og historikk

Etter en vellykket kjøring beholdes snapshot-data for i dag og i går, mens eldre snapshot-rader slettes. Metadata og fremdrift beholdes i 30 dager, og endringslisten for i dag og i går.

<details markdown="1">
<summary><strong>Flyt for daglig kjøring</strong></summary>

```mermaid
flowchart TD
    A([Start daglig kjøring]) --> B[Opprett eller finn dagens snapshot]
    B --> C[Importer ENHET fra Brønnøysundregistrene]
    C --> D[Importer UNDERENHET fra Brønnøysundregistrene]
    D --> E{Begge importene fullført?}

    E -- Nei --> F[Marker dagens snapshot som FAILED]
    F --> Z([Avslutt])

    E -- Ja --> G[Marker dagens snapshot som READY]
    G --> H{Har dagens og gårsdagens snapshots status READY?}

    H -- Nei --> I[Hopp over differanseberegning, publisering og opprydding]
    I --> Z

    H -- Ja --> J[ENHET: Finn NEW og UPDATED]
    J --> K[ENHET: Finn REMOVED]
    K --> L[UNDERENHET: Finn NEW og UPDATED]
    L --> M[UNDERENHET: Finn REMOVED]

    M --> N[Lagre endringer og fremdrift i PostgreSQL]
    N --> O[Publiser til Salesforce i batcher på maksimalt 100 poster]
    O --> P{Fullført publisering uten feil?}

    P -- Nei --> Q[Avslutt uten opprydding av gamle snapshot-data]
    Q --> Z

    P -- Ja --> R[Slett eldre snapshot-rader]
    R --> S[Behold snapshot-data for i dag og i går]
    S --> T[Behold metadata og fremdrift i 30 dager]
    T --> U[Behold endringslisten for i dag og i går]
    U --> V([Ferdig])
```

</details>
