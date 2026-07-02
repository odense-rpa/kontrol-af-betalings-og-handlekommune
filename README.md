# Kontrol af betalings- og handlekommune

Robotten kontrollerer i KMD Nexus, om borgere tilknyttet udvalgte organisationer mangler betalingskommune eller handlekommune i stamdata, og opretter automatisk en opgave til den ansvarlige sagsbehandler hvis data mangler.

## Hvad gør robotten?

1. Indlæser organisationer og deres mapping fra Excel-filen `Regelsæt.xlsx`.
2. Henter alle borgere for hver organisation i KMD Nexus.
3. Springer testborgere og afdøde borgere over.
4. Tilføjer borgere til arbejdskøen i Automation Server (undgår dubletter).
5. For hver borger i køen: tjekker om betalingskommune og handlekommune er udfyldt i stamdata.
6. Hvis data mangler: finder den primære organisation og ansvarlige sagsbehandler via forløbstypen "Børn og Unge Grundforløb" eller "Socialfagligt grundforløb".
7. Finder en aktiv indsats at placere opgaven på.
8. Opretter en Nexus-opgave af typen "Angiv handle- og betalekommune" med 3 dages forfald.
9. Rapporterer resultatet (oprettet opgave eller årsag til ingen handling) via odk-tools.
10. Tracker opgaven i Automation Server via Tracker.

## Forudsætninger

- Python ≥ 3.13
- [`uv`](https://docs.astral.sh/uv/) til pakkehåndtering
- Adgang til **Automation Server** (arbejdskø og credentials)
- Adgang til **KMD Nexus** (produktion)
- Adgang til **Nexus-databasen** og **Odense SQL Server**
- Excel-filen `Regelsæt.xlsx` med organisationsmapping

## Installation

```sh
uv sync
```

## Konfiguration

Credentials registreres i Automation Server:
- `KMD Nexus - produktion`
- `KMD Nexus - database`
- `Odense SQL Server`

| Miljøvariabel | Beskrivelse |
|---|---|
| `ATS_URL` | URL til Automation Server API (f.eks. `http://localhost/api`) |
| `ATS_TOKEN` | Bearer token til Automation Server (valgfri) |
| `ATS_WORKQUEUE_OVERRIDE` | Overstyr arbejdskø-ID (valgfri) |

## Kørsel

```sh
uv run python main.py --queue   # Fyld arbejdskøen med borgere
uv run python main.py           # Behandl arbejdskøen
```

## Afhængigheder

| Pakke | Formål |
|---|---|
| `automation-server-client` | Kommunikation med Automation Server, herunder arbejdskø og credentials |
| `odk-tools` | Tracking af opgaver og rapportering via Automation Server |
| `kmd-nexus-client` | Hentning og opdatering af borgere, organisationer, indsatser og opgaver i KMD Nexus |
| `nexus-database-client` | Direkte databaseopslag i Nexus-databasen for at slå medarbejdere op via activity ID |
| `openpyxl` | Læsning af Excel-filen `Regelsæt.xlsx` med organisationsmapping |

## GDPR og sikkerhed

Processen behandler CPR-numre som referencenøgle i arbejdskøen. Borgerdata fra KMD Nexus, herunder `patientIdentifier` og reimbursement-information, behandles under kørsel. Data gemmes ikke permanent uden for Nexus og Automation Server. Adgang til rapporter bør begrænses til autoriserede sagsbehandlere.
