from datetime import datetime, timedelta
from kmd_nexus_client import NexusClientManager
from kmd_nexus_client.tree_helpers import (
    filter_by_path,    
)
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from odk_tools.reporting import report

proces_navn = "Kontrol af betalings- og handlekommune"


class NexusService:
    def __init__(self, nexus: NexusClientManager, nexus_database: NexusDatabaseClient, tracker: Tracker):
        self.nexus = nexus        
        self.nexus_database = nexus_database
        self.tracker = tracker

    def _parse_nexus_date(self, date_value: str | None) -> datetime | None:
        if not date_value:
            return None

        return datetime.strptime(date_value, "%Y-%m-%d")

    def _hent_primær_organisationsrelation(self, borger: dict) -> dict | None:
        organisationer = self.nexus.organisationer.hent_organisationer_for_borger(
            borger,
            kun_aktive=False,
        )

        aktive_organisationer = [
            organisation
            for organisation in organisationer
            if (
                (effective_end_date := self._parse_nexus_date(organisation.get("effectiveEndDate"))) is None
                or effective_end_date > datetime.now()
            )
        ]

        aktive_organisationer.sort(
            key=lambda organisation: self._parse_nexus_date(organisation.get("effectiveStartDate")) or datetime.min,
            reverse=True,
        )

        return aktive_organisationer[0] if aktive_organisationer else None

    def _hent_medarbejder(self, borger: dict) -> dict | None:
        pathway = self.nexus.borgere.hent_visning(borger=borger)

        if pathway is None:
            return None

        referencer = self.nexus.borgere.hent_referencer(visning=pathway)
        
        sagsbehandler = filter_by_path(
            referencer,
            path_pattern="/Børn og Unge Grundforløb/*/professionalReference",
            active_pathways_only=True,
        )

        if len(sagsbehandler) == 0:
            # Check grundforløb for sagsbehandler
            sagsbehandler = filter_by_path(
                referencer,
                path_pattern="/Børn og Unge Grundforløb/professionalReference",
                active_pathways_only=True,
            )

            if len(sagsbehandler) == 0:
                return None

        # Resolve sagsbehandler, objekt i pathway har ingen kontakt information, dvs. vi resolver hele medarbejderen.
        sagsbehandler = self.nexus.hent_fra_reference(sagsbehandler[0])
        sagsbehandler = self.nexus_database.hent_medarbejder_med_activity_id(
            sagsbehandler.get("activityIdentifier", {}).get("activityId", "")
        )
        sagsbehandler = self.nexus.organisationer.hent_medarbejder_ved_initialer(
            sagsbehandler[0].get("primary_identifier", "")
        )

        if sagsbehandler is None:
            return None
        
        return sagsbehandler
    
    def _hent_indsats_til_opgaveplacering(self, borger: dict) -> dict | None:
        pathway = self.nexus.borgere.hent_visning(borger=borger)

        if pathway is None:
            return None

        referencer = self.nexus.borgere.hent_referencer(visning=pathway)
        

        grundforløb = ["Børn og Unge Grundforløb", "Socialfagligt grundforløb"]

        for forløb in grundforløb:
            indsatsreferencer = filter_by_path(
                referencer,
                path_pattern=f"/{forløb}/*/Indsatser/basketGrantReference",
                active_pathways_only=True,
            )

            indsatsreferencer = self.nexus.indsatser.filtrer_indsats_referencer(
                indsats_referencer=indsatsreferencer,
                kun_aktive=True,
                inkluder_indsatspakker=False
            )

            if len(indsatsreferencer) > 0:
                indsats = self.nexus.hent_fra_reference(indsatsreferencer[0])

                if indsats is not None:
                    return indsats

        return None


    def opret_opgave_og_rapporter(self, borger: dict, fejl_type: str):
        cpr = (borger.get("patientIdentifier") or {}).get("identifier")
        organisation = self._hent_primær_organisationsrelation(borger)
        

        medarbejder = self._hent_medarbejder(borger)

        if medarbejder is None:
            report(
                report_id="kontrol_af_betalings_og_handlekommune",
                group="Borgere",
                json={
                    "Cpr": cpr,                    
                    "Handling": "Borger har mangler i stamdata, men ingen ansvarlig sagsbehandler at lægge opgave til.",
                    "Organisation": organisation.get("organization", {}).get("name", "") if organisation else ""
                }
            )
            self.tracker.track_partial_task(process_name=proces_navn)
            return

        indsats = self._hent_indsats_til_opgaveplacering(borger)

        if indsats is None:
            report(
                report_id="kontrol_af_betalings_og_handlekommune",
                group="Borgere",
                json={
                    "Cpr": cpr,                    
                    "Handling": "Borger har ingen indsatser at oprette opgave på.",
                    "Organisation": organisation.get("organization", {}).get("name", "") if organisation else ""
                }
            )
            self.tracker.track_partial_task(process_name=proces_navn)
            return
        
        opgaver = self.nexus.opgaver.hent_opgave_historik(objekt=indsats) or []

        if len(opgaver) > 0:
            for opgave in opgaver:
            # Opgave er oprettet i forvejen på element
                if opgave.get("opgaveType", "") == "Angiv handle- og betalekommune":
                    return

        self.nexus.opgaver.opret_opgave(
            objekt=indsats,
            opgave_type="Angiv handle- og betalekommune",
            titel="HUSK - opret stamdata på handle- og betalingskommune.",
            ansvarlig_organisation=medarbejder["primaryOrganization"]["name"],
            ansvarlig_medarbejder=medarbejder,
            start_dato=datetime.now().date(),
            forfald_dato=datetime.now().date() + timedelta(days=3)
        )

        report(
                report_id="kontrol_af_betalings_og_handlekommune",
                group="Borgere",
                json={
                    "Cpr": cpr,                    
                    "Handling": fejl_type,
                    "Organisation": organisation.get("organization", {}).get("name", "") if organisation else ""

                }
        )
        self.tracker.track_task(process_name=proces_navn)
        
    