import argparse
import asyncio
import logging
import sys
import os

from automation_server_client import AutomationServer, Workqueue, Credential, WorkItemStatus
from kmd_nexus_client import NexusClientManager
from nexus_database_client import NexusDatabaseClient
from odk_tools.tracking import Tracker
from process.config import get_excel_mapping, load_excel_mapping
from process.nexus_service import NexusService

nexus: NexusClientManager
nexus_database_client: NexusDatabaseClient
nexus_service: NexusService
tracker: Tracker

proces_navn = "Kontrol af betalings- og handlekommune"


async def populate_queue(workqueue: Workqueue):    
    regler = get_excel_mapping()

    for organisation in regler.get("Organisation", []):
        org_objekt = nexus.organisationer.hent_organisation_ved_navn(navn=organisation)
        
        if org_objekt is None:
            continue

        borgere = nexus.organisationer.hent_borgere_for_organisation(organisation=org_objekt)

        for borger in borgere:
            # TODO: Check om borger er en reference eller fuldt objekt
            cpr = borger["patientIdentifier"]["identifier"]
            cpr = cpr.replace("-", "")

            if cpr in ("0108589995", "0505059996", "2512489996") or borger.get("patientState", {}).get("name") == "Død":
                continue

            eksisterende_kødata = workqueue.get_item_by_reference(cpr, WorkItemStatus.NEW)

            if len(eksisterende_kødata) > 0:
                continue

            workqueue.add_item(
                data=borger,
                reference=cpr
            )


async def process_workqueue(workqueue: Workqueue):
    logger = logging.getLogger(__name__)    

    for item in workqueue:
        with item:
            data = item.data  # Item data deserialized from json as dict
            borger = nexus.hent_fra_reference(data)

            try:
                fejl_type = ""

                if (borger.get("patientReimbursementInformation") is None or
                    borger["patientReimbursementInformation"].get("payingMunicipalityValueSchedule") is None):
                    fejl_type = "Borger har ikke betalingskommune tilknyttet."
                elif (borger["patientReimbursementInformation"].get("actingMunicipalityValueSchedule") is None):
                    fejl_type = "Borger har ikke handlekommune tilknyttet."
                else:
                    continue

                nexus_service.opret_opgave_og_rapporter(borger, fejl_type)                
                
            except Exception as e:
                # A WorkItemError represents a soft error that indicates the item should be passed to manual processing or a business logic fault
                logger.error(f"Error processing item: {item.reference}. Error: {e}")
                item.fail(str(e))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO        
    )

    ats = AutomationServer.from_environment()
    workqueue = ats.workqueue()

    nexus_credential = Credential.get_credential("KMD Nexus - produktion")
    nexus_database_credential = Credential.get_credential("KMD Nexus - database")    
    tracking_credential = Credential.get_credential("Odense SQL Server")

    tracker = Tracker(
        username=tracking_credential.username, 
        password=tracking_credential.password
    )

    nexus = NexusClientManager(
        client_id=nexus_credential.username,
        client_secret=nexus_credential.password,
        instance=nexus_credential.data["instance"],
    )

    nexus_database_client = NexusDatabaseClient(
        host = nexus_database_credential.data["hostname"],
        port = nexus_database_credential.data["port"],
        user = nexus_database_credential.username,
        password = nexus_database_credential.password,
        database = nexus_database_credential.data["database_name"],
    )
    
    nexus_service = NexusService(
        nexus=nexus,
        nexus_database = nexus_database_client,        
        tracker=tracker
    )

    # Parse command line arguments
    parser = argparse.ArgumentParser(description=proces_navn)
    parser.add_argument(
        "--excel-file",
        default="./Regelsæt.xlsx",
        help="Path to the Excel file containing mapping data (default: ./Regelsæt.xlsx)",
    )
    parser.add_argument(
        "--queue",
        action="store_true",
        help="Populate the queue with test data and exit",
    )
    args = parser.parse_args()

    # Validate Excel file exists
    if not os.path.isfile(args.excel_file):
        raise FileNotFoundError(f"Excel file not found: {args.excel_file}")

    # Load excel mapping data once on startup
    load_excel_mapping(args.excel_file)

    # Queue management
    if "--queue" in sys.argv:
        workqueue.clear_workqueue(WorkItemStatus.NEW)
        asyncio.run(populate_queue(workqueue))
        exit(0)

    # Process workqueue
    asyncio.run(process_workqueue(workqueue))
