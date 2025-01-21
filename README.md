# bicam-collection

# Overview
This repository contains the code used for the collection and cleaning of the Bulk Ingestion of Congressional Actions & Materials, also known as the BICAM dataset. Below, we will detail each section of the code, and the order in which they should be run to create the BICAM dataset.

## Sections

### database_construction
This section contains the code for creating the database schema, built in PostgreSQL. All scripts beginning with `build_` create the schemas and tables,
while the `join_congressional_govinfo.sql` script takes both production schemas and the staging schemas and joins them together into BICAM.

### scrapers
This section contains the code for scraping the data from the source websites. The `congressional` and `govinfo` folders contain the scrapers for the Congressional and GovInfo data, respectively, including the main scrapers, the cleanup scripts for testing and debugging, the script for inserting the data into the database, and the script for scraping text data from URLs in given components.

### lobbyist_matching
This section contains the code for extracting and matching references to legislation from lobbying filings.

### cleaning
This section contains the code for cleaning the data. The `staging_info.yml` file contains the information for the cleaning scripts, and the `final_cleaning_and_creation.py` script is the main script for cleaning the data, utilizing each individual cleaning script with custom arguments.

### backfills
This section contains the code for backfilling the data into the database. When joining the schemas, we find that some data exists from one source, but not in another, or we need to add data that raised errors during the original scraping process. These scripts are used to add this data to the database.

### bulk_exports
This section contains the code for exporting the data to zipped folders of CSV files.

## Sequence

To begin, create the schemas in `database_construction`, running the `build_` scripts in any order. 

Next, run the scrapers in `scrapers`. These both scrape the data from the source websites - you may need to setup external tables to fully
make use of the functionality to begin from the previous last-processed date and store errors. These scrapers utilize the package found within `api_interface` to interact with the API.

After the scrapers are run, insert the data into the database using the `insert_csv.py` script in `scrapers` with the proper arguments for the
source directory and the target schema.

At any point in this process, you can begin the lobbyist matching in `lobbyist_matching` by running `main.py` from the command line - everything should follow
from that, including matching and post-processing. Once this completes, insert the data into the database using the `insert_csv.py` script in `scrapers`.

Then, run the cleaning in `cleaning` by running `final_cleaning_and_creation.py` from the command line. All arguments should be provided
within `staging_info.yml`, and the script will follow the order of the phases as defined in the script.

Next, join the schemas in `database_construction` using the `join_congressional_govinfo.sql` script. if there are errors, you can run the `backfills` section with any missing data.

Finally, run the bulk exports in `bulk_exports` by running `exporter.py` from the command line with a target schema and target directory.