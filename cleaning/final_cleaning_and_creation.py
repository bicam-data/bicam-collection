import logging
import argparse
from datatypes import (
    amendments,
    bills,
    committeemeetings,
    committees,
    committeeprints,
    committeereports,
    congresses,
    hearings,
    members,
    nominations,
    treaties,
    congressional_directories,
    bill_collections,
    committee_prints,
    committee_reports,
    congressional_hearings,
    treaty_docs
)
from cleaning_coordinator import CleaningCoordinator
from dotenv import load_dotenv
import os

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_phase(modules: list, coordinator: CleaningCoordinator, start_from: str = None, phase_name: str = "") -> bool:
    """Process a single phase of modules"""
    coordinator.cleaning_modules = []
    coordinator.dependency_graph = None
    
    logger.info(f"\n{'='*20} Processing {phase_name} {'='*20}")
    
    # Register modules for this phase
    for name, func, schema in modules:
        coordinator.register_module(name, func, schema)
    
    coordinator.initialize()
    
    if start_from:
        phase_names = [name for name, _, _ in modules]
        if start_from not in phase_names:
            return False
        start_idx = phase_names.index(start_from)
        modules = modules[start_idx:]
    
    # Map special cases for verification
    table_name_map = {
        'committees_base': 'committees',
        'committees_relationships': 'committees'
    }
    
    for name, func, schema in modules:
        try:
            logger.info(f"Processing module {name}...")
            func(coordinator=coordinator)
            
            main_table = name.lstrip('_')
            # Use mapping for verification
            verify_table = table_name_map.get(main_table, main_table)
            
            count = coordinator.verify_table_population(schema, verify_table)
            if count == 0:
                raise Exception(f"No data was written to {schema}.{verify_table}")
            
            logger.info(f"Successfully completed {name} cleaning script with {count} rows")
            
        except Exception as e:
            logger.error(f"Error in {name} cleaning script: {str(e)}")
            raise
            
    return start_from and True

def main():
    parser = argparse.ArgumentParser(description='Run all cleaning scripts')
    parser.add_argument('--resume-from', type=str, help='Module name to resume from')
    args = parser.parse_args()



    coordinator = CleaningCoordinator()
    
        # Level 1: No dependencies
    modules_phase1 = [
        ('congresses', congresses.clean_congresses, 'congressional'),
    ]
    
    # Level 2: Depends only on congresses
    modules_phase2 = [
        ('members', members.clean_members, 'congressional'),
        ('treaties', treaties.clean_treaties, 'congressional'),
    ]
    
    # Level 3: Base committees table
    modules_phase3 = [
        ('committees_base', committees.clean_committees_base, 'congressional'),  # New function for base table only
    ]
    
    # Level 4A: Dependencies on committees_base 
    modules_phase4a = [
        ('bills', bills.clean_bills, 'congressional'),
        ('hearings', hearings.clean_hearings, 'congressional'),
    ]

    # Level 4B: Dependencies on 4A
    modules_phase4b = [
        ('nominations', nominations.clean_nominations, 'congressional'),
    ]
    # Level 5: Dependencies on bills and earlier
    modules_phase5 = [
        ('amendments', amendments.clean_amendments, 'congressional'),
        ('committeereports', committeereports.clean_committeereports, 'congressional'),
        ('committeeprints', committeeprints.clean_committeeprints, 'congressional'),
    ]
    
    # Level 6: Final dependencies and relationship tables
    modules_phase6 = [
        ('committees_relationships', committees.clean_committees_relationships, 'congressional'),  # New function for relationship tables
    ]
    
    modules_phase7 = [
        ('committeemeetings', committeemeetings.clean_committeemeetings, 'congressional'),
    ]
    
    modules_phase8 = [
        ('congressional_directories', congressional_directories.clean_congressional_directories, 'govinfo'),
        ('bill_collections', bill_collections.clean_bill_collections, 'govinfo'),
        ('committee_prints', committee_prints.clean_committee_prints, 'govinfo'),
        ('committee_reports', committee_reports.clean_committee_reports, 'govinfo'),
        ('congressional_hearings', congressional_hearings.clean_hearings, 'govinfo'),
        ('treaty_docs', treaty_docs.clean_treaties, 'govinfo'),
    ]
    
    phases = [
        ("Phase 1 - No Dependencies", modules_phase1),
        ("Phase 2 - Congress Dependencies", modules_phase2),
        ("Phase 3 - Base Committees", modules_phase3),
        ("Phase 4A - Primary Tables", modules_phase4a),
        ("Phase 4B - Dependencies on 4A", modules_phase4b),
        ("Phase 5 - Secondary Dependencies", modules_phase5),
        ("Phase 6 - Final Dependencies", modules_phase6),
        ("Phase 7 - Committee Meetings", modules_phase7),
        ("Phase 8 - GovInfo", modules_phase8),
    ]
    
    found_module = False
    for phase_name, modules in phases:
        if found_module:
            process_phase(modules, coordinator, phase_name=phase_name)
        else:
            found_module = process_phase(modules, coordinator, args.resume_from, phase_name)

if __name__ == "__main__":
    main()
    
    # TODO LIST:
# 1: final cleaning for congressional
# 4: link up govinfo/congressional into BICAM
# 4.5: RESCRAPE SPOTCHECK SCRIPT
# 5: export zips
# 6: finish lobbying matching
# 7: clean/port over code to github
### Processing order notes:
# standardize dates
# remove tags
# add raw texts
# redo counts for tables
# expand ids into parts in main tables
# make script to join the two
# rescrape congressional directories


# make spot check for committee bills/reports: hsru00, hsvr00
# hsag00
# hsap00
# hsju00
# hswm00
# slin00
# ssap00
# ssas00
# ssfi00
# ssfr00
# ssju00
# ssra00
# jslc00
# jspr00
# ssva00
# ssbu00
# scnc00
# hsbu00
# hsas29
# hsvr09
# hsha27
# hswm04
# hswm06
# hsju13
# hswm02
# hsap24
# hlfd00
# hsvr08
# hswm01
# hsba21
# hsvr11
# hlzs00
# hsvr10


# FULL CHECKS WITHOUT SUBCOMMITTEES
# hsap23



# establishing authority link for committee history
