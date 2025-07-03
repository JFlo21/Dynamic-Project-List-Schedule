"""
Dynamic Gantt Scheduling System - V5.1 (Builder Edition)

- Implements the comprehensive game plan for dynamic scheduling.
- Builds a full Gantt hierarchy (Scope > Phase > Work Request) from scratch on each run.
- Dynamically allocates poles, assigns crews with placeholders, and calculates cascading dates.
- All logic is centralized for creating a professional, automated schedule.

Author: [Your Name/Org]
"""

import os
import math
import smartsheet
import logging
from collections import defaultdict
from datetime import datetime, timedelta

# -------- CONFIGURATION --------
API_TOKEN = os.getenv('SMARTSHEET_API_TOKEN')

# --- DATA SOURCE SHEETS ---
SHEET_ID_TOTAL_POLES = 8495204601384836 # Source of truth for total poles per scope
SHEET_ID_PHASE_POLES = 1553121697288068 # Source of truth for all tasks (WRs, Phases, Scopes)

# --- OUTPUT SHEET ---
SHEET_ID_TARGET      = 1847107938897796 # BLANK sheet where the Gantt chart will be built

# --- EMAILS FOR CONTACT OBJECTS ---
# Provides the required email for any crew assigned by name.
CREW_EMAILS = {
    "Crew A": "crew.a@yourcompany.com",
    "Crew B": "crew.b@yourcompany.com",
    "Ramp-Up Crew (Estimate)": "placeholder.crew@yourcompany.com"
}

# --- COLUMN ID MAPPING ---
# NOTE: All 'target' IDs refer to columns in the BLANK SHEET_ID_TARGET
COLUMNS = {
    # This is the primary column on your target sheet.
    "primary_target": 6392416854298500,
    
    # Mapping for the "Phase Poles" sheet (your master task list)
    "source_task_list": {
        "scope": 3784709278224260,
        "phase": 1865904432041860,
        "wr": 6922793410842500,
        "poles": 2674900969672580,
        "resource": 7828491195862916,
    },
    # Mapping for the "Total Poles" sheet
    "source_total_poles": {
        "scope": 3784709278224260,
        "poles": 2795493429825412,
    },
    # Mapping for data columns in your blank Target Sheet
    "target_gantt": {
        "scope": 6392416854298500, # Also the primary column
        "phase": 4140617040613252,
        "wr": 8644216667983748,
        "resource": 1945268683231108,
        "placement": 7574768217444228,
        "poles": 2674900969672580,
        "start_date": 819368776388484,
        "end_date": 3071168590073732,
    }
}

# --- BUSINESS LOGIC ---
POLES_PER_DAY = 1.2
PLACEHOLDER_CREW = "Ramp-Up Crew (Estimate)"

# -------- LOGGING --------
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# -------- DATA STRUCTURES --------
class Job:
    def __init__(self, wr, scope, phase, crew, placement, poles=0):
        self.wr = wr
        self.scope = scope
        self.phase = phase
        self.crew = crew
        self.placement = placement or 9999
        self.poles = poles or 0
        self.start_date = None
        self.end_date = None

    def duration_days(self):
        return int(math.ceil(self.poles / POLES_PER_DAY)) if self.poles > 0 else 0

# -------- CORE LOGIC --------
def get_cell_value(row, col_id):
    """Safely gets a cell's value, handling objectValues for contacts."""
    cell = row.get_column(col_id)
    if not cell or cell.value is None:
        return None
    # For contact cells, prioritize the name from the objectValue
    if cell.object_value and hasattr(cell.object_value, 'name'):
        return cell.object_value.name
    return cell.value

def aggregate_data_from_sources(client):
    """Reads all source sheets and builds a unified, hierarchical dictionary of all jobs."""
    logging.info("Aggregating data from source sheets...")
    task_list_rows = client.Sheets.get_sheet(SHEET_ID_PHASE_POLES, include=['objectValue']).rows
    
    jobs_by_hierarchy = defaultdict(lambda: defaultdict(list))
    all_jobs = []

    for row in task_list_rows:
        scope = get_cell_value(row, COLUMNS['source_task_list']['scope'])
        phase = get_cell_value(row, COLUMNS['source_task_list']['phase'])
        wr = get_cell_value(row, COLUMNS['source_task_list']['wr'])
        
        if not all([scope, phase, wr]):
            logging.warning(f"Skipping row {row.row_number} in source sheet due to missing Scope, Phase, or WR.")
            continue

        job = Job(
            wr=wr, scope=scope, phase=phase,
            crew=get_cell_value(row, COLUMNS['source_task_list']['resource']) or PLACEHOLDER_CREW,
            placement=get_cell_value(row, COLUMNS['target_gantt']['placement']),
            poles=get_cell_value(row, COLUMNS['source_task_list']['poles'])
        )
        jobs_by_hierarchy[scope][phase].append(job)
        all_jobs.append(job)
        
    logging.info(f"Aggregated {len(all_jobs)} total jobs.")
    return jobs_by_hierarchy, all_jobs

def allocate_poles(client, jobs_by_hierarchy):
    """Calculates and assigns pole counts for phases with missing values."""
    logging.info("Allocating pole counts...")
    total_poles_sheet = client.Sheets.get_sheet(SHEET_ID_TOTAL_POLES)
    scope_totals = {
        row.get_column(COLUMNS['source_total_poles']['scope']).value: row.get_column(COLUMNS['source_total_poles']['poles']).value
        for row in total_poles_sheet.rows
    }

    for scope, phases in jobs_by_hierarchy.items():
        total_poles = scope_totals.get(scope, 0)
        if not total_poles: continue

        assigned_poles = sum(job.poles for phase in phases.values() for job in phase if job.poles)
        phases_with_unknown_poles = [phase for phase, jobs in phases.items() if not any(j.poles for j in jobs)]

        if phases_with_unknown_poles:
            remaining_poles = total_poles - assigned_poles
            if remaining_poles > 0:
                poles_per_phase = int(math.ceil(remaining_poles / len(phases_with_unknown_poles)))
                for phase_name in phases_with_unknown_poles:
                    for job in jobs_by_hierarchy[scope][phase_name]:
                        job.poles = poles_per_phase

def perform_scheduling(all_jobs):
    """Organizes jobs by crew and calculates cascading start/end dates."""
    logging.info("Performing scheduling...")
    crews = defaultdict(list)
    for job in all_jobs:
        crews[job.crew].append(job)

    for crew_name, jobs in crews.items():
        sorted_jobs = sorted(jobs, key=lambda j: j.placement)
        current_date = datetime.now() 

        for job in sorted_jobs:
            duration = job.duration_days()
            if duration > 0:
                job.start_date = current_date
                job.end_date = current_date + timedelta(days=duration - 1)
                # To calculate next start date, add 1 day. This simple logic doesn't skip weekends.
                current_date = job.end_date + timedelta(days=1)

def build_gantt_from_scratch(client, scheduled_jobs):
    """Deletes all rows in the target sheet and rebuilds the Gantt chart with hierarchy."""
    logging.info(f"Rebuilding Gantt chart on sheet ID {SHEET_ID_TARGET}...")
    
    # 1. Clear the sheet to ensure a clean slate
    try:
        sheet = client.Sheets.get_sheet(SHEET_ID_TARGET)
        if sheet.rows:
            all_row_ids = [row.id for row in sheet.rows]
            client.Sheets.delete_rows(SHEET_ID_TARGET, all_row_ids, ignore_errors=True)
            logging.info(f"Deleted {len(all_row_ids)} existing rows.")
    except smartsheet.exceptions.ApiError as e:
        if e.error.error_code == 1006: # Sheet not found is okay
            logging.info("Target sheet is empty. No rows to delete.")
        else: raise e

    # --- 2. Build Hierarchy: Scope (Parent) -> Phase (Child) -> WR (Grandchild) ---
    scope_to_row_id = {}
    phase_to_row_id = {}
    all_rows_to_add = []
    
    # Group jobs by hierarchy for building
    jobs_by_hierarchy = defaultdict(lambda: defaultdict(list))
    for job in scheduled_jobs:
        jobs_by_hierarchy[job.scope][job.phase].append(job)

    # A. Create Parent (Scope) Rows
    for scope_name in sorted(jobs_by_hierarchy.keys()):
        row = smartsheet.models.Row()
        row.cells.append({'column_id': COLUMNS['primary_target'], 'value': scope_name})
        all_rows_to_add.append(row)
    
    if not all_rows_to_add:
        logging.warning("No data found to build the schedule. Exiting.")
        return
        
    added_rows = client.Sheets.add_rows(SHEET_ID_TARGET, all_rows_to_add).result
    for row in added_rows:
        scope_to_row_id[row.cells[0].value] = row.id

    # B. Create Child (Phase) and Grandchild (WR) Rows
    all_rows_to_add = []
    for scope_name, phases in sorted(jobs_by_hierarchy.items()):
        scope_row_id = scope_to_row_id.get(scope_name)
        if not scope_row_id: continue

        # Add a row for the phase
        for phase_name, jobs in sorted(phases.items()):
            phase_row = smartsheet.models.Row()
            phase_row.parent_id = scope_row_id
            phase_row.cells.append({'column_id': COLUMNS['primary_target'], 'value': phase_name})
            phase_rows_to_add = client.Sheets.add_rows(SHEET_ID_TARGET, [phase_row]).result
            phase_row_id = phase_rows_to_add[0].id

            # Add rows for the work requests under this phase
            wr_rows_to_add = []
            for job in sorted(jobs, key=lambda j: j.placement):
                wr_row = smartsheet.models.Row()
                wr_row.parent_id = phase_row_id
                
                # Populate all cells for the job
                cells = [
                    {'column_id': COLUMNS['primary_target'], 'value': job.wr},
                    {'column_id': COLUMNS['target_gantt']['scope'], 'value': job.scope},
                    {'column_id': COLUMNS['target_gantt']['phase'], 'value': job.phase},
                    {'column_id': COLUMNS['target_gantt']['wr'], 'value': job.wr}
                ]
                
                email = CREW_EMAILS.get(job.crew)
                if email:
                    cells.append({'column_id': COLUMNS['target_gantt']['resource'], 'objectValue': {'objectType': 'CONTACT', 'name': job.crew, 'email': email}})
                
                if job.poles:
                    cells.append({'column_id': COLUMNS['target_gantt']['poles'], 'value': job.poles})
                if job.start_date:
                    cells.append({'column_id': COLUMNS['target_gantt']['start_date'], 'value': job.start_date.strftime('%Y-%m-%d')})
                if job.end_date:
                    cells.append({'column_id': COLUMNS['target_gantt']['end_date'], 'value': job.end_date.strftime('%Y-%m-%d')})
                
                wr_row.cells = cells
                wr_rows_to_add.append(wr_row)

            if wr_rows_to_add:
                client.Sheets.add_rows(SHEET_ID_TARGET, wr_rows_to_add)

    logging.info(f"Successfully built Gantt chart.")

# -------- MAIN EXECUTION --------
def main():
    """Main workflow to run the entire scheduling and building process."""
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    jobs_by_hierarchy, all_jobs = aggregate_data_from_sources(client)
    allocate_poles(client, jobs_by_hierarchy)
    perform_scheduling(all_jobs)
    build_gantt_from_scratch(client, all_jobs)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"A critical error occurred: {e}", exc_info=True)