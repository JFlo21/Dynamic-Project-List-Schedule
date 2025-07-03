"""
Dynamic Gantt Scheduling System - V8.3 (Final & Corrected)

- FINAL: Correctly maps "Scope ID #" as the phase identifier to build the full 3-level hierarchy.
- FIX: Resolved KeyError by stripping whitespace from column titles during mapping, making it more robust.
- Dynamically discovers all column IDs by name at runtime.
- Builds the Gantt chart from scratch on each run with full parent-child relationships.
"""

import os
import math
import smartsheet
import logging
from collections import defaultdict
from datetime import datetime, timedelta

# -------- CONFIGURATION --------
API_TOKEN = os.getenv('SMARTSHEET_API_TOKEN')

# --- DATA SOURCE & TARGET SHEETS ---
SHEET_ID_TOTAL_POLES = 8495204601384836
SHEET_ID_PHASE_POLES = 1553121697288068 # Master task list
SHEET_ID_TARGET      = 1847107938897796 # The blank sheet for the Gantt chart

# --- COLUMN NAMES (Derived from your JSON data and clarifications) ---
COLUMN_NAMES = {
    # Names for the master task list (SHEET_ID_PHASE_POLES)
    "source_scope": "Scope #",
    "source_phase": "Scope ID #", # Corrected mapping for the phase level
    "source_wr": "Work Request #",
    "source_poles": "Total Poles",
    "source_resource": "Foreman Assigned + General Foreman",
    
    # Names for the total poles sheet
    "total_poles_scope": "Job ID",
    "total_poles_count": "total # poles on the project to instal wire on",
    
    # Names for the columns in the final target Gantt sheet
    "target_primary": "Scope #", # This is the primary column
    "target_scope": "Scope #",   # This is the dedicated column for scope reporting
    "target_phase": "Scope Phase #",
    "target_wr": "Work Request #",
    "target_resource": "Assigned Resource", # Trailing space removed for consistency
    "target_placement": "Job Placement",
    "target_poles": "Pole Count (Days)",
    "target_start": "Expected Start Date",
    "target_end": "Expected End Date"
}

# --- EMAILS FOR CONTACT OBJECTS ---
CREW_EMAILS = {
    "Crew A": "crew.a@yourcompany.com",
    "Ramp-Up Crew (Estimate)": "placeholder.crew@yourcompany.com"
}

# --- BUSINESS LOGIC ---
POLES_PER_DAY = 1.2
PLACEHOLDER_CREW = "Ramp-Up Crew (Estimate)"

# -------- LOGGING --------
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# -------- DATA STRUCTURES --------
class Job:
    def __init__(self, wr, scope, phase, crew, placement, poles=0):
        self.wr, self.scope, self.phase, self.crew = wr, scope, phase, crew
        self.placement = placement or 9999
        self.poles = poles or 0
        self.start_date, self.end_date = None, None

    def duration_days(self):
        return int(math.ceil(self.poles / POLES_PER_DAY)) if self.poles > 0 else 0

# -------- CORE LOGIC --------
def build_column_map(sheet_obj):
    """Creates a dictionary mapping column titles to column IDs, stripping whitespace."""
    # CORRECTED: Added .strip() to make the mapping robust against whitespace issues.
    return {col.title.strip(): col.id for col in sheet_obj.columns}

def get_cell_value(row, col_map, col_name):
    """Safely gets a cell's value using the dynamic column map."""
    col_id = col_map.get(col_name)
    if not col_id: return None
    cell = row.get_column(col_id)
    if not cell or cell.value is None: return None
    if cell.object_value and hasattr(cell.object_value, 'name'):
        return cell.object_value.name
    return cell.value

def aggregate_data_from_sources(client, col_maps):
    """Reads source sheets and builds a hierarchical dictionary of all jobs."""
    logging.info("Aggregating data from source sheets...")
    task_list_sheet = client.Sheets.get_sheet(SHEET_ID_PHASE_POLES, include=['objectValue'])
    task_map = col_maps['phase_poles']
    
    jobs_by_hierarchy = defaultdict(lambda: defaultdict(list))
    all_jobs = []

    for row in task_list_sheet.rows:
        scope = get_cell_value(row, task_map, COLUMN_NAMES['source_scope'])
        phase = get_cell_value(row, task_map, COLUMN_NAMES['source_phase']) # Using corrected phase column
        wr = get_cell_value(row, task_map, COLUMN_NAMES['source_wr'])
        
        if not all([scope, phase, wr]):
            logging.warning(f"Skipping row {row.row_number} due to missing Scope, Phase, or WR.")
            continue

        job = Job(
            wr=wr, scope=scope, phase=phase,
            crew=get_cell_value(row, task_map, COLUMN_NAMES['source_resource']) or PLACEHOLDER_CREW,
            placement=get_cell_value(row, task_map, COLUMN_NAMES['target_placement']),
            poles=get_cell_value(row, task_map, COLUMN_NAMES['source_poles'])
        )
        jobs_by_hierarchy[scope][phase].append(job)
        all_jobs.append(job)
        
    logging.info(f"Aggregated {len(all_jobs)} total jobs into a 3-level hierarchy.")
    return jobs_by_hierarchy, all_jobs

def allocate_poles(client, jobs_by_hierarchy, col_maps):
    """Allocates pole counts for phases with missing values."""
    if not jobs_by_hierarchy: return
    logging.info("Allocating pole counts...")
    
    total_poles_sheet = client.Sheets.get_sheet(SHEET_ID_TOTAL_POLES)
    total_poles_map = col_maps['total_poles']
    scope_totals = {
        row.get_column(total_poles_map[COLUMN_NAMES['total_poles_scope']]).value: row.get_column(total_poles_map[COLUMN_NAMES['total_poles_count']]).value
        for row in total_poles_sheet.rows if row.get_column(total_poles_map[COLUMN_NAMES['total_poles_scope']])
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
    """Calculates cascading start/end dates."""
    if not all_jobs: return
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
                current_date = job.end_date + timedelta(days=1)

def build_gantt_from_scratch(client, jobs_by_hierarchy, col_maps):
    """Builds a 3-level Gantt chart (Scope > Phase > WR) from scratch."""
    if not jobs_by_hierarchy:
        logging.warning("No data to build. Gantt sheet will be empty.")
        return
        
    logging.info(f"Rebuilding Gantt chart on sheet ID {SHEET_ID_TARGET}...")
    target_map = col_maps['target']
    
    # 1. Clear the sheet
    try:
        sheet = client.Sheets.get_sheet(SHEET_ID_TARGET)
        if sheet.rows:
            client.Sheets.delete_rows(SHEET_ID_TARGET, [r.id for r in sheet.rows])
            logging.info(f"Deleted existing rows from target sheet.")
    except smartsheet.exceptions.ApiError as e:
        if e.error.error_code == 1006: # Sheet not found is okay
             logging.info("Target sheet is empty or not found. Proceeding to build.")
        else:
            raise e # Re-raise other API errors

    # 2. Add all rows with hierarchy in a single, efficient call
    rows_to_add = []
    for scope_name, phases in sorted(jobs_by_hierarchy.items()):
        scope_row = smartsheet.models.Row()
        scope_row.cells.append({'column_id': target_map[COLUMN_NAMES['target_primary']], 'value': scope_name})
        
        phase_rows = []
        for phase_name, jobs in sorted(phases.items()):
            phase_row = smartsheet.models.Row()
            phase_row.cells.append({'column_id': target_map[COLUMN_NAMES['target_primary']], 'value': phase_name})
            
            wr_rows = []
            for job in sorted(jobs, key=lambda j: j.placement):
                wr_row = smartsheet.models.Row()
                
                # Populate all cells for the work request
                cells = [
                    # The name of the WR goes in the primary column
                    {'column_id': target_map[COLUMN_NAMES['target_primary']], 'value': job.wr},
                    # Add data to other dedicated columns for reporting
                    {'column_id': target_map[COLUMN_NAMES['target_scope']], 'value': job.scope},
                    {'column_id': target_map[COLUMN_NAMES['target_phase']], 'value': job.phase},
                    {'column_id': target_map[COLUMN_NAMES['target_wr']], 'value': job.wr},
                ]
                
                email = CREW_EMAILS.get(job.crew)
                if email: cells.append({'column_id': target_map[COLUMN_NAMES['target_resource']], 'objectValue': {'objectType': 'CONTACT', 'name': job.crew, 'email': email}})
                
                if job.poles: cells.append({'column_id': target_map[COLUMN_NAMES['target_poles']], 'value': job.poles})
                if job.start_date: cells.append({'column_id': target_map[COLUMN_NAMES['target_start']], 'value': job.start_date.strftime('%Y-%m-%d')})
                if job.end_date: cells.append({'column_id': target_map[COLUMN_NAMES['target_end']], 'value': job.end_date.strftime('%Y-%m-%d')})
                
                wr_row.cells = cells
                wr_rows.append(wr_row)
            
            phase_row.children = wr_rows
            phase_rows.append(phase_row)
        
        scope_row.children = phase_rows
        rows_to_add.append(scope_row)

    if rows_to_add:
        client.Sheets.add_rows_with_children(SHEET_ID_TARGET, rows_to_add)
        logging.info(f"Successfully built Gantt chart with {len(rows_to_add)} top-level scopes.")

def main():
    """Main workflow to run the entire scheduling and building process."""
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    logging.info("Building column maps...")
    try:
        col_maps = {
            'total_poles': build_column_map(client.Sheets.get_sheet(SHEET_ID_TOTAL_POLES)),
            'phase_poles': build_column_map(client.Sheets.get_sheet(SHEET_ID_PHASE_POLES)),
            'target': build_column_map(client.Sheets.get_sheet(SHEET_ID_TARGET)),
        }
    except Exception as e:
        logging.error(f"FATAL: Failed to build column maps. Check Sheet IDs, permissions, and column names. Error: {e}")
        return
    logging.info("Column maps built successfully.")

    jobs_by_hierarchy, all_jobs = aggregate_data_from_sources(client, col_maps)
    allocate_poles(client, jobs_by_hierarchy, col_maps)
    perform_scheduling(all_jobs)
    build_gantt_from_scratch(client, jobs_by_hierarchy, col_maps)

if __name__ == "__main__":
    main()
