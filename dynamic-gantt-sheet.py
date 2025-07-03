"""
Dynamic Gantt Scheduling System - V9 (Multi-Sheet Aggregation)

- FINAL & CORRECTED: Implements a full multi-sheet data aggregation strategy.
- It now correctly uses the 'Work Request #' to look up and join data from all source sheets.
- The logic correctly sources Scope, Phase, WR, Resources, and Pole Counts from their respective sheets.
- Builds the full 3-level hierarchy from scratch on each run.
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
SHEET_ID_MASTER_TASK_LIST = 1553121697288068 # The primary source for the list of all tasks
SHEET_ID_RESOURCE_LOOKUP = 3733355007790980  # The source for resource assignments
SHEET_ID_TOTAL_POLES = 8495204601384836      # The source for high-level pole counts
SHEET_ID_TARGET = 1847107938897796           # The blank sheet for the Gantt chart

# --- COLUMN NAMES (Derived from your JSON data and clarifications) ---
COLUMN_NAMES = {
    # Names for the master task list (SHEET_ID_MASTER_TASK_LIST)
    "task_scope": "Scope #",
    "task_phase": "Scope ID #", # This is the Scope Phase #
    "task_wr": "Work Request #",
    "task_poles": "Total Poles", # Specific pole count for the task, if available

    # Names for the resource lookup sheet (SHEET_ID_RESOURCE_LOOKUP)
    "lookup_wr": "Work Request #", # This is the key to match on
    "lookup_resource": "Foreman Assigned + General Foreman", # This is the value to get
    
    # Names for the total poles sheet
    "total_poles_scope": "Job ID", # This is the Scope # on this sheet
    "total_poles_count": "total # poles on the project to instal wire on",
    
    # Names for the columns in the final target Gantt sheet
    "target_primary": "Work Request #", # The primary column will now be WR # for clarity
    "target_scope": "Scope #",
    "target_phase": "Scope Phase #",
    "target_wr": "Work Request #",
    "target_resource": "Assigned Resource",
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
        self.poles = float(poles or 0)
        self.start_date, self.end_date = None, None

    def duration_days(self):
        return int(math.ceil(self.poles / POLES_PER_DAY)) if self.poles > 0 else 0

# -------- CORE LOGIC --------
def build_column_map(sheet_obj):
    """Creates a dictionary mapping column titles to column IDs, stripping whitespace."""
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

def build_lookup_maps(client, col_maps):
    """Builds lookup dictionaries from the resource and total poles sheets."""
    logging.info("Building lookup maps...")
    
    # Resource Lookup Map: {WR # -> Resource Name}
    resource_sheet = client.Sheets.get_sheet(SHEET_ID_RESOURCE_LOOKUP, include=['objectValue'])
    resource_map = col_maps['resource_lookup']
    wr_to_resource = {
        get_cell_value(row, resource_map, COLUMN_NAMES['lookup_wr']): get_cell_value(row, resource_map, COLUMN_NAMES['lookup_resource'])
        for row in resource_sheet.rows
    }

    # Total Poles Lookup Map: {Scope # -> Total Poles}
    total_poles_sheet = client.Sheets.get_sheet(SHEET_ID_TOTAL_POLES)
    total_poles_map = col_maps['total_poles']
    scope_to_poles = {
        get_cell_value(row, total_poles_map, COLUMN_NAMES['total_poles_scope']): float(get_cell_value(row, total_poles_map, COLUMN_NAMES['total_poles_count']) or 0)
        for row in total_poles_sheet.rows
    }
    
    logging.info(f"Built resource map for {len(wr_to_resource)} WRs and pole map for {len(scope_to_poles)} scopes.")
    return wr_to_resource, scope_to_poles

def aggregate_data(client, col_maps, wr_to_resource):
    """Reads the master task list and enriches it with data from the lookup maps."""
    logging.info("Aggregating data from master task list...")
    task_list_sheet = client.Sheets.get_sheet(SHEET_ID_MASTER_TASK_LIST)
    task_map = col_maps['task_list']
    
    all_jobs = []
    for row in task_list_sheet.rows:
        wr = get_cell_value(row, task_map, COLUMN_NAMES['task_wr'])
        if not wr: continue

        scope = get_cell_value(row, task_map, COLUMN_NAMES['task_scope'])
        phase = get_cell_value(row, task_map, COLUMN_NAMES['task_phase'])
        
        if not all([scope, phase]):
            logging.warning(f"Skipping WR '{wr}' due to missing Scope or Phase.")
            continue

        job = Job(
            wr=wr, scope=scope, phase=phase,
            crew=wr_to_resource.get(wr) or PLACEHOLDER_CREW,
            placement=get_cell_value(row, task_map, COLUMN_NAMES['target_placement']),
            poles=get_cell_value(row, task_map, COLUMN_NAMES['task_poles'])
        )
        all_jobs.append(job)
        
    logging.info(f"Aggregated {len(all_jobs)} total jobs.")
    return all_jobs

def allocate_poles(all_jobs, scope_to_poles):
    """Allocates pole counts for jobs with missing values."""
    if not all_jobs: return
    logging.info("Allocating pole counts...")
    
    jobs_by_scope = defaultdict(list)
    for job in all_jobs:
        jobs_by_scope[job.scope].append(job)

    for scope, jobs in jobs_by_scope.items():
        total_poles = scope_to_poles.get(scope, 0)
        if not total_poles: continue

        assigned_poles = sum(job.poles for job in jobs if job.poles)
        jobs_to_allocate = [job for job in jobs if not job.poles]
        
        if jobs_to_allocate:
            remaining_poles = total_poles - assigned_poles
            if remaining_poles > 0:
                poles_per_job = int(math.ceil(remaining_poles / len(jobs_to_allocate)))
                for job in jobs_to_allocate:
                    job.poles = poles_per_job

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

def build_gantt_from_scratch(client, all_jobs, col_maps):
    """Builds a 3-level Gantt chart from scratch in a single API call."""
    if not all_jobs:
        logging.warning("No data to build. Gantt sheet will be empty.")
        return
        
    logging.info(f"Rebuilding Gantt chart on sheet ID {SHEET_ID_TARGET}...")
    target_map = col_maps['target']
    
    try:
        sheet = client.Sheets.get_sheet(SHEET_ID_TARGET)
        if sheet.rows:
            client.Sheets.delete_rows(SHEET_ID_TARGET, [r.id for r in sheet.rows])
    except smartsheet.exceptions.ApiError as e:
        if e.error.error_code != 1006: raise e

    jobs_by_hierarchy = defaultdict(lambda: defaultdict(list))
    for job in all_jobs:
        jobs_by_hierarchy[job.scope][job.phase].append(job)

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
                primary_col_id = target_map[COLUMN_NAMES['target_primary']]
                cells = [{'column_id': primary_col_id, 'value': job.wr}]
                
                data_mapping = { 'scope': 'target_scope', 'phase': 'target_phase', 'wr': 'target_wr', 'poles': 'target_poles' }
                for attr, key in data_mapping.items():
                    col_id = target_map.get(COLUMN_NAMES[key])
                    if col_id and col_id != primary_col_id:
                        value = getattr(job, attr, None)
                        if value is not None: cells.append({'column_id': col_id, 'value': value})

                if job.start_date: cells.append({'column_id': target_map[COLUMN_NAMES['target_start']], 'value': job.start_date.strftime('%Y-%m-%d')})
                if job.end_date: cells.append({'column_id': target_map[COLUMN_NAMES['target_end']], 'value': job.end_date.strftime('%Y-%m-%d')})

                email = CREW_EMAILS.get(job.crew)
                if email: cells.append({'column_id': target_map[COLUMN_NAMES['target_resource']], 'objectValue': {'objectType': 'CONTACT', 'name': job.crew, 'email': email}})

                wr_row.cells = cells
                wr_rows.append(wr_row)
            
            phase_row.children = wr_rows
            phase_rows.append(phase_row)
        
        scope_row.children = phase_rows
        rows_to_add.append(scope_row)

    if rows_to_add:
        client.Sheets.add_rows(SHEET_ID_TARGET, rows_to_add)
        logging.info(f"Successfully sent request to build Gantt chart.")

def main():
    """Main workflow to run the entire scheduling and building process."""
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)
    
    logging.info("Building column maps...")
    try:
        col_maps = {
            'total_poles': build_column_map(client.Sheets.get_sheet(SHEET_ID_TOTAL_POLES)),
            'task_list': build_column_map(client.Sheets.get_sheet(SHEET_ID_MASTER_TASK_LIST)),
            'resource_lookup': build_column_map(client.Sheets.get_sheet(SHEET_ID_RESOURCE_LOOKUP)),
            'target': build_column_map(client.Sheets.get_sheet(SHEET_ID_TARGET)),
        }
    except Exception as e:
        logging.error(f"FATAL: Failed to build column maps. Check Sheet IDs, permissions, and column names. Error: {e}")
        return
    logging.info("Column maps built successfully.")

    wr_to_resource, scope_to_poles = build_lookup_maps(client, col_maps)
    all_jobs = aggregate_data(client, col_maps, wr_to_resource)
    allocate_poles(all_jobs, scope_to_poles)
    perform_scheduling(all_jobs)
    build_gantt_from_scratch(client, all_jobs, col_maps)

if __name__ == "__main__":
    main()
