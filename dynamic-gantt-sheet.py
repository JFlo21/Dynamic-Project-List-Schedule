"""
Advanced Dynamic Project Scheduler for Smartsheet

- Handles multiple sheets and advanced schedule logic for crews, phases, and work requests
- Updates professional Gantt schedule with full hierarchy and cascading logic
- Fills in placeholders and recalculates all expected/actuals as real data arrives

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

# SHEET IDS
SHEET_ID_TOTAL_POLES = 8495204601384836
SHEET_ID_PHASE_POLES = 1553121697288068
SHEET_ID_TARGET      = 1847107938897796

# Column ID mapping (fill in all as you expand)
COLUMNS = {
    "scope_number": {
        "source_total": 3784709278224260,
        "source_phase": 3784709278224260,
        "target": 6392416854298500,
    },
    "scope_phase": {
        "source": 1865904432041860,
        "target": 4140617040613252,
    },
    "work_request": {
        "source": 6922793410842500,
        "target": 8644216667983748,
    },
    "assigned_resource": {
        "source": 7828491195862916,
        "target": 1945268683231108,
    },
    "job_placement": {
        "target": 7574768217444228,
    },
    "start_date_actual": {
        "source": 1089957554507652,
        "target": 481442343374724,
    },
    "expected_start_date": {
        "source": 4568417128107908,
        "target": 819368776388484,
    },
    "end_date_actual": {
        "source": 503555305459588,
        "target": 503555305459588,
    },
    "percent_complete": {
        "source": 1044421535289220,
        "target": 6448868310601604,
    },
    "pole_count_total": {
        "source": 2795493429825412,
    },
    "pole_count_phase": {
        "source": 2674900969672580,
        "target": 2674900969672580,
    },
    "expected_end_date": {
        "target": 3071168590073732,
    },
}

POLES_PER_DAY = 1.2
PLACEHOLDER_CREW = "Ramp-Up Crew (Estimate)"

# -------- LOGGING --------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# -------- DATA CLASSES --------
class Job:
    def __init__(self, scope, phase, wr, placement, crew, poles, row_id, is_placeholder=False):
        self.scope = scope
        self.phase = phase
        self.wr = wr
        self.placement = placement
        self.crew = crew
        self.poles = poles
        self.row_id = row_id
        self.is_placeholder = is_placeholder
        self.expected_start = None
        self.expected_end = None

    def duration(self):
        return int(math.ceil(self.poles / POLES_PER_DAY)) if self.poles else 0

class CrewSchedule:
    def __init__(self, name):
        self.name = name
        self.jobs = []

    def add_job(self, job):
        self.jobs.append(job)
        self.jobs.sort(key=lambda j: j.placement if j.placement is not None else 9999)

# -------- SHEET UTILITIES --------
def get_sheet(client, sheet_id):
    """Returns a list of row dicts with cell values keyed by column_id."""
    rows = []
    for row in client.Sheets.get_sheet(sheet_id).rows:
        cell_dict = {cell.column_id: cell.value for cell in row.cells}
        cell_dict['row_id'] = row.id
        cell_dict['parent_id'] = row.parent_id
        rows.append(cell_dict)
    return rows

# -------- POLE LOGIC --------
def get_scope_totals(total_poles_rows):
    """Returns {Scope #: total_poles}."""
    return {
        row.get(COLUMNS['scope_number']['source_total']): row.get(COLUMNS['pole_count_total']['source'])
        for row in total_poles_rows if row.get(COLUMNS['scope_number']['source_total'])
    }

def get_phase_pole_actuals(phase_poles_rows):
    """Returns {(Scope #, Scope Phase #): phase_poles} for assigned."""
    return {
        (row.get(COLUMNS['scope_number']['source_phase']), row.get(COLUMNS['scope_phase']['source'])):
        row.get(COLUMNS['pole_count_phase']['source'])
        for row in phase_poles_rows
        if row.get(COLUMNS['scope_number']['source_phase']) and row.get(COLUMNS['scope_phase']['source'])
    }

def allocate_unassigned_poles(scope_totals, phase_pole_actuals, all_phases):
    """Returns {(Scope #, Scope Phase #): pole_count} (real or estimated) for all phases."""
    results = dict(phase_pole_actuals)
    by_scope = defaultdict(list)
    for (scope, phase) in all_phases:
        by_scope[scope].append(phase)

    for scope, total in scope_totals.items():
        assigned = sum([phase_pole_actuals.get((scope, p), 0) or 0 for p in by_scope[scope]])
        unassigned_phases = [p for p in by_scope[scope] if (scope, p) not in phase_pole_actuals]
        if unassigned_phases:
            remainder = (total or 0) - assigned
            per_phase = int(math.ceil(remainder / len(unassigned_phases))) if len(unassigned_phases) > 0 else 0
            for phase in unassigned_phases:
                results[(scope, phase)] = per_phase
    return results

# -------- SCHEDULE ENGINE --------
def build_crews_and_jobs(target_rows, pole_assignments):
    """Returns {crew: CrewSchedule} and global job list."""
    crews = defaultdict(lambda: CrewSchedule(name=None))
    all_jobs = []

    for row in target_rows:
        scope = row.get(COLUMNS['scope_number']['target'])
        phase = row.get(COLUMNS['scope_phase']['target'])
        wr = row.get(COLUMNS['work_request']['target'])
        crew = row.get(COLUMNS['assigned_resource']['target']) or PLACEHOLDER_CREW
        placement = row.get(COLUMNS['job_placement']['target']) or 9999
        poles = pole_assignments.get((scope, phase)) or 0
        row_id = row['row_id']

        job = Job(scope, phase, wr, placement, crew, poles, row_id, is_placeholder=(crew == PLACEHOLDER_CREW))
        all_jobs.append(job)
        if not crews[crew].name:
            crews[crew].name = crew
        crews[crew].add_job(job)

    return crews, all_jobs

def schedule_crews(crews, crew_start_dates):
    """Updates jobs in place with expected_start and expected_end based on scheduling logic."""
    for crew, sched in crews.items():
        jobs = sorted(sched.jobs, key=lambda j: j.placement)
        cur_date = crew_start_dates.get(crew) or datetime.today()
        for job in jobs:
            job.expected_start = cur_date
            duration = job.duration()
            job.expected_end = cur_date + timedelta(days=duration - 1)
            cur_date = job.expected_end + timedelta(days=1)

# -------- MAIN WORKFLOW --------
def main():
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True) # Recommended for better error handling

    # 1. Load data
    logging.info("Loading data from Smartsheet...")
    total_poles_rows = get_sheet(client, SHEET_ID_TOTAL_POLES)
    phase_poles_rows = get_sheet(client, SHEET_ID_PHASE_POLES)
    target_rows = get_sheet(client, SHEET_ID_TARGET)
    logging.info("Data loaded.")

    # 2. Pole logic: Build assignment table for all phases
    logging.info("Allocating pole counts...")
    scope_totals = get_scope_totals(total_poles_rows)
    phase_pole_actuals = get_phase_pole_actuals(phase_poles_rows)
    all_phases = set()
    for row in target_rows:
        scope = row.get(COLUMNS['scope_number']['target'])
        phase = row.get(COLUMNS['scope_phase']['target'])
        if scope and phase:
            all_phases.add((scope, phase))
    pole_assignments = allocate_unassigned_poles(scope_totals, phase_pole_actuals, all_phases)
    logging.info("Pole counts allocated.")

    # 3. Build crew schedules and job lists
    logging.info("Building crew schedules...")
    crews, all_jobs = build_crews_and_jobs(target_rows, pole_assignments)
    logging.info("Schedules built.")

    # 4. Set crew start dates (placeholder: all start today; expand with real logic as needed)
    crew_start_dates = {crew: datetime.today() for crew in crews}
    # TODO: Replace above with your logic for alternating Thursday start, rotations, etc.

    # 5. Dynamic scheduling (jobs shift when jobs are inserted/moved/reassigned)
    logging.info("Calculating dynamic schedule dates...")
    schedule_crews(crews, crew_start_dates)
    logging.info("Schedule dates calculated.")

    # 6. Build Smartsheet update list, robustly skipping any columns with None
    logging.info("Preparing data for Smartsheet update...")
    updates = []
    for job in all_jobs:
        update_dict = {
            'row_id': job.row_id,
            str(COLUMNS['pole_count_phase']['target']): job.poles,
            str(COLUMNS['expected_start_date']['target']): job.expected_start.strftime('%Y-%m-%d') if job.expected_start else None,
            str(COLUMNS['expected_end_date']['target']): job.expected_end.strftime('%Y-%m-%d') if job.expected_end else None,
            str(COLUMNS['assigned_resource']['target']): job.crew,
        }
        # Remove any keys with column_id None or value None (robustness)
        updates.append({k: v for k, v in update_dict.items() if k != 'row_id' and k != 'None' and v is not None or k == 'row_id'})

    update_target_sheet(client, SHEET_ID_TARGET, updates)
    logging.info("Smartsheet schedule updated successfully.")

# -------- CORRECTED UPDATE FUNCTION --------
def update_target_sheet(client, sheet_id, updates):
    """Updates rows in Smartsheet with new values. Formats contact columns correctly."""
    batch = []
    # Get the column ID for the contact list for easy comparison
    contact_col_id = COLUMNS['assigned_resource']['target']

    for update in updates:
        row = smartsheet.models.Row()
        row.id = update['row_id']
        row.cells = []
        for col_id, val in update.items():
            if col_id == 'row_id' or col_id is None or col_id == 'None':
                continue

            # Create a new cell to be added to the row
            new_cell = {
                'column_id': int(col_id)
            }

            # If this is the contact column, use `objectValue` with the correct format
            if int(col_id) == contact_col_id:
                new_cell['objectValue'] = {
                    "objectType": "CONTACT",
                    "name": str(val)  # 'val' is the crew name string
                }
            # For all other column types, use the standard 'value'
            else:
                new_cell['value'] = val
            
            row.cells.append(new_cell)

        if row.cells: # Only add the row to the batch if it has cells to update
            batch.append(row)

    if batch:
        logging.info(f"Updating {len(batch)} rows in the target sheet.")
        client.Sheets.update_rows(sheet_id, batch)
    else:
        logging.info("No updates needed for target sheet.")


if __name__ == "__main__":
    # A simple try/except block at the top level to catch any exceptions
    try:
        main()
    except smartsheet.exceptions.ApiError as e:
        logging.error(f"Smartsheet API Error: {e.error.message}")
        logging.error(f"Error Code: {e.error.error_code}, Ref ID: {e.error.ref_id}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)