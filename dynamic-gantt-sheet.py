"""
Advanced Dynamic Project Scheduler for Smartsheet

- V3: Dynamically builds a resource map from a dedicated sheet for contact creation.
- Handles multiple sheets and advanced schedule logic for crews, phases, and work requests.
- Updates professional Gantt schedule with full hierarchy and cascading logic.
- Fills in placeholders and recalculates all expected/actuals as real data arrives.

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
# NEW: The sheet that contains your master list of crews/resources
SHEET_ID_RESOURCE    = 3733355007790980 

# ---!!! ACTION REQUIRED: PLEASE UPDATE THE EMAIL COLUMN ID BELOW !!!---
# This email address will be used for your placeholder crew.
PLACEHOLDER_CREW_EMAIL = "placeholder.crew@yourcompany.com"

# Column ID mapping
COLUMNS = {
    # NEW: Define columns for your Resource Sheet
    "resource_map": {
        "name": 7828491195862916, # This is the column with the crew/person's name
        "email": 0, # <<<--- CRITICAL: YOU MUST REPLACE '0' with the real column ID for the resource's email address
    },
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
        # This is the 'Assigned Resource' column on your main Gantt sheet
        "target": 1945268683231108,
    },
    "job_placement": {
        "target": 7574768217444228,
    },
    "expected_start_date": {
        "target": 819368776388484,
    },
    "expected_end_date": {
        "target": 3071168590073732,
    },
    "pole_count_phase": {
        "source": 2674900969672580,
        "target": 2674900969672580,
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
    sheet = client.Sheets.get_sheet(sheet_id)
    rows = []
    for row in sheet.rows:
        cell_dict = {cell.column_id: cell.value for cell in row.cells}
        cell_dict['row_id'] = row.id
        rows.append(cell_dict)
    return rows

# --- NEW FUNCTION ---
def build_resource_map(client):
    """Reads the resource sheet and builds a map of {crew_name: email}."""
    logging.info("Building resource map from resource sheet...")
    name_col_id = COLUMNS['resource_map']['name']
    email_col_id = COLUMNS['resource_map']['email']

    if email_col_id == 0:
        raise ValueError("FATAL: Please update the placeholder 'email' column ID in COLUMNS['resource_map'] before running.")

    resource_sheet = get_sheet(client, SHEET_ID_RESOURCE)
    resource_map = {}
    for row in resource_sheet:
        name = row.get(name_col_id)
        email = row.get(email_col_id)
        if name and email:
            resource_map[name] = email
    
    # Manually add the placeholder crew to the map
    resource_map[PLACEHOLDER_CREW] = PLACEHOLDER_CREW_EMAIL
    
    logging.info(f"Resource map built successfully with {len(resource_map)} entries.")
    return resource_map

# -------- POLE LOGIC (No changes needed) --------
def get_scope_totals(total_poles_rows):
    return {r.get(COLUMNS['scope_number']['source_total']): r.get(c) for r in total_poles_rows if r.get(COLUMNS['scope_number']['source_total'])}

def get_phase_pole_actuals(phase_poles_rows):
    return {(r.get(COLUMNS['scope_number']['source_phase']), r.get(COLUMNS['scope_phase']['source'])): r.get(COLUMNS['pole_count_phase']['source']) for r in phase_poles_rows if r.get(COLUMNS['scope_number']['source_phase']) and r.get(COLUMNS['scope_phase']['source'])}

def allocate_unassigned_poles(scope_totals, phase_pole_actuals, all_phases):
    results = dict(phase_pole_actuals)
    by_scope = defaultdict(list)
    for (scope, phase) in all_phases: by_scope[scope].append(phase)
    for scope, total in scope_totals.items():
        if total is None: continue
        assigned = sum([phase_pole_actuals.get((scope, p), 0) or 0 for p in by_scope[scope]])
        unassigned_phases = [p for p in by_scope[scope] if (scope, p) not in phase_pole_actuals]
        if unassigned_phases:
            remainder = total - assigned
            if remainder > 0:
                per_phase = int(math.ceil(remainder / len(unassigned_phases)))
                for phase in unassigned_phases: results[(scope, phase)] = per_phase
    return results

# -------- SCHEDULE ENGINE (No changes needed) --------
def build_crews_and_jobs(target_rows, pole_assignments):
    crews = defaultdict(lambda: CrewSchedule(name=None))
    all_jobs = []
    for row in target_rows:
        scope = row.get(COLUMNS['scope_number']['target'])
        phase = row.get(COLUMNS['scope_phase']['target'])
        if not scope or not phase: continue
        wr = row.get(COLUMNS['work_request']['target'])
        crew = row.get(COLUMNS['assigned_resource']['target']) or PLACEHOLDER_CREW
        placement = row.get(COLUMNS['job_placement']['target']) or 9999
        poles = pole_assignments.get((scope, phase), 0)
        job = Job(scope, phase, wr, placement, crew, poles, row['row_id'], is_placeholder=(crew == PLACEHOLDER_CREW))
        all_jobs.append(job)
        if not crews[crew].name: crews[crew].name = crew
        crews[crew].add_job(job)
    return crews, all_jobs

def schedule_crews(crews, crew_start_dates):
    for crew, sched in crews.items():
        jobs = sorted(sched.jobs, key=lambda j: j.placement)
        cur_date = crew_start_dates.get(crew) or datetime.today()
        for job in jobs:
            duration = job.duration()
            if duration > 0:
                job.expected_start = cur_date
                job.expected_end = cur_date + timedelta(days=duration - 1)
                cur_date = job.expected_end + timedelta(days=1)

# -------- MAIN WORKFLOW (MODIFIED) --------
def main():
    client = smartsheet.Smartsheet(API_TOKEN)
    client.errors_as_exceptions(True)

    # 1. Build the resource map FIRST
    resource_map = build_resource_map(client)

    # 2. Load data from project sheets
    logging.info("Loading data from project sheets...")
    total_poles_rows = get_sheet(client, SHEET_ID_TOTAL_POLES)
    phase_poles_rows = get_sheet(client, SHEET_ID_PHASE_POLES)
    target_rows = get_sheet(client, SHEET_ID_TARGET)
    logging.info("Data loaded.")

    # 3. Pole logic
    logging.info("Allocating pole counts...")
    all_phases = set((r.get(COLUMNS['scope_number']['target']), r.get(COLUMNS['scope_phase']['target'])) for r in target_rows if r.get(COLUMNS['scope_number']['target']) and r.get(COLUMNS['scope_phase']['target']))
    pole_assignments = allocate_unassigned_poles(get_scope_totals(total_poles_rows), get_phase_pole_actuals(phase_poles_rows), all_phases)
    logging.info("Pole counts allocated.")

    # 4. Build crew schedules and job lists
    logging.info("Building crew schedules...")
    crews, all_jobs = build_crews_and_jobs(target_rows, pole_assignments)
    logging.info("Schedules built.")

    # 5. Dynamic scheduling
    logging.info("Calculating dynamic schedule dates...")
    schedule_crews(crews, {crew: datetime.today() for crew in crews})
    logging.info("Schedule dates calculated.")

    # 6. Build Smartsheet update list
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
        updates.append({k: v for k, v in update_dict.items() if k != 'row_id' and v is not None or k == 'row_id'})

    # 7. Update the target sheet, passing the resource_map
    update_target_sheet(client, SHEET_ID_TARGET, updates, resource_map)
    logging.info("Smartsheet schedule updated successfully.")

# -------- UPDATE FUNCTION (MODIFIED) --------
def update_target_sheet(client, sheet_id, updates, resource_map):
    """Updates rows in Smartsheet, using the resource_map to create contacts."""
    batch = []
    contact_col_id = COLUMNS['assigned_resource']['target']

    for update in updates:
        row = smartsheet.models.Row()
        row.id = update['row_id']
        row.cells = []
        for col_id_str, val in update.items():
            if col_id_str == 'row_id' or val is None:
                continue
            
            col_id = int(col_id_str)
            new_cell = {'column_id': col_id}

            if col_id == contact_col_id:
                crew_name = str(val)
                crew_email = resource_map.get(crew_name)
                if crew_email:
                    new_cell['objectValue'] = {
                        "objectType": "CONTACT",
                        "name": crew_name,
                        "email": crew_email
                    }
                else:
                    logging.warning(f"No email found in resource map for crew '{crew_name}' on row {row.id}. Skipping assignment.")
                    continue 
            else:
                new_cell['value'] = val
            
            row.cells.append(new_cell)

        if row.cells:
            batch.append(row)

    if batch:
        logging.info(f"Updating {len(batch)} rows in the target sheet.")
        client.Sheets.update_rows(sheet_id, batch)
    else:
        logging.info("No rows required updates in the target sheet.")

# -------- MAIN EXECUTION BLOCK (MODIFIED) --------
if __name__ == "__main__":
    try:
        main()
    except ValueError as e:
        logging.error(e)
    except smartsheet.exceptions.ApiError as e:
        logging.error(f"Smartsheet API Error: {e.result.message}")
        logging.error(f"Error Code: {e.result.errorCode}, Ref ID: {e.result.refId}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)