import os
import math
import smartsheet
import logging
from collections import defaultdict
from datetime import datetime, timedelta

# --------------- CONFIG ---------------
API_TOKEN = os.getenv('SMARTSHEET_API_TOKEN')

# Sheet IDs (from your list)
SHEET_IDS = {
    'target': 1847107938897796,
    'total_poles': 8495204601384836,
    'phases': 1553121697288068,
    'crew_assign': 5723337641643908,  # Sheet with 'Foreman To Assign To?' column
    # Add others as needed...
}

# --------------- COLUMN ID MAPS (simplified sample) ---------------
COLUMN_MAP = {
    # Target sheet
    'scope_number_target': 6392416854298500,
    'scope_phase_target': 4140617040613252,
    'work_request_target': 8644216667983748,
    'assigned_resource_target': 937765308682116,  # Foreman To Assign To?
    'pole_count_days_target': 2674900969672580,   # Pole count by phase
    'expected_start_target': 819368776388484,
    'expected_end_target': 3071168590073732,
    'percent_complete_target': 6448868310601604,
    'actual_start_target': 481442343374724,
    'actual_end_target': 503555305459588,

    # Source sheets
    'scope_number': 3784709278224260,
    'scope_phase': 1865904432041860,
    'work_request': 6922793410842500,
    'hardening_pole_count': 5047293243510660,
    'non_hardening_pole_count': 2795493429825412,
    'foreman_to_assign_to': 937765308682116,
    'percent_complete': 1044421535289220,
    'actual_start': 1089957554507652,
    'actual_end': 503555305459588,
    # Add more as needed...
}

POLES_PER_DAY = 1.2

# --------------- LOGGING ---------------
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

# --------------- DATA LOAD HELPERS ---------------
def get_sheet(client, sheet_id):
    sheet = client.Sheets.get_sheet(sheet_id)
    rows = []
    for row in sheet.rows:
        cell_dict = {cell.column_id: cell.value for cell in row.cells}
        cell_dict['row_id'] = row.id
        rows.append(cell_dict)
    return rows

def get_total_poles(row):
    # Prefer Hardening, fallback to Non-Hardening
    val = row.get(COLUMN_MAP['hardening_pole_count'])
    if val is not None:
        return float(val)
    val = row.get(COLUMN_MAP['non_hardening_pole_count'])
    return float(val) if val is not None else 0

# --------------- CORE SCHEDULING LOGIC ---------------
class Job:
    def __init__(self, scope, phase, wr, crew, placement, poles, row_id):
        self.scope = scope
        self.phase = phase
        self.wr = wr
        self.crew = crew
        self.placement = placement
        self.poles = poles
        self.row_id = row_id
        self.expected_start = None
        self.expected_end = None

    def duration(self):
        return int(math.ceil(float(self.poles) / POLES_PER_DAY)) if self.poles else 0

def build_jobs(target_rows, pole_lookup, crew_lookup):
    jobs = []
    for row in target_rows:
        scope = row.get(COLUMN_MAP['scope_number_target'])
        phase = row.get(COLUMN_MAP['scope_phase_target'])
        wr = row.get(COLUMN_MAP['work_request_target'])
        placement = row.get('placement', 9999)  # You may have to adjust for your real field
        row_id = row['row_id']
        # Get pole count (by phase, else by scope, else 0)
        poles = pole_lookup.get((scope, phase), 0)
        crew = crew_lookup.get(wr, "UNASSIGNED")
        jobs.append(Job(scope, phase, wr, crew, placement, poles, row_id))
    return jobs

def schedule_jobs(jobs):
    # Group jobs by crew, schedule sequentially by placement
    jobs_by_crew = defaultdict(list)
    for job in jobs:
        jobs_by_crew[job.crew].append(job)
    for crew, crew_jobs in jobs_by_crew.items():
        crew_jobs.sort(key=lambda j: j.placement)
        current = datetime.today()
        for job in crew_jobs:
            job.expected_start = current
            dur = job.duration()
            job.expected_end = current + timedelta(days=dur-1)
            current = job.expected_end + timedelta(days=1)

def build_pole_lookup(total_poles_rows, phase_rows):
    # Returns {(scope, phase): poles}, and (scope): total_poles
    poles_by_scope = {}
    for row in total_poles_rows:
        scope = row.get(COLUMN_MAP['scope_number'])
        if not scope:
            continue
        poles_by_scope[scope] = get_total_poles(row)
    poles_by_phase = {}
    for row in phase_rows:
        scope = row.get(COLUMN_MAP['scope_number'])
        phase = row.get(COLUMN_MAP['scope_phase'])
        if scope and phase:
            poles_by_phase[(scope, phase)] = row.get(COLUMN_MAP['pole_count_days_target'], 0)
    return poles_by_scope, poles_by_phase

def build_crew_lookup(crew_rows):
    # Returns {Work Request #: crew}
    lookup = {}
    for row in crew_rows:
        wr = row.get(COLUMN_MAP['work_request'])
        crew = row.get(COLUMN_MAP['foreman_to_assign_to'])
        if wr and crew:
            lookup[wr] = crew
    return lookup

# --------------- UPDATE TARGET SHEET ---------------
def update_target_sheet(client, jobs):
    updates = []
    for job in jobs:
        updates.append({
            'row_id': job.row_id,
            str(COLUMN_MAP['assigned_resource_target']): job.crew,
            str(COLUMN_MAP['expected_start_target']): job.expected_start.strftime('%Y-%m-%d') if job.expected_start else None,
            str(COLUMN_MAP['expected_end_target']): job.expected_end.strftime('%Y-%m-%d') if job.expected_end else None,
            str(COLUMN_MAP['pole_count_days_target']): job.poles,
        })
    batch = []
    for update in updates:
        row = smartsheet.models.Row()
        row.id = update['row_id']
        row.cells = []
        for col_id, val in update.items():
            if col_id == 'row_id':
                continue
            if val is not None:
                row.cells.append({'column_id': int(col_id), 'value': val})
        batch.append(row)
    if batch:
        client.Sheets.update_rows(SHEET_IDS['target'], batch)

# --------------- MAIN PIPELINE ---------------
def main():
    client = smartsheet.Smartsheet(API_TOKEN)
    target_rows = get_sheet(client, SHEET_IDS['target'])
    total_poles_rows = get_sheet(client, SHEET_IDS['total_poles'])
    phase_rows = get_sheet(client, SHEET_IDS['phases'])
    crew_rows = get_sheet(client, SHEET_IDS['crew_assign'])

    poles_by_scope, poles_by_phase = build_pole_lookup(total_poles_rows, phase_rows)
    crew_lookup = build_crew_lookup(crew_rows)
    # Combine logic for poles for each job row (prefer by phase, else by scope)
    pole_lookup = {}
    for row in target_rows:
        scope = row.get(COLUMN_MAP['scope_number_target'])
        phase = row.get(COLUMN_MAP['scope_phase_target'])
        # Try by phase
        poles = poles_by_phase.get((scope, phase))
        if not poles:
            poles = poles_by_scope.get(scope, 0)
        pole_lookup[(scope, phase)] = poles
    jobs = build_jobs(target_rows, pole_lookup, crew_lookup)
    schedule_jobs(jobs)
    update_target_sheet(client, jobs)
    logging.info("Gantt schedule updated.")

if __name__ == "__main__":
    main()