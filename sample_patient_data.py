import pandas as pd
import random
from datetime import datetime, timedelta

num_patients = 100
max_cases_per_patient = 10
case_types = ['benefit', 'TRIAGE', 'inbound', 'appeal', 'pap', 'prior_auth']
payer_names = ['Safari', 'Toyata', 'Nokia', 'Sony', 'Microsoft']
case_origins = ['FAX', 'Call', 'Mail', 'Email']
FRM_names = ['Bill Gates', 'ambani', 'Steve Jobs', 'Elon Musk', 'Jeff Bezos']

def generate_random_date(start_date, end_date):
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_patient_data(patient_id, num_cases):
    data = []
    for _ in range(num_cases):
        case_type = random.choice(case_types)
        payer_name = random.choice(payer_names)
        case_origin = random.choice(case_origins)
        frm = random.choice(FRM_names)
        case_id = f'c_{random.randint(0, 999):03d}_{case_type}'
        case_status = random.choice(['closed', 'cancelled', 'In progress'])
        case_open_date = generate_random_date(datetime(2024, 1, 1), datetime(2024, 6, 30)).strftime('%Y-%m-%d')
        case_open_time = f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}'
        case_closed_date = generate_random_date(datetime(2024, 1, 1), datetime(2024, 6, 30)).strftime('%Y-%m-%d')
        case_close_time = f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}'
        case_outcome = random.choice(['coverage not available', 'PA REQUIRED', 'approved'])
        Deniel_reason = random.choice([None, 'coverage not met', 'appeal required', 'dispatched'])
        Member_plan_id = f'MP{str(random.randint(1, 10)).zfill(3)}'
        Member_plan_Name = f'Plan_{chr(65 + random.randint(0, 9))}'

        data.append([patient_id, case_id, case_type, case_status, case_open_date, case_open_time, 
                     case_closed_date, case_close_time, case_outcome, Deniel_reason, Member_plan_id, 
                     Member_plan_Name, payer_name, case_origin, frm])
        # print(data)

    return data

data = []
for i in range(1, num_patients + 1):
    patient_id = f'p_{i:02d}'
    num_cases = random.randint(1, max_cases_per_patient) 
    data.extend(generate_patient_data(patient_id, num_cases))

df = pd.DataFrame(data, columns=[
    'patient_id', 'case_id', 'case_type', 'case_status', 'case_open_date', 'case_open_time', 'case_closed_date', 'case_close_time', 'case_outcome', 'Deniel_reason', 'Member_plan_id', 'Member_plan_tName', 'Payer_name', 'case_origin', 'FRM'])

df.to_csv('sample_medical_data.csv', index=False)
