import psycopg2
import subprocess
import time
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-6s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    level=logging.INFO,
)




# Database connection parameters - update these with your details
db_params = {
    "dbname": "airflow_db",
    "user": "flevin",
    "password": "myuser",
    "host": "localhost"
}

def kill_matching_processes(dag_id):
    # Use ps and grep to find processes with names matching the dag_id and "task supervisor"
    process_list_command = f"ps aux | grep '{dag_id}' | grep 'task supervisor' | grep -v grep"
    processes = subprocess.run(process_list_command, shell=True, text=True, capture_output=True)
    logging.info('Found the following processes')
    logging.info(processes)

    for line in processes.stdout.splitlines():
        # Extracting process ID (PID) assuming standard ps output where PID is the second column
        columns = line.split()
        if len(columns) > 1:
            pid = columns[1]
            try:
                # Kill the process
                command = f"kill -9 {pid}"
                logging.info(f'Executing: {command}')
                subprocess.run(command, shell=True)
                logging.info(f"Killed process {pid} for DAG {dag_id}")
                # Sleep for 1 second between kills
                time.sleep(1)
            except Exception as e:
                logging.info(f"Error killing process {pid}: {e}")

# Connect to the Airflow Postgres database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Query to find running DAGs with last_scheduling_decision >= current_date
query = """
SELECT dag_id, execution_date
FROM dag_run
WHERE state = 'running' AND last_scheduling_decision >= current_date;
"""

try:
    # Execute the query
    cur.execute(query)
    
    # Fetch all running DAGs
    running_dags = cur.fetchall()

    # Loop through each running DAG and kill its matching processes
    if running_dags:
        for dag_id, execution_date in running_dags:
            kill_matching_processes(dag_id)
    else:
        logging.info('No running dags')

finally:
    # Close the database connection
    cur.close()
    conn.close()