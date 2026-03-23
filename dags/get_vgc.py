from vgcrk9.vgcOperator import vgcPlayers,vgcTeams,vgcPairings
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime

# tournament_id
tournament = 'SY02sMDp6JmCcnCynSLn'

# default arguments for all dags
DEFAULT_ARGS = {
    'owner': 'admin',
    'start_date': datetime(2026,1,1),   # DAG Start Window
    'end_date': datetime(2026,5,2),      # Expiration Time of DAG
    'retry_delay': timedelta(minutes=2),  # Wait 2 min between retries
    'max_retry_delay': timedelta(minutes=10),  # Max wait time
    'retry_exponential_backoff': True  # 2min → 4min → 8min
}

# main DAG
dag = DAG(
    "parse_players",
    default_args=DEFAULT_ARGS,
    schedule_interval=timedelta(hours=1),  # call hourly after "start_time"
    max_active_runs=1,
    tags=['vgc','parse event']
)

# get tournament player roster
get_player_data = vgcPlayers(
    task_id='get_tournament_players',
    tournament=tournament,
    dag=dag
)

# get url of team sheets from player roster
get_team_data = vgcTeams(
    task_id='get_tournament_teams',
    tournament=tournament,
    dag=dag
)

# get tournament pairings
get_pairings_data = vgcPairings(
    task_id='get_tournament_pairings',
    tournament=tournament,
    dag=dag
)

ending = DummyOperator(task_id='dag_complete', dag=dag)

# process
get_player_data >> get_team_data >> get_pairings_data >> ending
