import logging
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from vgcrk9.parse_rk9 import parse_tournament_players, get_tournament_pairings, db_transaction, append_df_to_table, parse_team_url
import glob
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import random
import time
from datetime import datetime





class vgcPlayers(BaseOperator):

    '''
    
    Get tournament players
    
    '''

    # input tournament URL name
    def __init__(self,tournament:str=None,
                 *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        self.tournament = tournament
        self.players = None
        self.tournament_info = None
        self.public_connection = 'postgres_local'


    def _get_player_table(self, context: Context):

        """
        
        Extract data from player roster
        
        """

        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_local')
            conn = pg_hook.get_conn()
        except Exception as e:
            self.log.error(f"Error connecting to DB: {e}")
            raise

        engine = pg_hook.get_sqlalchemy_engine()

        # extract tournament roster (players)
        self.players, self.tournament_info = parse_tournament_players(self.tournament)

        self.players = self.players.astype('str')
        self.players.columns = self.players.columns.str.lower()
        self.players['standing'] = self.players['standing'].fillna(-1)
        self.tournament_name = self.tournament_info['event']

        db_transaction(engine,f"delete from vgc.players p where p.event = '{self.tournament_name}'")
        append_df_to_table(conn, self.players, 'vgc.players')   



    def execute(self, context: Context):

        ti = context['ti'] 
        
        # extract data from 
        self._get_player_table(context)

        self.log.info(self.tournament_info)
        self.log.info('done!')

        # xcom push the team sheet urls and 
        # general tournament data
        data = {
            'urls': self.players['url'].to_list(),
            'tournament_info': self.tournament_info
        }
        return data




class vgcTeams(BaseOperator):

    '''
    
    Get Tournament Teams
    
    '''

    # input tournament URL name
    def __init__(self,tournament:str=None,
                 *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        self.tournament = tournament
        self.teams = None

    def _get_player_teams(self,context:Context):

        ti = context['ti'] 

        # xcom data pull from previous task 
        self.tournament_info = ti.xcom_pull(  
            task_ids='get_tournament_players', 
            key='return_value'
        )['tournament_info']

        # list of team urls 
        self.urls = ti.xcom_pull(  
            task_ids='get_tournament_players', 
            key='return_value'
        )['urls']

        self.log.info('pulled xcome from previous task!')
        self.tournament_name = self.tournament_info['event']


        temp = ['/teamlist/public/' + self.tournament + '/' + i for i in self.urls]
        self.log.info(f'{len(temp)} team urls found!')
        
        if(list(set(temp))[0] != ''):
        
            lst_teams = []
            for ii,team in enumerate(temp):
                if(ii%50 == 0):
                    print(f'Iteration {ii}')
                lst_teams.append(parse_team_url(team))
                delay = random.uniform(1.02,1.17)
                time.sleep(delay)
            
            teams = pd.concat(lst_teams)
            teams['moves'] = teams['moves'].apply(lambda row: sorted(row)) # sort moves by alphabet 
            teams[['Move 1','Move 2','Move 3','Move 4']] = teams['moves'].apply(pd.Series).add_prefix('Move ')
            teams = teams.drop(['moves'],axis=1)
            teams = teams[['name','tera_type','ability','item','Move 1','Move 2','Move 3','Move 4','URL']].copy()
            teams['Event'] = self.tournament_name
            teams['URL'] = teams['URL'].apply(lambda row: row.split('/')[-1])
            teams['Added'] = datetime.now()
            teams.columns = ['Name','Tera_type','Ability','Item','Move_1','Move_2','Move_3','Move_4','URL','Event','Added']
        
        else:
            print('no teams found!')
            teams = None

        # # extract player tournament teams from roster
        # self.teams = get_tournament_teams(self.tournament,self.tournament_info,self.urls)

        self.log.info('Parsed player teams!')
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_local')
            conn = pg_hook.get_conn()
        except Exception as e:
            self.log.error(f"Error connecting to DB: {e}")
            raise

        engine = pg_hook.get_sqlalchemy_engine()


        self.teams = teams.astype('str')
        self.teams.columns = self.teams.columns.str.lower()
        db_transaction(engine,f"delete from vgc.teams p where p.event = '{self.tournament_name}'")
        append_df_to_table(conn, self.teams, 'vgc.teams')   

        self.log.info('Parsed data written to DB')

        
    def execute(self, context: Context):

        ti = context['ti'] 
        
        self._get_player_teams(context)
        self.log.info('done!')



class vgcPairings(BaseOperator):

    '''
    
    Get player pairings for each tournament
    
    '''

    # input tournament URL name
    def __init__(self,tournament:str=None,
                 *args, **kwargs):
        
        super().__init__(*args, **kwargs)
        self.tournament = tournament
        self.pairings = None



    def _get_player_pairings(self,context:Context):

        ti = context['ti'] 

        # xcom data pull from previous task 
        self.tournament_info = ti.xcom_pull(  
            task_ids='get_tournament_players', 
            key='return_value'
        )['tournament_info']

        self.tournament_name = self.tournament_info['event']

        pairings = get_tournament_pairings(self.tournament,self.tournament_info)


        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres_local')
            conn = pg_hook.get_conn()
        except Exception as e:
            self.log.error(f"Error connecting to DB: {e}")
            raise

        engine = pg_hook.get_sqlalchemy_engine()

        self.pairings = pairings.astype('str')
        self.pairings.columns = self.pairings.columns.str.lower()
        self.pairings['added'] = datetime.now()
        db_transaction(engine,f"delete from vgc.pairings p where p.event = '{self.tournament_name}'")
        append_df_to_table(conn, self.pairings, 'vgc.pairings')

        self.log.info('Parsed data written to DB')



    def execute(self, context: Context):

        ti = context['ti'] 
        
        # extract data from 
        self._get_player_pairings(context)
        self.log.info('done!')      
