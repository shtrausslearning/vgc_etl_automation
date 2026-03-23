import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import random
import time
from datetime import datetime
import psycopg2
import psycopg2.extras as extras
import numpy as np
from sqlalchemy import create_engine, text



def parse_team_url(url):

    '''

    Given a URL parsed from registed player sheet extract the registed team

    '''

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    response = requests.get('https://rk9.gg' + url, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    pokemon_divs = soup.find_all("div", 
                                 class_="pokemon bg-light-green-50 p-3")

    team_data = []
    for div in pokemon_divs:

        # Skip non-EN Pokémon
        if not div.find("b", string="EN"):
            continue

        name = None
        contents = list(div.children)
        for i, content in enumerate(contents):
            if isinstance(content, str) and content.strip():
                # Skip whitespace, img alt text, and grab first meaningful Pokémon name
                cleaned = re.sub(r'\s+', ' ', content.strip())
                if len(cleaned) > 1 and not cleaned.startswith(('Tera', 'Ability', 'EN')):
                    name = cleaned
                    break

        # Extract moves from badge spans (ALWAYS WORKS)
        moves = [span.get_text().strip() for span in div.find_all("span", class_="badge")]

        # Extract structured data - direct text after each <b> label
        tera_type = ability = item = None

        for b_tag in div.find_all("b"):
            label = b_tag.get_text(strip=True)
            next_text = b_tag.next_sibling
            if next_text and isinstance(next_text, str):
                value = next_text.strip()
            else:
                value = ""
                sibling = b_tag.next_sibling
                while sibling and value == "":
                    if isinstance(sibling, str) and sibling.strip():
                        value = sibling.strip()
                    sibling = sibling.next_sibling

            if label == "Tera Type:" and value:
                tera_type = value.split()[0]
            elif label == "Ability:" and value:
                ability = value.strip()
            elif label == "Held Item:" and value:
                item = value.strip()

        team_data.append({
            "name": name or "Unknown",
            "tera_type": tera_type or "Unknown",
            "ability": ability or "Unknown", 
            "item": item or "Unknown",
            "moves": moves,
            "URL": url
        })

    return pd.DataFrame(team_data)


def parse_tournament_players(tournament_url:str) -> pd.DataFrame:

    '''

    Extract the tournament players

    '''
    
    url = 'https://rk9.gg/roster/' + tournament_url
    
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    
    # Extract critical information
    event_name = soup.find('h4').get_text(strip=True)
    roster_title = soup.find('h3', class_='mb-0').get_text(strip=True)
    
    # Extract event date and timezone
    date_section = soup.find('h5', class_='my-0')
    event_date_full = date_section.find('br').previous_sibling.strip()
    timezone_match = soup.find('span', class_='badge-light')
    timezone = timezone_match.get_text(strip=True) if timezone_match else "Unknown"
    
    # Extract deadlines using regex for precise parsing
    deadline_section = soup.find('p', class_='my-0')
    deadline_text = deadline_section.get_text()
    
    try:
        registration_close = re.search(r'Registration closes:\s*(\d{4}-\d{2}-\d{2}\s+at\s+\d{1,2}:\d{2}\s+[AP]M)', deadline_text).group(1)
        team_submission_close = re.search(r'Team list submission closes:\s*(\d{4}-\d{2}-\d{2}\s+at\s+\d{1,2}:\d{2}\s+[AP]M)', deadline_text).group(1)
    except:
        registration_close = None
        team_submission_close = None
    
    # Structured output
    critical_info = {
        'event': event_name,
        'roster_section': roster_title,
        'event_date': event_date_full,
        'timezone': timezone,
        'registration_deadline': registration_close,
        'team_submission_deadline': team_submission_close
    }
    
    print("Critical Information Extracted:")
    for key, value in critical_info.items():
        print(f"{key.replace('_', ' ').title()}: {value}")
    
    table = soup.find('table')  # Adjust selector if needed: class_='roster-table'
    rows = table.find_all('tr')[1:]  # Skip header
    header = table.find_all('tr')[0] 
    cols = header.find_all(['td', 'th'])
    col_headers = [col.get_text(strip=True) for col in cols]
    try:
        team_col = col_headers.index('Team List')
    except:
        team_col = None
    ncols = len(col_headers)
    
    # col_headers = ['Player ID', 'First name', 'Last name', 'Country', 'Division', 'Trainer name', 'Team List', 'Standing']
    
    data = []
    for row in rows:
        
        cols = row.find_all(['td', 'th'])
        
        row_data = {}
        for i, col in enumerate(cols):
            try:
                if i == team_col:  # Team List column index
                    links = col.find_all('a', href=True)
                    if links:
                        row_data['Team list'] = ', '.join([link.get_text(strip=True) for link in links])
                        row_data['URL'] = ', '.join([link['href'] for link in links])  # Full URLs
                    else:
                        row_data['Team list'] = col.get_text(strip=True)
                        row_data['URL'] = ''
                else:
                    row_data[col_headers[i]] = col.get_text(strip=True)
            except:
                row_data[col_headers[i]] = None
        
        data.append(row_data)
        
        # players
    players = pd.DataFrame(data)
    players['Event'] = critical_info['event']
    players['URL'] = players['URL'].apply(lambda row: row.split('/')[-1])
    
    mapper = {'Player ID':'Player_ID','First name':'First_name','Last name':'Last_name','Trainer name':'Trainer_name','Team list':'Team_list'}
    players = players.rename(columns=mapper)
    players['Player'] = players['First_name'] + ' ' + players['Last_name']  + ' [' + players['Country'] + ']'
    
    if('Standing' not in players.columns):
        players['Standing'] = None
        players = players[['Player_ID','First_name','Last_name','Country','Division','Trainer_name','Team_list','URL','Standing','Event']].copy()
    else:
        players = players[['Player_ID','First_name','Last_name','Country','Division','Trainer_name','Team_list','URL','Standing','Event']].copy()
    
    players['Player'] = players['First_name'] + ' ' + players['Last_name'] + ' ['  + players['Country'] + ']'
    players['Added'] = datetime.now()

    print(f'{players.shape[0]} players attended event {critical_info['event']} 🎉')

    return players, critical_info



def get_tournament_pairings(tournament_url:str,tournament_info: dict) -> pd.DataFrame:

    try:
        event_name = tournament_info['event']
    except:
        event_name = None

    def extract_all_round_pairings(base_url):
        
        global event_name
        
        """Extract ALL round URLs from hx-get attributes"""
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract ALL elements with class="mb-0"
        mb0_elements = soup.find_all(class_='mb-0')
    
        tournament_title = mb0_elements[0].get_text(strip=True)  # "Tournament Pairings"
        event_name = mb0_elements[1].get_text(strip=True)       
        
        print(f"Title: {tournament_title}")
        print(f"Event: {event_name}")
        
        round_data = []
        tab_panes = soup.find_all('div', class_='tab-pane')
        
        for tab_pane in tab_panes:
            tab_id = tab_pane.get('id', '')
            if re.search(r'P\d+R\d+', tab_id):
                hx_div = tab_pane.find('div', attrs={'hx-get': True})
                if hx_div:
                    hx_url = hx_div.get('hx-get')
                    full_url = f"https://rk9.gg{hx_url}"
                    
                    pod_match = re.search(r'P(\d+)R(\d+)', tab_id)
                    if pod_match:
                        round_data.append({
                            'pod': pod_match.group(1),
                            'round': pod_match.group(2),
                            'url': full_url
                        })
        return round_data
        


    def extract_player_data(player_div):
        
        """Enhanced extraction - parse name, record AND points"""
        name_span = player_div.find('span', class_='name')
        name = name_span.get_text(strip=False) if name_span else ''
        name = re.sub('\n',' ',name)
        
        full_text = player_div.get_text(strip=False)
        # full_text = player_div
        
        # Extract points (look for "15 pts" pattern)
        points_match = re.search(r'(\d+)\s*pts', full_text)
        points = int(points_match.group(1)) if points_match else 0
        
        # Extract record (look for "X-Y-Z" pattern before points)
        record_match = re.search(r'\((\d+-\d+-\d+)\)', full_text)
        record = record_match.group(1) if record_match else ''
        
        # Clean up record text
        if record and name:
            record_text = full_text.replace(name, '').replace(f'({record})', '').replace(f'{points} pts', '').strip()
        else:
            record_text = ''
        
        classes = player_div.get('class', [])
        is_winner = 'winner' in classes
        is_loser = 'loser' in classes
        
        return name, record, record_text, points, is_winner, is_loser
    
    def extract_pairings_from_round(round_url):
        
        """Simplified extraction with points extraction"""
        response = requests.get(round_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pairings = [] 
        match_rows = soup.find_all('div', class_=lambda x: x and 'row' in x and 'match' in x and 'no-gutter' in x)
        
        for row in match_rows:
            try:
                p1_div = row.find('div', class_=lambda x: x and 'player1' in x)
                p2_div = row.find('div', class_=lambda x: x and 'player2' in x)
                table_div = row.find('div', class_=lambda x: x and 'col-2' in x)
                
                if p1_div and p2_div:
                    # Extract enhanced data with points
                    p1_name, p1_record, p1_record_text, p1_points, p1_winner, p1_loser = extract_player_data(p1_div)
                    p2_name, p2_record, p2_record_text, p2_points, p2_winner, p2_loser = extract_player_data(p2_div)
                    
                    # Table number
                    table_num = ''
                    if table_div:
                        table_span = table_div.find('span', class_='tablenumber')
                        table_num = table_span.get_text(strip=True) if table_span else ''
                    
                    pairings.append({
                        'table': table_num,
                        'player1': p1_name,
                        'player1_record': p1_record,
    #                     'player1_record_text': p1_record_text,
                        'player1_points': p1_points,
                        'player1_winner': p1_winner,
    #                     'player1_loser': p1_loser,
                        'player2': p2_name,
                        'player2_record': p2_record,
    #                     'player2_record_text': p2_record_text,
                        'player2_points': p2_points,
                        'player2_winner': p2_winner,
    #                     'player2_loser': p2_loser,
    #                     'p1_div_classes': ' '.join(p1_div.get('class', [])),
    #                     'p2_div_classes': ' '.join(p2_div.get('class', []))
                    })
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        
        return pairings

    base_url = "https://rk9.gg/pairings/" + tournament_url
    
    print("🔍 Getting rounds...")
    rounds = extract_all_round_pairings(base_url)
    
    round_labels = [f"P{r['pod']}R{r['round']}" for r in rounds]
    print(f"Found {len(rounds)} rounds: {round_labels}")
    
    all_data = []
    for round_info in rounds:
    #     print(f"\n--- Processing P{round_info['pod']}R{round_info['round']} ---")
        pairings = extract_pairings_from_round(round_info['url'])
        
        for pairing in pairings:
            pairing['pod'] = round_info['pod']
            pairing['round'] = round_info['round']
            all_data.append(pairing)
        
    # Create DataFrame
    pairings = pd.DataFrame(all_data)
    pairings['event'] = event_name
    pairings.columns = ['Game_table','Player1','Player1_record','Player1_points','Player1_winner',
                        'Player2','Player2_record','Player2_points','Player2_winner','Pod','Round','Event']
    print(f"\n🎉 TOTAL: {len(pairings)} pairings")

    return pairings



def append_df_to_table(conn, 
                        df, 
                        table_name):

    """
    
    Append pandas DataFrame to existing PostgreSQL table using psycopg2
    Assumes DataFrame columns match table columns exactly
    
    """

    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))
    query = "INSERT INTO %s (%s) VALUES %%s" % (table_name, cols)
    
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
        print(f"Appended {len(df)} rows to {table_name}")
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error:", error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()



def db_transaction(engine, 
                    sql_query: str, 
                    *args, **kwargs):

    """
    
    Execute SQL query in a transaction
    Rolls back on error, commits on success
    
    """

    conn = None
    try:
        conn = engine.connect()
        with conn.begin(): 
            executed = conn.execute(text(sql_query), *args, **kwargs)
            try:
                result = executed.fetchall()
            except:
                result = None
            return result
    except Exception as e:
        if conn:
            pass
        raise e
    finally:
        if conn:
            conn.close()