#db.py
import os
import sqlalchemy
import pandas as pd
import requests

api_key='f49c9825267d23c6a491aaa379f5'

nicknames={
'14838': 'Fat Face',
'16243':'BK Broiler',
'1547':'Lefty',
'12294':'Quick Dick',
'15466':'No Sex',
'11276':'Billy Wingtips',
'5768':'Charley Golf',
'18841':'Thik Dik Vik',
'19428':'Billy Clits',
'8825':'Pleats',
'12337':'Cock Rack',
'18238':'Abe the Babe',
'6892':'Three Sticks',
'14609':'Kenny',
'15575':'Big Hog',
'17488': 'Well Hung',
'12965': 'Big Dick Rick',
'10091': 'Backdoor Ror',
'16602':'Three Sticks',
}

#masters in 2021 was event id 526

event_id_dict={
        'THE PLAYERS Championship':11,
        'The Masters':14,
        'The Open Championship':100,
        'U.S. Open':26,
        'PGA Championship':33,
        'Masters Tournament':14,
        }

event_name_dict={
        'THE PLAYERS Championship':'THE PLAYERS Championship',
        'The Masters':'The Masters',
        'The Open Championship':'The Open Championship',
        'U.S. Open':'U.S. Open',
        'PGA Championship':'PGA Championship',
        'Masters Tournament':'The Masters',
        }

draft_slot_dict={'1':[1,20,21,40],
                 '2':[2,19,22,39],
                 '3':[3,18,23,38],
                 '4':[4,17,24,37],
                 '5':[5,16,25,36],
                 '6':[6,15,26,35],
                 '7':[7,14,27,34],
                 '8':[8,13,28,33],
                 '9':[9,12,29,32],
                 '10':[10,11,30,31]}
       



def datagolf_url(endpoint):
    base_url = 'https://feeds.datagolf.com/'
    return(base_url + endpoint)

def fetch_golfer_stats(api_key):
    params={'tour':'pga', 'file_format':'json', 'display':'ranks', 'key':api_key}
    resp=requests.request("GET", datagolf_url('preds/skill-ratings'), params=params)
    envelope = resp.json()
    player_stats=envelope['players']
    player_stats_df=pd.DataFrame(player_stats)
    reduced=player_stats_df[['player_name','sg_ott','sg_app','sg_arg','sg_putt','sg_total','driving_dist','driving_acc']]
    reduced_list = list(reduced.itertuples(index=False, name=None))
    return(reduced_list)
    
def connect_tcp_socket():
    db_host = os.environ.get('CLOUD_SQL_CONNECTION_NAME')
    db_user = os.environ.get('CLOUD_SQL_USERNAME')
    db_pass = os.environ.get('CLOUD_SQL_PASSWORD')
    db_name = os.environ.get('CLOUD_SQL_DATABASE_NAME')
    
    engine_url = sqlalchemy.engine.url.URL.create(drivername="postgresql+pg8000", username=db_user, password=db_pass,database=db_name,query={"unix_sock": "{}/{}/.s.PGSQL.5432".format("/cloudsql",db_host)})
    pool = sqlalchemy.create_engine(engine_url)
    return(pool)

def get_current_draft_board():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''SELECT dr.dg_id, draft_round, draft_year, tournament, igf_golfer, pick_number, overall_selection, player_name from draft_results dr
        left join pga_golfers pg on dr.dg_id = pg.dg_id where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft)''')
        board = result.fetchall()
    draft_results=pd.DataFrame(board, columns=['dg_id', 'draft_round', 'draft_year', 'tournament', 'igf_golfer', 'draft_slot', 'pick_number', 'player_name'])
    conn.close()
    return(draft_results)
    
def get_cum(d_year):
    engine = connect_tcp_socket()
    if d_year == 'All-Time':
        with engine.connect() as conn:
            result = conn.execute('''select igf_golfer,
                              sum(case when tournament='THE PLAYERS Championship' then igf_score else 0 end) players,
                              sum(case when tournament='The Masters' then igf_score else 0 end) masters,
                              sum(case when tournament='PGA Championship' then igf_score else 0 end) pga,
                              sum(case when tournament='U.S. Open' then igf_score else 0 end) us_open,
                              sum(case when tournament='The Open Championship' then igf_score else 0 end) open_championship,
                              sum(igf_score) total from igf_scores group by igf_golfer order by total asc''')
            cum = result.fetchall()
    else:
        with engine.connect() as conn:
            result = conn.execute('''select igf_golfer,
                              sum(case when tournament='THE PLAYERS Championship' then igf_score else 0 end) players,
                              sum(case when tournament='The Masters' then igf_score else 0 end) masters,
                              sum(case when tournament='PGA Championship' then igf_score else 0 end) pga,
                              sum(case when tournament='U.S. Open' then igf_score else 0 end) us_open,
                              sum(case when tournament='The Open Championship' then igf_score else 0 end) open_championship,
                              sum(igf_score) total from igf_scores where tournament_year = ''' + str(d_year) + ' group by igf_golfer order by total asc')
            cum = result.fetchall()
    cum_df = pd.DataFrame(cum, columns=['igf_golfer','players','masters','pga','us_open','open_championship','total'])
    cum_tuple= list(cum_df.itertuples(index=False, name=None))
    return(cum_tuple)
        

def create_draft_slots(tournament, draft_year, draft_dict):
    draft_list=[]
    for k,v in draft_dict.items():
        rnd=1
        for i in draft_slot_dict[v]:
            draft_list.append((rnd,int(v),i, tournament, k, draft_year))
            rnd+=1
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        conn.execute("delete from draft_results where tournament = '" + tournament + "' and draft_year = " + str(draft_year))
        for x in draft_list:
            conn.execute('INSERT INTO draft_results (draft_round, pick_number, overall_selection, tournament, igf_golfer, draft_year) values ' + str(x))
    conn.close()
    return(draft_list)
    
def get_past_draft_board(draft_year, tournament):
    select_str= "SELECT dg_id, igf_golfer, draft_round, overall_selection from draft_results where draft_year = " + draft_year + " and tournament = '" + tournament + "'"
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute(select_str)
        board = result.fetchall()
    draft_results=pd.DataFrame(board, columns=['dg_id', 'igf_golfer', 'draft_round', 'pick_number'])
    conn.close()
    return(draft_results)

def are_results_in(tournament_year, tournament):
    results=False
    select_str = "SELECT * from tournament_results where tournament = '" + tournament + "' and tournament_year = " + str(tournament_year)
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute(select_str)
        results_in = result.fetchall()
    if len(results_in) > 0:
        results=True
    return(results)

def get_min_pick():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''SELECT MIN(overall_selection) from draft_results where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft) and dg_id IS NULL ''')
        pick_number = result.fetchall()
        try:
            overall_selection = pick_number[0][0]
        except:
            overall_selection = 'Draft Complete!'
    conn.close()
    return(overall_selection)
    
def get_pick_igf():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''SELECT igf_golfer from draft_results where overall_selection = (select MIN(overall_selection) from draft_results where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft) and dg_id  IS NULL) and draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft)''')
        golfer = result.fetchall()
        try:
            golfer_on_clock = golfer[0][0]
        except:
            golfer_on_clock = ''
    conn.close()
    return(golfer_on_clock )
    
def submit_pick(pick_number, dg_id):
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        conn.execute('update draft_results set dg_id= ' + str(dg_id) + ' where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft) and overall_selection =  ' + str(pick_number))
    conn.close()

def submit_draft(draft_year, tournament, pick_number, dg_id):
    engine = connect_tcp_socket()
    if dg_id == 'DELETE':
        with engine.connect() as conn:
            conn.execute("update draft_results set dg_id=NULL where draft_year = "+ str(draft_year) + " and tournament = '" + str(tournament) + "' and overall_selection =  " + str(pick_number))
    else:
        with engine.connect() as conn:
            conn.execute("update draft_results set dg_id=" + str(dg_id) + " where draft_year = "+ str(draft_year) + " and tournament = '" + str(tournament) + "' and overall_selection = " + str(pick_number))
    conn.close()

def update_current_draft(draft_year, tournament):
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        conn.execute("update current_draft set draft_year=" + str(draft_year) + ", tournament = '" + str(tournament) + "'")
    conn.close()

def get_tournament_results(tournament_year, tournament):
    select_str = "SELECT dg_id, tournament, tournament_year, position, total_score, igf_score, player_name from tournament_results where tournament = '" + tournament + "' and tournament_year = " + str(tournament_year)
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute(select_str)
        tourney = result.fetchall()
    tournament_results = pd.DataFrame(tourney, columns=['dg_id', 'tournament', 'tournament_year', 'position', 'total_score', 'igf_score', 'player_name'])
    conn.close()
    return(tournament_results)

def get_current_draft_name():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''SELECT draft_year, tournament from current_draft''')
        draft = result.fetchall()
    draft_name=pd.DataFrame(draft, columns=[ 'draft_year', 'tournament'])
    conn.close()
    return(draft_name)
    
def get_champions():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''Select * from igf_champions''')
        champions = result.fetchall()
    champions_table=pd.DataFrame(champions, columns=[ 'tournament_year', 'tpc_winner', 'tpc_runner_up', 'masters_winner', 'masters_runner_up', 'pga_winner', 'pga_runner_up', 'us_winner', 'us_runner_up', 'british_winner', 'british_runner_up'])
    champs = list(champions_table.itertuples(index=False, name=None))
    conn.close()
    return(champs)
    
def get_igf_results(view_by):
    data_table = ()
    engine = connect_tcp_socket()
    if view_by == 'money':
        with engine.connect() as conn:
            result = conn.execute('''select * from igf_payouts''')
            results = result.fetchall()
            data_table=pd.DataFrame(results, columns=['igf_golfer', 'tpc', 'masters', 'pga', 'us', 'british', 'cum'])
            data_table.fillna(0, inplace=True)
            data_table['total'] = data_table.drop('igf_golfer', axis=1).sum(axis=1)
            data_table['tpc'] = data_table['tpc'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['masters'] = data_table['masters'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['pga'] = data_table['pga'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['us'] = data_table['us'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['british'] = data_table['british'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['cum'] = data_table['cum'].astype(int).apply(lambda x: "${:,}".format(x))
            data_table['total'] = data_table['total'].astype(int).apply(lambda x: "${:,}".format(x))
            
    elif view_by == 'first':
        with engine.connect() as conn:
            result = conn.execute('''select * from igf_winners''')
            results = result.fetchall()
            data_table=pd.DataFrame(results, columns=['igf_golfer', 'tpc', 'masters', 'pga', 'us', 'british','cum'])
            data_table.fillna(0, inplace=True)
            data_table['total'] = data_table.drop('igf_golfer', axis=1).sum(axis=1)
            data_table.astype({'tpc': int, 'masters': int, 'pga': int, 'us': int, 'british': int,'cum': int,'total': int})
    elif view_by == 'second':
        with engine.connect() as conn:
            result = conn.execute('''select * from igf_runner_ups''')
            results = result.fetchall()
            data_table=pd.DataFrame(results, columns=[ 'igf_golfer', 'tpc', 'masters', 'pga', 'us', 'british','cum'])
            data_table.fillna(0, inplace=True)
            data_table['total'] = data_table.drop('igf_golfer', axis=1).sum(axis=1)
            data_table.astype({'tpc': int, 'masters': int, 'pga': int, 'us': int, 'british': int,'cum': int,'total': int})
    data = list(data_table.itertuples(index=False, name=None))
    conn.close()
    return(data)
    
def get_player_results():
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''select player_name, count(t.position) as rounds, avg(t.igf_score)*1 as avg_igf, avg(igf_rank)*1 as avg_igf_rank, sum(case when i.igf_rank = 1 then 1 else 0 end) igf_wins, (sum(case when i.igf_rank = 1 then 1 else 0 end) / count(t.position)::float)*100 igf_wins_pct, sum(case when (t.position = 'CUT' or t.position = 'WD' or t.position = 'DQ') then 1 else 0 end) cut_count, (sum(case when (t.position = 'CUT' or t.position = 'WD' or t.position = 'DQ') then 1 else 0 end) / count(t.position)::float)*100 as cut_pct from draft_results d  left join tournament_results t on d.draft_year = t.tournament_year and d.tournament = t.tournament and d.dg_id = t.dg_id left join igf_leaderboards i on i.draft_year = d.draft_year and i.tournament=d.tournament and i.igf_golfer = d.igf_golfer where t.igf_score is not null group by t.player_name having count(t.position)>4 order by avg_igf asc''')
        player_results = result.fetchall()
    results_table=pd.DataFrame(player_results, columns=['Golfer', 'Rounds Drafted', 'Avg IGF Score', 'Avg IGF Place', 'IGF Wins', 'IGF Win %', 'IGF Cuts', 'IGF Cut %'])
    rounded=results_table.round({'IGF Win %':1, 'IGF Cut %':1})
    rounded['Avg IGF Score'] = rounded['Avg IGF Score'].astype(float).round(2)
    rounded['Avg IGF Place'] = rounded['Avg IGF Place'].astype(float).round(2)
    results = list(rounded.itertuples(index=False, name=None))
    conn.close()
    return(results)

def get_latest_field(api_key):
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''select dg_id, overall_selection from draft_results where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft) and dg_id IS NOT NULL''')
        golfers = result.fetchall()
    drafted=pd.DataFrame(golfers, columns=['dg_id', 'overall'])
    conn.close()
    params={'tour':'pga', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('field-updates'), params=params)
    envelope = resp.json()
    field=envelope['field']
    field_list=[]
    for i in field:
        if i['dg_id'] not in drafted['dg_id'].values:
            field_list.append((i['dg_id'], i['player_name']))
    field_df=pd.DataFrame(field_list, columns=['dg_id','player_name'])
    return(field_df)

def get_all_pga_for_draft(api_key):
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute('''select dg_id, overall_selection from draft_results where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft) and dg_id IS NOT NULL''')
        golfers = result.fetchall()
    drafted=pd.DataFrame(golfers, columns=['dg_id', 'overall'])
    conn.close()
    params={'tour':'pga', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('get-player-list'), params=params)
    envelope = resp.json()
    pga_list=[]
    for i in envelope:
        if i['dg_id'] not in drafted['dg_id'].values:
            pga_list.append((i['dg_id'], i['player_name']))
    pga_df=pd.DataFrame(pga_list, columns=['dg_id','player_name'])
    return(pga_df)

def submit_research(dg_id):
    return('TBD')

def get_all_pga(api_key):
    params={'tour':'pga', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('get-player-list'), params=params)
    envelope = resp.json()
    return(envelope)
    
    
def get_player_from_dg_id(api_key, dg_id):
    params={'tour':'pga', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('get-player-list'), params=params)
    envelope = resp.json()
    player=next((item for item in envelope if item["dg_id"] == 0), None)
    player_name='Bad ID'
    if player != None:
        player_name=player['player_name']
    return(player_name)
    
def add_nicknames(df, nicknames):
    for i, r in df.iterrows():
        if str(r['dg_id']) in nicknames.keys():
            df.at[i,'player_name_x'] = r['player_name_x']+ ' (' + nicknames[str(r['dg_id'])]+ ')'
    return(True)

def add_nicknames_draft(df, nicknames):
    for i, r in df.iterrows():
        if str(r['dg_id']) in nicknames.keys():
            df.at[i,'player_name'] = r['player_name']+ ' (' + nicknames[str(r['dg_id'])]+ ')'
    return(True)

def get_latest_preds(api_key):
    params={'display':'value', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('preds/in-play'), params=params)
    envelope = resp.json()
    event_name = envelope['info']['event_name']
    live_stats = envelope['data']
    leaderboard = pd.DataFrame(live_stats)
    return(event_name, leaderboard)
    
def get_historical_results(api_key, event_id, year):
    params={'display':'value', 'file_format':'json', 'key':api_key, 'event_id': event_id, 'year': year, 'tour':'pga'}
    resp=requests.request("GET", datagolf_url('historical-raw-data/rounds'), params=params)
    envelope = resp.json()
    event_name = envelope['event_name']
    past_stats = envelope['scores']
    past_ldb=[]
    past_ldb_stats=[]
    for i in past_stats:
        score=0
        round_cnt=0
        dist=0
        acc=0
        gir=0
        scrambling=0
        avg_dist=0
        avg_acc=0
        avg_gir=0
        avg_scrambling=0
        for k,v in i.items():
            if 'round' in k:
                score += v['score']-v['course_par']
                round_cnt += 1
                if 'driving_dist' in v and v['driving_dist'] != None:
                    dist += v['driving_dist']
                if 'driving_acc' in v and v['driving_dist'] != None:
                    acc += v['driving_acc']
                if 'gir' in v and v['driving_dist'] != None:
                    gir += v['gir']
                if 'scrambling' in v and v['driving_dist'] != None:
                    scrambling += v['scrambling']
        if round_cnt > 0:
            avg_dist = dist / round_cnt
            avg_acc = acc / round_cnt
            avg_gir = gir / round_cnt
            avg_scrambling = scrambling / round_cnt
        past_ldb.append({'dg_id':i['dg_id'], 'current_pos':i['fin_text'], 'player_name':i['player_name'], 'current_score':score})
        past_ldb_stats.append({'dg_id':i['dg_id'], 'current_pos':i['fin_text'], 'player_name':i['player_name'], 'current_score':score, 'd_dist':str(round(avg_dist)), 'd_acc':str(round(avg_acc*100,1))+'%', 'gir': str(round(avg_gir*100,1))+'%','scrambling': str(round(avg_scrambling*100,1))+'%'})
    past_leaderboard=pd.DataFrame(past_ldb)
    past_leaderboard_and_stats=pd.DataFrame(past_ldb_stats)
    return(event_name, past_leaderboard, past_leaderboard_and_stats)

def get_current_tournament_stats(api_key):
    params={'display':'value',
            'file_format':'json',
            'key':api_key,
            'round': 'event_avg',
            'tour':'pga',
            'stats':'accuracy,distance,gir,great_shots, poor_shots,scrambling'}
    resp=requests.request("GET", datagolf_url('preds/live-tournament-stats'), params=params)
    envelope = resp.json()
    event_name = envelope['event_name']
    live_stats = envelope['live_stats']
    live_ldb=[]
    dist=0
    acc=0
    gir=0
    gs=0
    ps=0
    scrambling=0
    for i in live_stats:
        if 'distance' in i.keys():
            if i['distance'] == None:
                dist=0
            else:
                dist = i['distance']
        if 'accuracy' in i.keys():
            if i['accuracy'] ==  None:
                acc=0
            else:
                acc = i['accuracy']
        if 'gir' in i.keys():
            if i['gir'] == None:
                gir=0
            else:
                gir = i['gir']
        if 'great_shots' in i.keys():
            if i['great_shots'] == None:
                gs = 0
            else:
                gs = i['great_shots']
        if 'poor_shots' in i.keys():
            if i['poor_shots'] == None:
                ps = 0
            else:
                ps = i['poor_shots']
        if 'scrambling' in i.keys():
            if i['scrambling'] == None:
                scrambling = 0
            else:
                scrambling = i['scrambling']
        live_ldb.append({'dg_id':i['dg_id'],
                         'd_dist':str(round(dist)),
                         'd_acc':str(round(acc*100,1))+'%',
                         'gir': str(round(gir*100,1))+'%',
                         'gs': str(round(gs)),
                         'ps': str(round(ps)),
                         'scrambling': str(round(scrambling*100,1))+'%'})
    live_leaderboard=pd.DataFrame(live_ldb)
    return(event_name, live_leaderboard)

def get_latest_event_and_date(api_key):
    params={'display':'value', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('preds/in-play'), params=params)
    envelope = resp.json()
    if envelope['info']['event_name'] in event_name_dict.keys():
        event_name = event_name_dict[envelope['info']['event_name']]
    else:
        event_name = envelope['info']['event_name']
    event_year = int(envelope['info']['last_update'][:4])
    return(event_name, event_year)

def get_latest_field_info(api_key):
    params={'display':'value', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('field-updates'), params=params)
    envelope = resp.json()
    event_name = envelope['event_name']
    return(event_name)

def get_igf_score(leaderboard, dg_id):
    #first fetch the players score (potentially only through 36 or WD)
    worst_made_cut = max(leaderboard[(leaderboard['current_pos'] != 'CUT') & (leaderboard['current_pos'] != 'WD')]['current_score'])
    worst_incl_cut = max(leaderboard['current_score'])
    if dg_id in list(leaderboard['dg_id']):
        current_score = leaderboard[leaderboard['dg_id']==dg_id]['current_score'].iloc[0]
        if leaderboard[(leaderboard['dg_id'] == dg_id)]['current_pos'].iloc[0] == 'CUT':
            igf_score=max(current_score,worst_made_cut+2)
        elif leaderboard[(leaderboard['dg_id'] == dg_id)]['current_pos'].iloc[0] == 'WD':
            igf_score=max(current_score,worst_made_cut+4, worst_incl_cut+2)
        else:
            igf_score=current_score
    else:
        # this assumes the player WD before the tournament started
        igf_score=max(worst_made_cut+4, worst_incl_cut+2)
    return(igf_score)

def is_field_live(api_key):
    is_live=False
    draft_name = get_current_draft_name()
    event_name = get_latest_field_info(api_key)
    if event_name in event_name_dict.keys():
        if draft_name['tournament'][0] == event_name_dict[event_name]:
            is_live=True
    return(is_live)

def is_leaderboard_live(api_key):
    is_live=False
    draft_name = get_current_draft_name()
    event_and_year = get_latest_event_and_date(api_key)
    if event_and_year[0] in event_name_dict.keys():
        if draft_name['tournament'][0] == event_name_dict[event_and_year[0]] and draft_name['draft_year'][0] == event_and_year[1]:
            is_live=True
    return(is_live)


def add_golfers_to_leaderboard(igf_leaderboard, golfer_leaderboard):
    igf_leaderboard['1st Golfer']=pd.Series()
    igf_leaderboard['2nd Golfer']=pd.Series()
    igf_leaderboard['3rd Golfer']=pd.Series()
    igf_leaderboard['4th Golfer']=pd.Series()
    loop_dict={'1':'1st Golfer', '2':'2nd Golfer', '3':'3rd Golfer','4':'4th Golfer'}
    for k in igf_leaderboard.iterrows():
        top_4=golfer_leaderboard[golfer_leaderboard['igf_golfer']==k[0]].sort_values(by='igf_score', ascending=True)
        loop = 1
        for v in top_4['igf_score'].items():
            igf_leaderboard.loc[k[0],loop_dict[str(loop)]] = str(v[0]) +' (' + str(v[1]) +')'
            loop += 1
    igf_leaderboard['sort1'] = igf_leaderboard['1st Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort2'] = igf_leaderboard['2nd Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort3'] = igf_leaderboard['3rd Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort4'] = igf_leaderboard['4th Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard=igf_leaderboard.sort_values(by=['igf_score','sort1','sort2','sort3','sort4'], ascending=True)
    igf_leaderboard=igf_leaderboard.drop(['sort1','sort2','sort3','sort4'], axis=1)
    return(igf_leaderboard)

def add_dgid_to_leaderboard(igf_leaderboard, golfer_leaderboard):
    igf_leaderboard['1st Golfer']=pd.Series()
    igf_leaderboard['2nd Golfer']=pd.Series()
    igf_leaderboard['3rd Golfer']=pd.Series()
    igf_leaderboard['4th Golfer']=pd.Series()
    igf_leaderboard['sort1']=pd.Series()
    igf_leaderboard['sort2']=pd.Series()
    igf_leaderboard['sort3']=pd.Series()
    igf_leaderboard['sort4']=pd.Series()
    loop_dict={'1':'1st Golfer', '2':'2nd Golfer', '3':'3rd Golfer','4':'4th Golfer'}
    for k in igf_leaderboard.iterrows():
        top_4=golfer_leaderboard[golfer_leaderboard['igf_golfer']==k[0]].sort_values(by='igf_score', ascending=True)
        loop = 1
        for v in top_4['igf_score'].items():
            igf_leaderboard.loc[k[0],loop_dict[str(loop)]] = str(v[0])
            igf_leaderboard.loc[k[0],'sort'+str(loop)] = int(v[1])
            loop += 1
    igf_leaderboard=igf_leaderboard.sort_values(by=['igf_score','sort1','sort2','sort3','sort4'], ascending=True)
    igf_leaderboard=igf_leaderboard.drop(['sort1','sort2','sort3','sort4'], axis=1)
    return(igf_leaderboard)
    
    
def test_add_golfers_to_leaderboard(igf_leaderboard, golfer_leaderboard):
    igf_leaderboard['1st Golfer']=pd.Series()
    igf_leaderboard['2nd Golfer']=pd.Series()
    igf_leaderboard['3rd Golfer']=pd.Series()
    igf_leaderboard['4th Golfer']=pd.Series()
    loop_dict={'1':'1st Golfer', '2':'2nd Golfer', '3':'3rd Golfer','4':'4th Golfer'}
    for k in igf_leaderboard.iterrows():
        top_4=golfer_leaderboard[golfer_leaderboard['igf_golfer']==k[0]].sort_values(by='igf_score', ascending=True)
        loop = 1
        for v in top_4['igf_score'].items():
            igf_leaderboard.loc[k[0],loop_dict[str(loop)]] = str(v[0]) +' (' + str(v[1]) +')'
            loop += 1
    igf_leaderboard['sort1'] = igf_leaderboard['1st Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort2'] = igf_leaderboard['2nd Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort3'] = igf_leaderboard['3rd Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard['sort4'] = igf_leaderboard['4th Golfer'].str.extract('([-+]?\\d+)', expand=False).astype(int)
    igf_leaderboard=igf_leaderboard.sort_values(by=['igf_score','sort1','sort2','sort3','sort4'], ascending=True)
    #igf_leaderboard=igf_leaderboard.drop(['sort1','sort2','sort3','sort4'], axis=1)
    return(igf_leaderboard)

def test_add_dgid_to_leaderboard(igf_leaderboard, golfer_leaderboard):
    igf_leaderboard['1st Golfer']=pd.Series()
    igf_leaderboard['2nd Golfer']=pd.Series()
    igf_leaderboard['3rd Golfer']=pd.Series()
    igf_leaderboard['4th Golfer']=pd.Series()
    igf_leaderboard['sort1']=pd.Series()
    igf_leaderboard['sort2']=pd.Series()
    igf_leaderboard['sort3']=pd.Series()
    igf_leaderboard['sort4']=pd.Series()
    loop_dict={'1':'1st Golfer', '2':'2nd Golfer', '3':'3rd Golfer','4':'4th Golfer'}
    for k in igf_leaderboard.iterrows():
        top_4=golfer_leaderboard[golfer_leaderboard['igf_golfer']==k[0]].sort_values(by='igf_score', ascending=True)
        loop = 1
        for v in top_4['igf_score'].items():
            igf_leaderboard.loc[k[0],loop_dict[str(loop)]] = str(v[0])
            igf_leaderboard.loc[k[0],'sort'+str(loop)] = int(v[1])
            loop += 1
    igf_leaderboard=igf_leaderboard.sort_values(by=['igf_score','sort1','sort2','sort3','sort4'], ascending=True)
    #igf_leaderboard=igf_leaderboard.drop(['sort1','sort2','sort3','sort4'], axis=1)
    return(igf_leaderboard)

def post_data_to_results(leaderboard, draft_year, tournament):
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        conn.execute("delete from tournament_results where tournament = '" + tournament + "' and tournament_year = " + str(draft_year))
        for i in leaderboard:
            conn.execute('insert into tournament_results (dg_id, position, player_name, total_score, igf_score,  tournament_year, tournament) values ' + str(i))
    conn.close()
    return("Done")
    
def igf_a_leaderboard(datagolf_key, draft_year, tournament):
    draft_results = get_past_draft_board(draft_year, tournament)
    name, leaderboard, leaderboard_and_stats = get_historical_results(datagolf_key, event_id_dict[tournament], draft_year)
    combined = draft_results.merge(leaderboard, how='left', on='dg_id')
    combined['igf_score'] = combined.apply(lambda x: get_igf_score(leaderboard, x.dg_id), 1)
    igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
    reduced = combined[['igf_golfer', 'pick_number', 'current_pos','current_score', 'igf_score',]].set_index(combined.player_name_x).sort_values(by=['igf_golfer','pick_number'])
    reduced.fillna(value='WD', inplace = True)
    final_igf=add_golfers_to_leaderboard(igf_leaderboard, reduced)
    igf_tuple = list(final_igf.itertuples(index=True, name=None))
    leaderboard_type='results'
    reduced_tuple = list(reduced.itertuples(index=True, name=None))
    return(igf_tuple, reduced_tuple, leaderboard_type)
    
def post_tournament_results(datagolf_key, draft_year, tournament):
    name, leaderboard, ldb_and_stats = get_historical_results(datagolf_key, event_id_dict[tournament], draft_year)
    leaderboard['igf_score'] = leaderboard.apply(lambda x: get_igf_score(leaderboard, x.dg_id), 1)
    leaderboard['tournament_year']=draft_year
    leaderboard['tournament']=tournament
    leaderboard.fillna(value='WD', inplace = True)
    igf_tuple = list(leaderboard.itertuples(index=False, name=None))
    leaderboard_type='results'
    post_data_to_results(igf_tuple, draft_year, tournament)
    return(igf_tuple, leaderboard_type)

def update_current_leaderboard(datagolf_key):
    draft_results = get_current_draft_board()
    if draft_results.empty:
        reduced_tuple = ()
        igf_tuple = ()
        igf_tuple_dg = ()
        leaderboard_type='draft'
    else:
        if is_leaderboard_live(datagolf_key):
            name, leaderboard = get_latest_preds(datagolf_key)
            stats_name, stats = get_current_tournament_stats(datagolf_key)
            leaderboard_with_stats = leaderboard.merge(stats, how='left', on='dg_id')
            combined = draft_results.merge(leaderboard_with_stats, how='left', on='dg_id')
            combined['igf_score'] = combined.apply(lambda x: get_igf_score(leaderboard, x.dg_id), 1)
            add_nicknames(combined, nicknames)
            igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
            reduced = combined[['igf_golfer', 'pick_number', 'current_pos', 'thru', 'today', 'make_cut','d_dist','d_acc','gir','gs', 'ps', 'scrambling','current_score', 'igf_score','dg_id']].set_index(combined.player_name_x).sort_values(by=['igf_golfer','pick_number'])
            #reduced = combined[['igf_golfer', 'pick_number', 'current_pos', 'thru', 'today', 'make_cut','current_score', 'igf_score','dg_id']].set_index(combined.player_name_x).sort_values(by=['pick_number'])
            reduced.fillna(value='WD', inplace = True)
            reduced['make_cut'].replace(to_replace='WD',value=0, inplace=True)
            reduced.loc[:, 'make_cut'] = reduced.make_cut.apply(lambda x: str(round(x*100,1))+'%')
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            final_igf=add_golfers_to_leaderboard(igf_leaderboard, reduced)
            igf_tuple = list(final_igf.itertuples(index=True, name=None))
            reduced_dg = combined[['igf_golfer', 'pick_number', 'current_pos','current_score', 'igf_score']].set_index(combined.dg_id)
            reduced_dg.fillna(value='WD', inplace = True)
            final_igf_dg=add_dgid_to_leaderboard(igf_leaderboard, reduced_dg)
            igf_tuple_dg = list(final_igf_dg.itertuples(index=True, name=None))
            leaderboard_type='live'
        elif are_results_in(draft_results['draft_year'].iloc[0], draft_results['tournament'].iloc[0]):
            tournament_results = get_tournament_results(draft_results['draft_year'].iloc[0], draft_results['tournament'].iloc[0])
            combined = draft_results.merge(tournament_results, how='left', on='dg_id')
            add_nicknames(combined, nicknames)
            igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
            reduced = combined[['igf_golfer', 'pick_number', 'position','total_score', 'igf_score']].set_index(combined.player_name_x).sort_values(by=['pick_number'])
            reduced.fillna(value='WD', inplace = True)
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            final_igf=add_golfers_to_leaderboard(igf_leaderboard, reduced)
            igf_tuple = list(final_igf.itertuples(index=True, name=None))
            reduced_dg = combined[['igf_golfer', 'pick_number', 'position','total_score', 'igf_score']].set_index(combined.dg_id)
            reduced_dg.fillna(value='WD', inplace = True)
            final_igf_dg=add_dgid_to_leaderboard(igf_leaderboard, reduced_dg)
            igf_tuple_dg = list(final_igf_dg.itertuples(index=True, name=None))
            leaderboard_type='results'
        else:
            reduced = draft_results[['igf_golfer', 'pick_number', 'dg_id']].set_index(draft_results.player_name).sort_values(by=['pick_number'])
            try:
                reduced['dg_id'] = reduced['dg_id'].astype('int')
            except:
                reduced['dg_id'] = reduced['dg_id']
            #add_nicknames_draft(reduced, nicknames)
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            igf_tuple = ()
            igf_tuple_dg = ()
            leaderboard_type='draft'
    return(igf_tuple, reduced_tuple, leaderboard_type, igf_tuple_dg)
    
def test_update_current_leaderboard(datagolf_key):
    draft_results = get_current_draft_board()
    if draft_results.empty:
        reduced_tuple = ()
        igf_tuple = ()
        igf_tuple_dg = ()
        leaderboard_type='draft'
    else:
        if is_leaderboard_live(datagolf_key):
            name, leaderboard = get_latest_preds(datagolf_key)
            stats_name, stats = get_current_tournament_stats(datagolf_key)
            leaderboard_with_stats = leaderboard.merge(stats, how='left', on='dg_id')
            combined = draft_results.merge(leaderboard_with_stats, how='left', on='dg_id')
            combined['igf_score'] = combined.apply(lambda x: get_igf_score(leaderboard, x.dg_id), 1)
            add_nicknames(combined, nicknames)
            igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
            reduced = combined[['igf_golfer', 'pick_number', 'current_pos', 'thru', 'today', 'make_cut','d_dist','d_acc','gir','gs', 'ps', 'scrambling','current_score', 'igf_score','dg_id']].set_index(combined.player_name_x).sort_values(by=['igf_golfer','pick_number'])
            #reduced = combined[['igf_golfer', 'pick_number', 'current_pos', 'thru', 'today', 'make_cut','current_score', 'igf_score','dg_id']].set_index(combined.player_name_x).sort_values(by=['pick_number'])
            reduced.fillna(value='WD', inplace = True)
            reduced['make_cut'].replace(to_replace='WD',value=0, inplace=True)
            reduced.loc[:, 'make_cut'] = reduced.make_cut.apply(lambda x: str(round(x*100,1))+'%')
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            final_igf=test_add_golfers_to_leaderboard(igf_leaderboard, reduced)
            igf_tuple = list(final_igf.itertuples(index=True, name=None))
            reduced_dg = combined[['igf_golfer', 'pick_number', 'current_pos','current_score', 'igf_score']].set_index(combined.dg_id)
            reduced_dg.fillna(value='WD', inplace = True)
            final_igf_dg=test_add_dgid_to_leaderboard(igf_leaderboard, reduced_dg)
            igf_tuple_dg = list(final_igf_dg.itertuples(index=True, name=None))
            reduced_tuple_dg = list(reduced_dg.itertuples(index=True, name=None))
            leaderboard_type='live'
        elif are_results_in(draft_results['draft_year'].iloc[0], draft_results['tournament'].iloc[0]):
            tournament_results = get_tournament_results(draft_results['draft_year'].iloc[0], draft_results['tournament'].iloc[0])
            combined = draft_results.merge(tournament_results, how='left', on='dg_id')
            add_nicknames(combined, nicknames)
            igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
            reduced = combined[['igf_golfer', 'pick_number', 'position','total_score', 'igf_score']].set_index(combined.player_name_x).sort_values(by=['pick_number'])
            reduced.fillna(value='WD', inplace = True)
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            final_igf=add_golfers_to_leaderboard(igf_leaderboard, reduced)
            igf_tuple = list(final_igf.itertuples(index=True, name=None))
            reduced_dg = combined[['igf_golfer', 'pick_number', 'position','total_score', 'igf_score']].set_index(combined.dg_id)
            reduced_dg.fillna(value='WD', inplace = True)
            final_igf_dg=add_dgid_to_leaderboard(igf_leaderboard, reduced_dg)
            igf_tuple_dg = list(final_igf_dg.itertuples(index=True, name=None))
            leaderboard_type='results'
        else:
            reduced = draft_results[['igf_golfer', 'pick_number', 'dg_id']].set_index(draft_results.player_name).sort_values(by=['pick_number'])
            try:
                reduced['dg_id'] = reduced['dg_id'].astype('int')
            except:
                reduced['dg_id'] = reduced['dg_id']
            #add_nicknames_draft(reduced, nicknames)
            reduced_tuple = list(reduced.itertuples(index=True, name=None))
            igf_tuple = ()
            igf_tuple_dg = ()
            leaderboard_type='draft'
    return(igf_tuple, reduced_tuple, leaderboard_type, igf_tuple_dg, reduced_tuple_dg)

def update_past_leaderboard(datagolf_key, draft_year, tournament):
    draft_results = get_past_draft_board(draft_year, tournament)
    if are_results_in(draft_year, tournament):
        tournament_results = get_tournament_results(draft_year, tournament)
        try:
            stats_name, stats, stats_and_ldb = get_historical_results(datagolf_key, event_id_dict[tournament], draft_year)
            results_with_stats = tournament_results.merge(stats_and_ldb, how='left', on='dg_id')
            combined = draft_results.merge(results_with_stats, how='left', on='dg_id')
            add_nicknames(combined, nicknames)
        except:
            combined = draft_results.merge(tournament_results, how='left', on='dg_id')
            add_nicknames_draft(combined, nicknames)
        igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
        if 'd_dist' in combined.columns and 'd_acc' in combined.columns and 'gir' in combined.columns and 'scrambling' in combined.columns:
            reduced = combined[['igf_golfer', 'pick_number', 'position', 'd_dist','d_acc','gir','scrambling','total_score', 'igf_score', 'dg_id']].set_index(combined.player_name_x).sort_values(by=['pick_number'])
            final_leaderboard_heads = ('Golfer', 'IGFer', 'Pick', 'Pos.','D. Dist.', 'D. Accu', 'GIR', 'Scrambling','Total','IGF Score')
        else:
            reduced = combined[['igf_golfer', 'pick_number', 'position', 'total_score', 'igf_score', 'dg_id']].set_index(combined.player_name).sort_values(by=['pick_number'])
            final_leaderboard_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number', 'Position', 'Total','IGF Score')
        #reduced.fillna(value='WD', inplace = True)
        reduced_tuple = list(reduced.itertuples(index=True, name=None))
        final_igf=add_golfers_to_leaderboard(igf_leaderboard, reduced)
        igf_tuple = list(final_igf.itertuples(index=True, name=None))
        reduced_dg = combined[['igf_golfer', 'pick_number', 'position','total_score', 'igf_score']].set_index(combined.dg_id)
        reduced_dg.fillna(value='WD', inplace = True)
        final_igf_dg=add_dgid_to_leaderboard(igf_leaderboard, reduced_dg)
        igf_tuple_dg = list(final_igf_dg.itertuples(index=True, name=None))
        leaderboard_type='results'
    else:
        reduced = draft_results[['igf_golfer', 'pick_number', 'dg_id']].set_index(draft_results.player_name).sort_values(by=['pick_number'])
        reduced['dg_id'] = reduced['dg_id'].astype('str').str.rstrip('.0')
        #add_nicknames_draft(reduced, nicknames)
        igf_tuple = ()
        leaderboard_type='draft'
        reduced_tuple = list(reduced.itertuples(index=True, name=None))
        final_leaderboard_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number', 'Position', 'Total','IGF Score')
    return(igf_tuple, reduced_tuple, leaderboard_type, igf_tuple_dg, final_leaderboard_heads)
    
def get_top_picks(draft_year, tournament):
    engine = connect_tcp_socket()
    if draft_year == 'All Time' and tournament == 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute('''select d.igf_golfer, p.player_name, count(d.dg_id) from draft_results d left join pga_golfers p on d.dg_id = p.dg_id group by d.igf_golfer, p.player_name having count(d.dg_id) > 1 order by count(d.dg_id) desc''')
            draft = result.fetchall()
        hard_on=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    elif draft_year == 'All Time' and tournament != 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, count(d.dg_id) from draft_results d left join pga_golfers p on d.dg_id = p.dg_id where tournament = '" + tournament + "' group by d.igf_golfer, p.player_name having count(d.dg_id) > 1 order by count(d.dg_id) desc")
            draft = result.fetchall()
        hard_on=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    elif tournament == 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, count(d.dg_id) from draft_results d left join pga_golfers p on d.dg_id = p.dg_id where draft_year = " + draft_year + " group by d.igf_golfer, p.player_name having count(d.dg_id) > 1 order by count(d.dg_id) desc")
            draft = result.fetchall()
        hard_on=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    else:
        with engine.connect() as conn:
            result = conn.execute('select d.igf_golfer, p.player_name, count(d.dg_id) from draft_results d left join pga_golfers p on d.dg_id = p.dg_id where draft_year = ' +draft_year+ " and tournament = '"+ tournament + "' group by d.igf_golfer, p.player_name having count(d.dg_id) > 1 order by count(d.dg_id) desc")
            draft = result.fetchall()
        hard_on=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    conn.close()
    hard_on_tuple = list(hard_on.itertuples(index=False, name=None))
    return(hard_on_tuple)

def get_best_performer(draft_year, tournament):
    engine = connect_tcp_socket()
    if draft_year == 'All Time' and tournament == 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, sum(p.total_score)/count(p.total_score) as avg_score, sum(p.total_score) as total_score, count(p.total_score) as num_tourneys from draft_results d left join tournament_results p on (d.dg_id = p.dg_id and d.draft_year = p.tournament_year and d.tournament = p.tournament) group by d.igf_golfer, p.player_name having count(p.total_score) > 1 order by sum(p.total_score)/count(p.total_score) asc")
            draft = result.fetchall()
        best_perf=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    elif draft_year == 'All Time' and tournament != 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, sum(p.total_score)/count(p.total_score) as avg_score, sum(p.total_score) as total_score, count(p.total_score) as num_tourneys from draft_results d left join tournament_results p on (d.dg_id = p.dg_id and d.draft_year = p.tournament_year and d.tournament = p.tournament) where tournament = '" + tournament + "'group by d.igf_golfer, p.player_name having count(p.total_score) > 1 order by sum(p.total_score)/count(p.total_score) asc")
            draft = result.fetchall()
        best_perf=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    elif tournament == 'All Tournaments':
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, sum(p.total_score)/count(p.total_score) as avg_score, sum(p.total_score) as total_score, count(p.total_score) as num_tourneys from draft_results d left join tournament_results p on (d.dg_id = p.dg_id and d.draft_year = p.tournament_year and d.tournament = p.tournament) where draft_year = " + draft_year + "group by d.igf_golfer, p.player_name having count(p.total_score) > 1 order by sum(p.total_score)/count(p.total_score) asc")
            draft = result.fetchall()
        best_perf=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'times_drafted'])
    else:
        with engine.connect() as conn:
            result = conn.execute("select d.igf_golfer, p.player_name, sum(p.total_score)/count(p.total_score) as avg_score, sum(p.total_score) as total_score, count(p.total_score) as num_tourneys from draft_results d left join tournament_results p on (d.dg_id = p.dg_id and d.draft_year = p.tournament_year and d.tournament = p.tournament) where draft_year = " +draft_year+ " and tournament = '"+ tournament + "' group by d.igf_golfer, p.player_name having count(p.total_score) > 1 order by sum(p.total_score)/count(p.total_score) asc")
            draft = result.fetchall()
        best_perf=pd.DataFrame(draft, columns=[ 'igf_golfer', 'player_name', 'avg_score', 'total_score', 'num_tourneys'])
    conn.close()
    best_perf_tuple = list(best_perf.itertuples(index=False, name=None))
    return(best_perf_tuple)
    
    
def test_fun(datagolf_key, draft_year, tournament):
    draft_results, draft_tuple = get_past_draft_board(draft_year, tournament)
    tournament_results = get_tournament_results(draft_year, tournament)
    #stats_name, stats, stats_and_ldb = get_historical_results(datagolf_key, event_id_dict[tournament], draft_year)
    #results_with_stats = tournament_results.merge(stats_and_ldb, how='left', on=['dg_id','player_name'])
    #combined = draft_results.merge(results_with_stats, how='left', on=['dg_id', 'player_name'])
    combined = draft_results.merge(tournament_results, how='left', on=['dg_id', 'player_name'])
    add_nicknames(combined, nicknames)
    igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().sort_values().to_frame()
    if 'd_dist' in combined.columns and 'd_acc' in combined.columns and 'gir' in combined.columns and 'scrambling' in combined.columns:
        reduced = combined[['igf_golfer', 'pick_number', 'position', 'd_dist','d_acc','gir','scrambling','total_score', 'igf_score', 'dg_id']].set_index(combined.player_name).sort_values(by=['pick_number'])
        final_leaderboard_heads = ('Golfer', 'IGFer', 'Pick', 'Pos.','D. Dist.', 'D. Accu', 'GIR', 'Scrambling','Total','IGF Score')
    else:
        reduced = combined[['igf_golfer', 'pick_number', 'position', 'total_score', 'igf_score', 'dg_id']].set_index(combined.player_name).sort_values(by=['pick_number'])
        final_leaderboard_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number', 'Position', 'Total','IGF Score')
    #reduced.fillna(value='WD', inplace = True)
    igf_tuple = list(igf_leaderboard.itertuples(index=True, name=None))
    reduced_tuple = list(reduced.itertuples(index=True, name=None))
    return(igf_tuple, reduced_tuple, final_leaderboard_heads)


# ============================================
# IGF PROFILE PAGE FUNCTIONS
# ============================================

def get_igf_profile_data(igf_name):
    """
    Fetch all data needed for an IGFer's profile page.
    Returns a dictionary with all profile sections, or None if IGFer not found.
    """
    engine = connect_tcp_socket()
    
    # First, verify this IGFer exists
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT DISTINCT igf_golfer FROM draft_results WHERE igf_golfer = '" + igf_name.replace("'", "''") + "'"
        )
        if not result.fetchone():
            return None
    
    profile = {}
    
    # Get first year in IGF
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT MIN(draft_year) FROM draft_results WHERE igf_golfer = '" + igf_name.replace("'", "''") + "'"
        )
        row = result.fetchone()
        profile['first_year'] = row[0] if row else 'N/A'
    
    # Get total tournaments played
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT COUNT(DISTINCT draft_year || tournament) FROM draft_results WHERE igf_golfer = '" + igf_name.replace("'", "''") + "'"
        )
        row = result.fetchone()
        profile['tournaments_played'] = row[0] if row else 0
    
    # Get wins count from igf_leaderboards
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM igf_leaderboards WHERE igf_golfer = '" + igf_name.replace("'", "''") + "' AND igf_rank = 1"
        )
        row = result.fetchone()
        profile['total_wins'] = row[0] if row else 0
    
    # Get runner-ups count
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM igf_leaderboards WHERE igf_golfer = '" + igf_name.replace("'", "''") + "' AND igf_rank = 2"
        )
        row = result.fetchone()
        profile['total_runner_ups'] = row[0] if row else 0
    
    # Get CUM wins from cum_leaderboard table
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT COUNT(*) FROM cum_leaderboard WHERE igf_golfer = '" + igf_name.replace("'", "''") + "' AND cum_rank = 1"
        )
        row = result.fetchone()
        profile['cum_wins'] = row[0] if row else 0
    
    # Get total earnings
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT COALESCE(tpc_payout,0) + COALESCE(master_payout,0) + COALESCE(pga_payout,0) + COALESCE(us_payout,0) + COALESCE(open_payout,0) + COALESCE(cum_payout,0) FROM igf_payouts WHERE igf_golfer = '" + igf_name.replace("'", "''") + "'"
        )
        row = result.fetchone()
        total = row[0] if row and row[0] else 0
        profile['total_earnings'] = "${:,}".format(int(total))
    
    # Get average IGF score
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT AVG(igf_score) FROM igf_scores WHERE igf_golfer = '" + igf_name.replace("'", "''") + "'"
        )
        row = result.fetchone()
        avg = row[0] if row and row[0] else 0
        profile['avg_igf_score'] = round(float(avg), 1) if avg else 'N/A'
    
    # Get tournament history with IGF place
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT i.draft_year, i.tournament, i.igf_rank, i.igf_score
            FROM igf_leaderboards i
            WHERE i.igf_golfer = '""" + igf_name.replace("'", "''") + """'
            ORDER BY i.draft_year DESC, i.tournament
        """)
        rows = result.fetchall()
        profile['tournament_history'] = [
            {
                'year': row[0],
                'tournament': row[1],
                'igf_place': row[2],
                'igf_score': row[3]
            }
            for row in rows
        ]
    
    # Get CUM by year
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT tournament_year,
                SUM(CASE WHEN tournament='THE PLAYERS Championship' THEN igf_score ELSE 0 END) as players,
                SUM(CASE WHEN tournament='The Masters' THEN igf_score ELSE 0 END) as masters,
                SUM(CASE WHEN tournament='PGA Championship' THEN igf_score ELSE 0 END) as pga,
                SUM(CASE WHEN tournament='U.S. Open' THEN igf_score ELSE 0 END) as us_open,
                SUM(CASE WHEN tournament='The Open Championship' THEN igf_score ELSE 0 END) as open_championship,
                SUM(igf_score) as total
            FROM igf_scores
            WHERE igf_golfer = '""" + igf_name.replace("'", "''") + """'
            GROUP BY tournament_year
            ORDER BY tournament_year DESC
        """)
        rows = result.fetchall()
        profile['cum_by_year'] = [
            {
                'year': row[0],
                'players': row[1],
                'masters': row[2],
                'pga': row[3],
                'us_open': row[4],
                'open_championship': row[5],
                'total': row[6]
            }
            for row in rows
        ]
    
    # Get yearly performance for chart
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT tournament_year, SUM(igf_score) as total_cum
            FROM igf_scores
            WHERE igf_golfer = '""" + igf_name.replace("'", "''") + """'
            GROUP BY tournament_year
            ORDER BY tournament_year ASC
        """)
        rows = result.fetchall()
        profile['yearly_performance'] = [
            {
                'year': row[0],
                'total_cum': int(row[1]) if row[1] else 0
            }
            for row in rows
        ]
    
    # Get draft history with golfer results
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT d.draft_year, d.tournament, d.overall_selection, p.player_name, t.position, t.igf_score
            FROM draft_results d
            LEFT JOIN pga_golfers p ON d.dg_id = p.dg_id
            LEFT JOIN tournament_results t ON d.dg_id = t.dg_id 
                AND d.draft_year = t.tournament_year 
                AND d.tournament = t.tournament
            WHERE d.igf_golfer = '""" + igf_name.replace("'", "''") + """'
            ORDER BY d.draft_year DESC, d.tournament, d.overall_selection
        """)
        rows = result.fetchall()
        profile['draft_history'] = [
            {
                'year': row[0],
                'tournament': row[1],
                'pick_number': row[2],
                'golfer_name': row[3] or 'N/A',
                'position': row[4] or '-',
                'igf_score': row[5] or '-'
            }
            for row in rows
        ]
    
    # Image URL (will look for /static/images/igf/{igf_name}.png)
    # Naming convention: "B. Love" -> "b_love.png"
    safe_name = igf_name.lower().replace(' ', '_').replace('.', '')
    profile['image_url'] = '/static/images/igf/' + safe_name + '.png'
    profile['safe_name'] = safe_name
    
    # Get CUM rank history
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT draft_year, cum, cum_rank
            FROM cum_leaderboard
            WHERE igf_golfer = '""" + igf_name.replace("'", "''") + """'
            ORDER BY draft_year DESC
        """)
        rows = result.fetchall()
        profile['cum_rankings'] = [
            {
                'year': row[0],
                'cum_score': row[1],
                'cum_rank': row[2]
            }
            for row in rows
        ]
    
    # Get earnings breakdown by tournament
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT 
                COALESCE(tpc_payout, 0) as tpc,
                COALESCE(master_payout, 0) as masters,
                COALESCE(pga_payout, 0) as pga,
                COALESCE(us_payout, 0) as us_open,
                COALESCE(open_payout, 0) as open_champ,
                COALESCE(cum_payout, 0) as cum
            FROM igf_payouts
            WHERE igf_golfer = '""" + igf_name.replace("'", "''") + """'
        """)
        row = result.fetchone()
        if row:
            profile['earnings_breakdown'] = {
                'tpc': "${:,}".format(int(row[0])),
                'masters': "${:,}".format(int(row[1])),
                'pga': "${:,}".format(int(row[2])),
                'us_open': "${:,}".format(int(row[3])),
                'open_champ': "${:,}".format(int(row[4])),
                'cum': "${:,}".format(int(row[5]))
            }
        else:
            profile['earnings_breakdown'] = {
                'tpc': "$0", 'masters': "$0", 'pga': "$0", 
                'us_open': "$0", 'open_champ': "$0", 'cum': "$0"
            }
    
    # Get wins breakdown by tournament from igf_winners table
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT 
                COALESCE(tpc_wins, 0) as tpc,
                COALESCE(master_wins, 0) as masters,
                COALESCE(pga_wins, 0) as pga,
                COALESCE(us_wins, 0) as us_open,
                COALESCE(open_wins, 0) as open_champ,
                COALESCE(cum_wins, 0) as cum
            FROM igf_winners
            WHERE igf_golfer = '""" + igf_name.replace("'", "''") + """'
        """)
        row = result.fetchone()
        if row:
            profile['wins_breakdown'] = {
                'tpc': int(row[0]),
                'masters': int(row[1]),
                'pga': int(row[2]),
                'us_open': int(row[3]),
                'open_champ': int(row[4]),
                'cum': int(row[5])
            }
        else:
            profile['wins_breakdown'] = {
                'tpc': 0, 'masters': 0, 'pga': 0, 
                'us_open': 0, 'open_champ': 0, 'cum': 0
            }
    
    # Get favorite golfers (most drafted)
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT p.player_name, COUNT(*) as times_drafted
            FROM draft_results d
            JOIN pga_golfers p ON d.dg_id = p.dg_id
            WHERE d.igf_golfer = '""" + igf_name.replace("'", "''") + """'
            GROUP BY p.player_name
            ORDER BY times_drafted DESC
            LIMIT 10
        """)
        rows = result.fetchall()
        profile['favorite_golfers'] = [
            {
                'golfer_name': row[0],
                'times_drafted': row[1]
            }
            for row in rows
        ]
    
    # Get best performing picks (lowest avg IGF score among golfers drafted 2+ times)
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT p.player_name, COUNT(*) as times_drafted, AVG(t.igf_score) as avg_score
            FROM draft_results d
            JOIN pga_golfers p ON d.dg_id = p.dg_id
            LEFT JOIN tournament_results t ON d.dg_id = t.dg_id 
                AND d.draft_year = t.tournament_year 
                AND d.tournament = t.tournament
            WHERE d.igf_golfer = '""" + igf_name.replace("'", "''") + """'
                AND t.igf_score IS NOT NULL
            GROUP BY p.player_name
            HAVING COUNT(*) >= 2
            ORDER BY avg_score ASC
            LIMIT 5
        """)
        rows = result.fetchall()
        profile['best_picks'] = [
            {
                'golfer_name': row[0],
                'times_drafted': row[1],
                'avg_score': round(float(row[2]), 1) if row[2] else 'N/A'
            }
            for row in rows
        ]
    
    conn.close()
    return profile


def get_all_igf_members():
    """
    Get a list of all IGF members for navigation/listing purposes.
    """
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT DISTINCT igf_golfer FROM draft_results ORDER BY igf_golfer"
        )
        members = [row[0] for row in result.fetchall()]
    conn.close()
    return members


def get_igf_member_summary():
    """
    Get summary stats for all IGF members for the members listing page.
    """
    engine = connect_tcp_socket()
    with engine.connect() as conn:
        result = conn.execute("""
            SELECT 
                d.igf_golfer,
                MIN(d.draft_year) as first_year,
                COUNT(DISTINCT d.draft_year || d.tournament) as tournaments_played,
                COALESCE(SUM(CASE WHEN l.igf_rank = 1 THEN 1 ELSE 0 END), 0) as wins,
                COALESCE(SUM(CASE WHEN l.igf_rank = 2 THEN 1 ELSE 0 END), 0) as runner_ups,
                COALESCE(p.tpc_payout, 0) + COALESCE(p.master_payout, 0) + COALESCE(p.pga_payout, 0) + 
                COALESCE(p.us_payout, 0) + COALESCE(p.open_payout, 0) + COALESCE(p.cum_payout, 0) as total_earnings
            FROM draft_results d
            LEFT JOIN igf_leaderboards l ON d.igf_golfer = l.igf_golfer 
                AND d.draft_year = l.draft_year 
                AND d.tournament = l.tournament
            LEFT JOIN igf_payouts p ON d.igf_golfer = p.igf_golfer
            GROUP BY d.igf_golfer, p.tpc_payout, p.master_payout, p.pga_payout, p.us_payout, p.open_payout, p.cum_payout
            ORDER BY total_earnings DESC NULLS LAST
        """)
        rows = result.fetchall()
        members = [
            {
                'name': row[0],
                'first_year': row[1],
                'tournaments_played': row[2],
                'wins': row[3],
                'runner_ups': row[4],
                'total_earnings': "${:,}".format(int(row[5])) if row[5] else "$0"
            }
            for row in rows
        ]
    conn.close()
    return members