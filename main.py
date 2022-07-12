def datagolf_url(endpoint):
    base_url = 'https://feeds.datagolf.com/'
    return(base_url + endpoint)

def get_latest_scoring(api_key):
    params={'display':'value', 'file_format':'json', 'key':api_key}
    resp=requests.request("GET", datagolf_url('preds/live-tournament-stats'), params=params)
    envelope = resp.json()
    event_name = envelope['event_name']
    live_stats = envelope['live_stats']
    leaderboard = pd.DataFrame(live_stats,columns=['dg_id','player_name','position','sg_app','sg_arg','sg_ott','sg_putt','sg_t2g','sg_total','thru','today','total'] )
    return(event_name, leaderboard)
  
def get_current_draft_board(conn):
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''SELECT * from draft_results where draft_year = (select draft_year from current_draft) and tournament = (select tournament from current_draft)''')
    result = cursor.fetchall()
    draft_results=pd.DataFrame(result, columns=['key','dg_id', 'draft_round', 'draft_year', 'tournament', 'igf_golfer', 'draft_slot', 'pick_number'])
    return(draft_results)
  
def get_igf_score(leaderboard, dg_id):
    #first fetch the players score (potentially only through 36 or WD)
    current_score = leaderboard[leaderboard['dg_id']==dg_id]['total'].iloc[0]
    worst_made_cut = max(leaderboard[(leaderboard['position'] != 'CUT') & (leaderboard['position'] != 'WD')]['total'])
    if leaderboard[(leaderboard['dg_id'] == dg_id)]['position'].iloc[0] == 'CUT':
        igf_score=max(current_score,worst_made_cut+2)
    elif leaderboard[(leaderboard['dg_id'] == dg_id)]['position'].iloc[0] == 'WD':
        igf_score=max(current_score,worst_made_cut+7)
    else:
        igf_score=current_score
    return(igf_score)
  
def update_leaderboard(datagolf_key, conn):
    name, leaderboard = get_latest_scoring(datagolf_key)
    draft_results = get_current_draft_board(conn)
    combined = draft_results.merge(leaderboard, how='left', on='dg_id')
    combined['igf_score'] = combined.apply(lambda x: get_igf_score(leaderboard, x.dg_id), 1)
    igf_leaderboard = combined.groupby('igf_golfer')['igf_score'].nsmallest(3).groupby('igf_golfer').sum().to_dict()
    reduced = combined[['player_name', 'igf_golfer', 'pick_number', 'position', 'sg_total', 'thru', 'today', 'total', 'igf_score']].set_index(combined.dg_id)
    live_leaderboard = reduced.to_dict()
    return(live_leaderboard, igf_leaderboard)
