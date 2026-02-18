from flask import Flask, render_template, url_for, request, redirect
from db import update_current_leaderboard, update_past_leaderboard, get_all_pga_for_draft, get_min_pick, submit_pick, get_pick_igf, get_all_pga, submit_research, submit_draft, get_top_picks, get_latest_field, is_field_live, post_tournament_results, update_current_draft, get_current_draft_name, create_draft_slots, test_fun, get_cum, get_champions, get_player_results, get_igf_results, test_update_current_leaderboard, fetch_golfer_stats
import os


app = Flask(__name__)

# Custom Jinja filter to convert IGF name to image filename
# "B. Love" -> "b_love"
@app.template_filter('igf_image_name')
def igf_image_name_filter(igf_name):
    if igf_name:
        return igf_name.lower().replace(' ', '_').replace('.', '')
    return ''

igf_leaderboard_heads = ('IGF Golfer', 'IGF Score', '1st', '2nd', '3rd', '4th')
live_leaderboard_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number', 'Position', 'Thru', 'Today', 'Make Cut %', 'Total','IGF Score')
live_leaderboard_heads_new = ('Golfer', 'IGFer', 'Pick', 'Pos', 'Thru', 'Today', 'Cut %', 'D. Dist', 'D. Accu', 'GIR','Great Shots','Poor Shots','Scrambling','Total','IGF Score')
final_leaderboard_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number', 'Position', 'Total','IGF Score')
just_draft_heads = ('Pro Golfer', 'IGF Golfer', 'Pick Number')

import datetime

@app.context_processor
def inject_years():
    current = datetime.date.today().year
    # for example, years 2000 up through this year:
    years = list(range(current, 2012, -1))
    return { 'years': years }

@app.route('/', methods =["GET", "POST"])
def current():
    draft_name=get_current_draft_name()
    current_draft_year=str(draft_name['draft_year'].iloc[0])
    current_draft_tourney=str(draft_name['tournament'].iloc[0])
    board_string = current_draft_year + ' ' + current_draft_tourney
    current_pick = get_min_pick()
    if current_pick == None:
        visability = ''' hidden="hidden" '''
        golferList = []
    else:
        visability = ''
        df = (
            get_latest_field(os.environ.get('DATA_GOLF_KEY'))
            if is_field_live(os.environ.get('DATA_GOLF_KEY'))
            else get_all_pga_for_draft(os.environ.get('DATA_GOLF_KEY'))
        )
        # convert to a list of simple dicts for Jinja
        golferList = df.to_dict(orient='records')
    current_igf = get_pick_igf()
    if request.method == "POST":
        submit_pick(current_pick, request.form.get("golfer"))
        return redirect(url_for('current'))
    else:
        igf_data, tournament_data, heads, igf_data_dg = update_current_leaderboard(os.environ.get('DATA_GOLF_KEY'))
        if heads == 'live':
            #headings=live_leaderboard_heads
            headings=live_leaderboard_heads_new
        elif heads == 'draft':
            headings = just_draft_heads    
        else:
            headings = final_leaderboard_heads
    return(render_template('current.html',
                               visability=visability,
                               igf_data=igf_data,
                               igf_data_dg=igf_data_dg,
                               igf_leaderboard_heads=igf_leaderboard_heads,
                               tournament_data=tournament_data,
                               headings=headings,
                               current_pick=current_pick,
                               current_igf=current_igf,
                               golferList=golferList,
                               board_string=board_string,))

@app.route('/past', methods =["GET", "POST"])
def past():
    if request.method == "POST":
       # getting input with name = dYear in HTML form
       draft_year = request.form.get("dYear")
       # getting input with name = tourney in HTML form
       tournament = request.form.get("tourney")
       igf_data, tournament_data, heads, igf_data_dg, final_ldb_heads = update_past_leaderboard(os.environ.get('DATA_GOLF_KEY'), draft_year, tournament)
       if heads == 'draft':
           headings = just_draft_heads    
       else:
           headings = final_ldb_heads
       board_string = draft_year + ' ' + tournament + ' Results'
    else:
       igf_data=()
       tournament_data=()
       board_string=''
       headings=()
       igf_data_dg=()
    return(render_template("past.html",
                           headings=headings,
                           tournament_data=tournament_data,
                           igf_leaderboard_heads=igf_leaderboard_heads,
                           igf_data = igf_data,
                           igf_data_dg=igf_data_dg,
                           board_string=board_string))

@app.route('/cum', methods=["POST", "GET"])
def cum():
    if request.method == "POST":
        # getting input with name = dYear in HTML form
        d_year = request.form.get("dYear")
        cum_data = get_cum(d_year)
        board_string = 'CUM Data for:  '+ str(d_year)
        cum_heads = ('IGF Golfer','THE PLAYERS Championship','The Masters','PGA Championship','U.S. Open','The Open Championship','Total CUM')
    else:
        cum_data = ()
        board_string = ''
        cum_heads= ()
    return(render_template('cum.html',
           cum_data=cum_data,
           cum_heads=cum_heads,
           board_string=board_string))

@app.route('/golfer_results', methods=["GET"])
def golfer_results():
    results_data = get_player_results()
    results_heads=('Golfer', 'Rounds Drafted', 'Avg IGF Score', 'Avg IGF Place', 'IGF Wins', 'IGF Win %', 'IGF Cuts', 'IGF Cut %')
    return(render_template('golfer_results.html',
           results_data=results_data,
           results_heads=results_heads))

@app.route('/api/golfer_search', methods=["GET"])
def golfer_search():
    from flask import jsonify
    from db import search_golfers
    try:
        query = request.args.get('q', '').strip()
        if len(query) < 2:
            return jsonify({'results': []})
        
        results = search_golfers(query)
        return jsonify({'results': results})
    except Exception as e:
        print(f"API Search error: {e}")
        return jsonify({'results': [], 'error': str(e)})

@app.route('/champions', methods=["GET"])
def champions():
    champs = get_champions()
    champ_heads = ('Year','THE PLAYERS Championship','The Masters','PGA Championship','U.S. Open','The Open Championship','Ryder Cup')
    return(render_template('champions.html',
                           champs=champs,
                           champ_heads=champ_heads))
    
@app.route('/igf_results', methods=["POST", "GET"])
def igf_results():
    if request.method == "POST":
        view_by = request.form.get("view_by")
        if view_by == 'first':
            head_str = ' Winners'
            igf_heads = ('Golfer','THE PLAYERS Championship','The Masters','PGA Championship','U.S. Open','The Open Championship', 'CUM', 'Ryder Cup', 'Total')
        elif view_by == 'second':
            head_str = ' Runner Ups'
            igf_heads = ('Golfer','THE PLAYERS Championship','The Masters','PGA Championship','U.S. Open','The Open Championship', 'CUM', 'Ryder Cup', 'Total')
        elif view_by == 'money':
            head_str = ' Earnings'
            igf_heads = ('Golfer','THE PLAYERS Championship','The Masters','PGA Championship','U.S. Open','The Open Championship', 'CUM', 'Ryder Cup', 'Total')
        results = get_igf_results(view_by)
    else:
        results=()
        igf_heads=()
        head_str=''
    return(render_template('igf_results.html',
                           results=results,
                           igf_heads=igf_heads,
                           head_str=head_str))
    
@app.route('/hard_on', methods=["POST", "GET"])
def hard_on():
    if request.method == "POST":
        # getting input with name = dYear in HTML form
        draft_year = request.form.get("dYear")
        # getting input with name = tourney in HTML form
        tournament = request.form.get("tourney")
        hard_on_data = get_top_picks(draft_year, tournament)
        board_string = 'Hard On Data for:  '+ tournament + ' for ' + str(draft_year)
        hard_on_heads = ('IGF Golfer','Player Name','Hard On Count')
        tab_name='hard_on'
    else:
        hard_on_data = ()
        board_string = ''
        hard_on_heads= ()
        tab_name=''
    return(render_template('hard_on.html',
           hard_on_data=hard_on_data,
           hard_on_heads=hard_on_heads,
           board_string=board_string,
           tab_name=tab_name))

@app.route('/admin', methods=["POST", "GET"])
def admin():
    golferList = get_all_pga(os.environ.get('DATA_GOLF_KEY'))
    if request.method == "POST":
        submit_draft(request.form.get("dYear"), request.form.get("tourney"), request.form.get("selection"), request.form.get("golfer"))
        return redirect(url_for('admin'))
    return render_template("admin.html", golferList=golferList)

@app.route('/post', methods=["POST", "GET"])
def post():
    if request.method == "POST":
        draft_year = request.form.get("dYear")
        tournament = request.form.get("tourney")
        final_leaderboard_heads=('dg id', 'Position','Pro Golfer',  'Total','IGF Score', 'Year', 'Tournament')
        igf_data, heads = post_tournament_results(os.environ.get('DATA_GOLF_KEY'), draft_year, tournament)
    else:
        igf_data=()
        final_leaderboard_heads=()
    return(render_template('post.html',
                               igf_data=igf_data,
                               final_leaderboard_heads=final_leaderboard_heads))
    
@app.route('/update_draft', methods=["POST", "GET"])
def update_draft():
    current_draft_heads=('Year', 'Tournament')
    if request.method == "POST":
        draft_year = request.form.get("dYear")
        tournament = request.form.get("tourney")
        update_current_draft(draft_year, tournament)
        return redirect(url_for('update_draft'))
    else:
        draft_name=get_current_draft_name()
        current_draft_year=draft_name['draft_year'].iloc[0]
        current_draft_tourney=draft_name['tournament'].iloc[0]
    return(render_template('update_draft.html',
                               current_draft_year=current_draft_year,
                               current_draft_tourney=current_draft_tourney,
                               current_draft_heads=current_draft_heads))
    
@app.route('/draft_order', methods=["POST", "GET"])
def draft_order():
    if request.method == "POST":
        draft_year = request.form.get("dYear")
        tournament = request.form.get("tourney")
        draft_dict={'B. Love': str(request.form.get("love")),
            'D. Quinn':str(request.form.get("quinn")),
            'D. Virtue': str(request.form.get("virtue")),
            'J. Brackmann': str(request.form.get("brackmann")),
            'J. Arpaia': str(request.form.get("arpaia")),
            'J. Nieting': str(request.form.get("nieting")),
            'M. Massa': str(request.form.get("massa")),
            'R. Ross': str(request.form.get("ross")),
            'T. Olsen': str(request.form.get("olsen")),
            'T. Morso': str(request.form.get("morso"))}
        create_draft_slots(tournament, draft_year, draft_dict)
        return redirect(url_for('draft_order'))
    return(render_template('draft_order.html'))

@app.route('/test', methods =["GET", "POST"])
def test():
    igf_data, tournament_data, heads, igf_data_dg, reduced_tuple_dg = test_update_current_leaderboard(os.environ.get('DATA_GOLF_KEY'))
    return(render_template("test.html",
                           tournament_data=tournament_data,
                           igf_data_dg=igf_data_dg,
                           igf_data = igf_data,
                           reduced_tuple_dg=reduced_tuple_dg))

@app.route('/site_map', methods =["GET"])
def site_map():
    return(render_template("site_map.html"))

@app.route('/research', methods=["GET"])
def research():
    # Redirect to golfer_results - DataGolf stats now shown on individual golfer profiles
    return redirect(url_for('golfer_results'))

@app.route('/igf/<igf_name>', methods=["GET"])
def igf_profile(igf_name):
    from db import get_igf_profile_data
    
    # Get all profile data for this IGFer
    profile_data = get_igf_profile_data(igf_name)
    
    if profile_data is None:
        return render_template('404.html', message=f"IGFer '{igf_name}' not found"), 404
    
    return render_template(
        'igf_profile.html',
        igf_name=igf_name,
        profile=profile_data
    )

@app.route('/igf_members', methods=["GET"])
def igf_members():
    from db import get_all_igf_members, get_igf_member_summary
    
    members = get_igf_member_summary()
    
    return render_template(
        'igf_members.html',
        members=members
    )

@app.route('/golfer/<int:dg_id>', methods=["GET"])
def golfer_profile(dg_id):
    from db import get_golfer_profile_data, fetch_golfer_stats_by_id
    
    # Get all profile data for this golfer
    profile_data = get_golfer_profile_data(dg_id)
    
    if profile_data is None:
        return render_template('404.html', message=f"Golfer not found"), 404
    
    # Get DataGolf strokes gained stats
    api_key = os.environ.get('DATA_GOLF_KEY')
    dg_stats = fetch_golfer_stats_by_id(api_key, dg_id)
    
    return render_template(
        'golfer_profile.html',
        dg_id=dg_id,
        profile=profile_data,
        dg_stats=dg_stats
    )

# Error Handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html', error_code=404), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('404.html', 
                           error_code=500, 
                           message="Something went wrong on our end. The greenskeeper has been notified."), 500

@app.errorhandler(Exception)
def handle_exception(e):
    # Pass through HTTP errors
    if hasattr(e, 'code'):
        if e.code == 404:
            return render_template('404.html', error_code=404), 404
        return render_template('404.html', error_code=e.code, message=str(e)), e.code
    # Handle non-HTTP exceptions
    return render_template('404.html', 
                           error_code=500, 
                           message="An unexpected error occurred. Please try again."), 500

if __name__ == "__main__":
    app.run(debug=True)
