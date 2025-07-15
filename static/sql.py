# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 00:06:19 2023

@author: david
"""

#insert into igf_results (igf_golfer, tournament_year, tournament, igf_score) select igf_golfer, tournament_year, tournament, sum(igf_score) from (select scores.*, row_number() over (partition by igf_golfer, tournament, tournament_year order by igf_score asc)
#from (select igf_golfer, d.dg_id,  igf_score, t.tournament, tournament_year from draft_results d left join tournament_results t on d.dg_id = t.dg_id and d.tournament = t.tournament and d.draft_year = t.tournament_year) scores) ranked_scores where row_number <=3 group by igf_golfer, tournament_year, tournament order by igf_golfer, tournament, tournament_year;

#select tournament_year, igf_golfer, sum(igf_score) as cum from igf_results group by igf_golfer, tournament_year order by tournament_year desc, cum asc; 

#select scores.*, row_number from (select tournament_year, tournament, igf_golfer, row_number() over (partition by tournament_year, tournament order by igf_score asc) from igf_results) scores where row_number <2 order by tournament_year, tournament, row_number asc;

#insert into tournament_results (igf_golfer, tournament_year, tournament, igf_score) ();