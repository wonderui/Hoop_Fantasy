# import modules ----------------------

import nba_py
import nba_py.game
import nba_py.player
import nba_py.team

import pandas as pd
import numpy as np

import datetime
# import time
# import functools

# import pymysql
from sqlalchemy import create_engine

import sys
sys.path.append('/Users/wonderui/OneDrive/6_Module_Package')
sys.path.append('/Users/WangRui/OneDrive/6_Module_Package')
import hoop_pwd
pwd = hoop_pwd.password

old_settings = np.seterr(all='print')
np.geterr()

print('modules imported')


# create sql conn and load game stats logs ----------------------

conn = create_engine('mysql+pymysql://root:%s@118.190.202.87:3306/nba_stats' % pwd)
game_stats_logs = pd.read_sql_table('game_stats_logs', conn)
game_stats_logs = game_stats_logs[game_stats_logs['GAME_TYPE'] != 'all_star']
print(str(len(game_stats_logs)) + ' player stats loaded.')


# set date ----------------------

today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
someday = datetime.date(2017, 1, 1)

print('date set')


# load player list ----------------------

all_players = nba_py.player.PlayerList(season='2017-18').info()

print('players list loaded')


# define functions ----------------------

def get_games(date):
    '''
    :param date: datetime.date, the match day
    :return: df, all the games on the given day
    '''
    return nba_py.Scoreboard(month=date.month,
                             day=date.day,
                             year=date.year,
                             league_id='00',
                             offset=0).game_header()[['GAME_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID']]


def get_players(games):
    '''
    :param games: df, some games
    :return: df, all players of the given games
    '''
    home_team_player = all_players[all_players['TEAM_ID'].isin(games['HOME_TEAM_ID'])][['PERSON_ID', 'TEAM_ID']]
    home_team_player['Location'] = 'HOME'
    away_team_player = all_players[all_players['TEAM_ID'].isin(games['VISITOR_TEAM_ID'])][['PERSON_ID', 'TEAM_ID']]
    away_team_player['Location'] = 'AWAY'
    players = pd.concat([home_team_player, away_team_player])
    game_team = pd.concat([games[['HOME_TEAM_ID', 'GAME_ID']].rename(columns={'HOME_TEAM_ID': 'TEAM_ID'}),
                           games[['VISITOR_TEAM_ID', 'GAME_ID']].rename(columns={'VISITOR_TEAM_ID': 'TEAM_ID'})])
    players = pd.merge(players, game_team, on='TEAM_ID')
    team_team = pd.concat([games[['HOME_TEAM_ID', 'VISITOR_TEAM_ID']].\
                           rename(columns={'HOME_TEAM_ID': 'TEAM_ID', 'VISITOR_TEAM_ID': 'Against_Team_ID'}),
                           games[['VISITOR_TEAM_ID', 'HOME_TEAM_ID']].\
                           rename(columns={'VISITOR_TEAM_ID': 'TEAM_ID', 'HOME_TEAM_ID': 'Against_Team_ID'})])
    return pd.merge(players, team_team, on='TEAM_ID')


def get_last_n_game_logs(player_id, game_id, n):
    '''
    :param player_id: int, player id
    :param game_id: str, game id
    :param n: int, size of games
    :return: df, the n game log of the player before the given game
    '''
    player_game_logs = game_stats_logs[game_stats_logs['PLAYER_ID'] == player_id]
    last_n_game = player_game_logs[player_game_logs['GAME_ID_O'] < game_id].sort_values('GAME_ID_O').tail(n)
    return last_n_game[['MINS', 'PTS', 'AST', 'OREB', 'DREB', 'STL', 'BLK', 'TO', 'FGM', 'FGA', 'FG3M']]


def get_score_36(game_logs):
    '''
    :param game_logs: df, game logs
    :return: list, [0]the average fantasy score(int, 36mins) of the given game log, [1]the score cov(float) of the 
    given game log
    '''
    convert_to_36 = lambda x: x[['PTS', 'AST', 'OREB', 'DREB', 'STL', 'BLK',
                                 'TO', 'FGM', 'FGA', 'FG3M']] * 36 / x['MINS']
    stats = game_logs.apply(convert_to_36, axis=1)
    stats['SCO'] = stats['PTS'] * 1 + stats['AST'] * 1.5 + \
    stats['OREB'] * 1 + stats['DREB'] * 0.7 + \
    stats['STL'] * 2 + stats['BLK'] * 1.8 + stats['TO'] * -1 + \
    stats['FGM'] * 0.4 + (stats['FGA'] - stats['FGM']) * -1 + stats['FG3M'] * 0.5
    return stats['SCO'].mean(), stats['SCO'].std()/stats['SCO'].mean()


def get_ma(row, n):
    '''
    :param row: pd.series, player id and game id
    :param n: int, size of ma
    :return: float, average fantasy score of the player in n games before the given game
    '''
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    ma_n = get_score_36(get_last_n_game_logs(player_id, game_id_o, n))[0]
    return round(float(ma_n), 2)


def get_min(row, n):
    '''
    :param row: 
    :param n: 
    :return: 
    '''
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    min_n = get_last_n_game_logs(player_id, game_id_o, n)['MINS'].mean()
    return round(float(min_n), 2)


def get_min_cov(row, n):
    '''
    :param row: 
    :param n: 
    :return: 
    '''
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    min_cov_n = get_last_n_game_logs(player_id, game_id_o, n)['MINS'].std()/\
    get_last_n_game_logs(player_id, game_id_o, n)['MINS'].mean()
    return round(float(min_cov_n), 3)


def get_sco_cov(row, n):
    '''
    :param row: 
    :param n: 
    :return: 
    '''
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    sco_cov_n = get_score_36(get_last_n_game_logs(player_id, game_id_o, n))[1]
    return round(float(sco_cov_n), 3)

print('functions defined')


# execute the programme to get the expect score ----------------------

games = get_games(someday)

players = get_players(games)

players['MA_20'] = players.apply(lambda x: get_ma(x, 20), axis=1)
players['MA_10'] = players.apply(lambda x: get_ma(x, 10), axis=1)
players['MA_5'] = players.apply(lambda x: get_ma(x, 5), axis=1)
players['MIN_20'] = players.apply(lambda x: get_min(x, 20), axis=1)
players['MIN_10'] = players.apply(lambda x: get_min(x, 10), axis=1)
players['MIN_5'] = players.apply(lambda x: get_min(x, 5), axis=1)
players['MIN_COV_20'] = players.apply(lambda x: get_min_cov(x, 20), axis=1)
players['SCO_COV_20'] = players.apply(lambda x: get_sco_cov(x, 20), axis=1)

players['EXP_SCO'] = round(players[['MA_20', 'MA_10', 'MA_5']].mean(axis=1) *
                           players[['MIN_20', 'MIN_10', 'MIN_5']].mean(axis=1) / 36, 2)
print(players)
