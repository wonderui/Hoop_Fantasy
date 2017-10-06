# import modules ----------------------

import nba_py
import nba_py.game
import nba_py.player
import nba_py.team

import pandas as pd
import numpy as np

old_settings = np.seterr(all='print')
np.geterr()

print('modules imported')


# define functions ----------------------

def get_games(date):
    """
    :param date: datetime.date, the match day
    :return: df, all the games on the given day
    """
    return nba_py.Scoreboard(month=date.month,
                             day=date.day,
                             year=date.year,
                             league_id='00',
                             offset=0).game_header()[['GAME_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID']]


def get_players(games, all_players):
    """
    :param games: df, some games
    :param all_players: df, all players list of this season
    :return: df, all players of the given games
    """
    home_team_player = all_players[all_players['TEAM_ID'].isin(games['HOME_TEAM_ID'])][['PERSON_ID', 'TEAM_ID']]
    home_team_player['Location'] = 'HOME'
    away_team_player = all_players[all_players['TEAM_ID'].isin(games['VISITOR_TEAM_ID'])][['PERSON_ID', 'TEAM_ID']]
    away_team_player['Location'] = 'AWAY'
    players = pd.concat([home_team_player, away_team_player])
    game_team = pd.concat([games[['HOME_TEAM_ID', 'GAME_ID']].rename(columns={'HOME_TEAM_ID': 'TEAM_ID'}),
                           games[['VISITOR_TEAM_ID', 'GAME_ID']].rename(columns={'VISITOR_TEAM_ID': 'TEAM_ID'})])
    players = pd.merge(players, game_team, on='TEAM_ID')
    team_team = pd.concat(
        [games[['HOME_TEAM_ID', 'VISITOR_TEAM_ID']].rename(columns={'HOME_TEAM_ID': 'TEAM_ID',
                                                                    'VISITOR_TEAM_ID': 'Against_Team_ID'}),
         games[['VISITOR_TEAM_ID', 'HOME_TEAM_ID']].rename(columns={'VISITOR_TEAM_ID': 'TEAM_ID',
                                                                    'HOME_TEAM_ID': 'Against_Team_ID'})])
    return pd.merge(players, team_team, on='TEAM_ID')


def get_players_p(games, game_stats_logs):
    """
    :param games: df, some games
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :return: df, all players of the given games at the match date
    """
    players = pd.DataFrame()
    for i in games.index:
        players = players.append(game_stats_logs[(game_stats_logs['GAME_ID'] == games.iloc[i]['GAME_ID']) &
                                                 (game_stats_logs['TEAM_ID'] == games.iloc[i]['HOME_TEAM_ID'])])
        players = players.append(game_stats_logs[(game_stats_logs['GAME_ID'] == games.iloc[i]['GAME_ID']) &
                                                 (game_stats_logs['TEAM_ID'] == games.iloc[i]['VISITOR_TEAM_ID'])])

    players['Location'] = players.apply(lambda x: 'HOME' if x['TEAM_ID'] ==
                                        int(games[games['GAME_ID'] == x['GAME_ID']]['HOME_TEAM_ID'])
                                        else 'AWAY', axis=1)

    team_team = pd.concat(
            [games[['HOME_TEAM_ID', 'VISITOR_TEAM_ID']].rename(columns={'HOME_TEAM_ID': 'TEAM_ID',
                                                                        'VISITOR_TEAM_ID': 'Against_Team_ID'}),
             games[['VISITOR_TEAM_ID', 'HOME_TEAM_ID']].rename(columns={'VISITOR_TEAM_ID': 'TEAM_ID',
                                                                        'HOME_TEAM_ID': 'Against_Team_ID'})])

    return pd.merge(players, team_team,
                    on='TEAM_ID')[['PLAYER_ID', 'TEAM_ID', 'Location', 'GAME_ID',
                                   'Against_Team_ID']].rename(columns={'PLAYER_ID': 'PERSON_ID'})


def get_last_n_game_logs(game_stats_logs, player_id, game_id, n):
    """
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :param player_id: int, player id
    :param game_id: str, game id
    :param n: int, size of games
    :return: df, the n game log of the player before the given game
    """
    player_game_logs = game_stats_logs[game_stats_logs['PLAYER_ID'] == player_id]
    last_n_game = player_game_logs[player_game_logs['GAME_ID_O'] < game_id].sort_values('GAME_ID_O').tail(n)
    return last_n_game[['MINS', 'PTS', 'AST', 'OREB', 'DREB', 'STL', 'BLK', 'TO', 'FGM', 'FGA', 'FG3M']]


def get_score_36(game_logs):
    """
    :param game_logs: df, game logs
    :return: list, [0]the average fantasy score(int, 36mins) of the given game log, [1]the score cov(float) of the 
    given game log
    """
    convert_to_36 = lambda x: x[['PTS', 'AST', 'OREB', 'DREB', 'STL', 'BLK',
                                 'TO', 'FGM', 'FGA', 'FG3M']] * 36 / x['MINS']
    stats = game_logs.apply(convert_to_36, axis=1)
    stats['SCO'] = stats['PTS'] * 1 + stats['AST'] * 1.5 + stats['OREB'] * 1 + stats['DREB'] * 0.7 + \
        stats['STL'] * 2 + stats['BLK'] * 1.8 + stats['TO'] * -1 + \
        stats['FGM'] * 0.4 + (stats['FGA'] - stats['FGM']) * -1 + stats['FG3M'] * 0.5
    return stats['SCO'].mean(), stats['SCO'].std() / stats['SCO'].mean()


def get_ma(game_stats_logs, row, n):
    """
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :param row: pd.series, player id and game id
    :param n: int, size of ma
    :return: float, average fantasy score of the player in n games before the given game
    """
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    ma_n = get_score_36(get_last_n_game_logs(game_stats_logs, player_id, game_id_o, n))[0]
    return round(float(ma_n), 2)


def get_min(game_stats_logs, row, n):
    """
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :param row: pd.series, player id and game id
    :param n: int, size of ma
    :return: float, average mins the player played in n games before the given game
    """
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    min_n = get_last_n_game_logs(game_stats_logs, player_id, game_id_o, n)['MINS'].mean()
    return round(float(min_n), 2)


def get_min_cov(game_stats_logs, row, n):
    """
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :param row: pd.series, player id and game id
    :param n: int, size of ma
    :return: float, cov of mins the player played in n games before the given game
    """
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    min_cov_n = get_last_n_game_logs(game_stats_logs,
                                     player_id,
                                     game_id_o,
                                     n)['MINS'].std() / get_last_n_game_logs(game_stats_logs,
                                                                             player_id,
                                                                             game_id_o,
                                                                             n)['MINS'].mean()
    return round(float(min_cov_n), 3)


def get_sco_cov(game_stats_logs, row, n):
    """
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :param row: pd.series, player id and game id
    :param n: int, size of ma
    :return: float, cov of scores the player get in n games before the given game
    """
    player_id = row['PERSON_ID']
    game_id_o = row['GAME_ID'][3:5] + row['GAME_ID'][:3] + row['GAME_ID'][-5:]
    sco_cov_n = get_score_36(get_last_n_game_logs(game_stats_logs, player_id, game_id_o, n))[1]
    return round(float(sco_cov_n), 3)


def get_exp_sco(players, game_stats_logs):
    """
    :param players: df, players list
    :param game_stats_logs: df, all previous game stats logs imported from sql
    :return: df, all players with their expect fantasy score
    """
    players['MA_20'] = players.apply(lambda x: get_ma(game_stats_logs, x, 20), axis=1)
    print('ma20 complete!')
    players['MA_10'] = players.apply(lambda x: get_ma(game_stats_logs, x, 10), axis=1)
    print('ma10 complete!')
    players['MA_5'] = players.apply(lambda x: get_ma(game_stats_logs, x, 5), axis=1)
    print('ma5 complete!')
    players['MIN_20'] = players.apply(lambda x: get_min(game_stats_logs, x, 20), axis=1)
    print('min20 complete!')
    players['MIN_10'] = players.apply(lambda x: get_min(game_stats_logs, x, 10), axis=1)
    print('min10 complete!')
    players['MIN_5'] = players.apply(lambda x: get_min(game_stats_logs, x, 5), axis=1)
    print('min5 complete!')
    players['MIN_COV_20'] = players.apply(lambda x: get_min_cov(game_stats_logs, x, 20), axis=1)
    print('min_cov_20 complete!')
    players['SCO_COV_20'] = players.apply(lambda x: get_sco_cov(game_stats_logs, x, 20), axis=1)
    print('sco_cov_20 complete!')
    players = players[players['SCO_COV_20'] > 0].copy()
    print('sco cov less than 0 droped!')

    players['EXP_SCO'] = round(players[['MA_20', 'MA_10', 'MA_5']].mean(axis=1) *
                               players[['MIN_20', 'MIN_10', 'MIN_5']].mean(axis=1) / 36, 2)
    print('all done!')
    return players


print('functions defined')
