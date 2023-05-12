from concurrent.futures import as_completed
import json
import pandas as pd
import requests
import tqdm
from itertools import chain
from requests_futures.sessions import FuturesSession

def game_pks_from_date(start_date: str, end_date: str, sport_id: str = "11") -> list[str]:
    """ Wraps around the mlb stats api to gather gamePks to search statcast or other sources.

    Args:
        start_date: String containing a date in yyyy-mm-dd format to start the search (inclusive)
        end_date: String containing a date in yyyy-mm-dd format to end the search (inclusive)

    Returns:
        A list of strings of gamePks from the specified dates 
    """
    content = json.loads(requests.get("https://statsapi.mlb.com/api/v1/schedule", params={"sportId": sport_id, "startDate": start_date, "endDate": end_date}).content)
    dates = content['dates']
    games = list(chain(*[date['games'] for date in dates]))
    # TODO build in option for statcast data and find way to identify statcast elligibility from schedule api (via venue?)
    return [game["gamePk"] for game in games if game['status']['codedGameState'] == "F"]

def pitchdata_from_pks(pks: list[str], max_net_threads: int =  10) -> pd.DataFrame:
    """ Uses multiple threads to gather relevant pitch by pitch data from game schedule

    Args:
        pks: A list of pk strings to search for stats
        max_net_threads: An int representing the number of network threads to exsist at any given time

    Returns:
        A list of strings of gamePks from the specified dates 
    """
    session = FuturesSession(max_workers=max_net_threads)
    futures = [session.get("https://baseballsavant.mlb.com/gf", params={"game_pk": pk}) for pk in pks]
    master_table = pd.DataFrame()

    print("Gathering Games")
    with tqdm(total=len(futures)) as pbar:
        for completed_future in as_completed(futures):
            res =json.loads(completed_future.result().content)
            velo = pd.DataFrame(res['exit_velocity'])
            table = pd.DataFrame(res['team_home']+res['team_away'])
            table = table.merge(velo, on="play_id", how="left")
            master_table = pd.concat([master_table, table])
            pbar.update(1)

    return master_table