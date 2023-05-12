from concurrent.futures import as_completed
import json
from typing import List
import pandas as pd
import requests
from tqdm import tqdm
from itertools import chain
from requests_futures.sessions import FuturesSession

# These functions allow for relatively quick searching of milb games and statcast data
# EX: pitchdata_from_pks(game_pks_from_date("2023-05-01", "2023-05-10"))
# Creates a df that has pitch level data from triple a milb games with statcast data
# from May 1st to May 10th!


def game_pks_from_date(
    start_dt: str, end_dt: str, sport_id: str = "11", team_id: str = None
) -> List[str]:
    """
    Wraps around the mlb stats api to gather gamePks to search statcast or other sources.

    Args:
        start_date: String containing a date in yyyy-mm-dd format to start the search (inclusive)
        end_date: String containing a date in yyyy-mm-dd format to end the search (inclusive)

    Returns:
        A list of strings of gamePks from the specified dates
    """
    content = json.loads(
        requests.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={
                "sportId": sport_id,
                "startDate": start_dt,
                "endDate": end_dt,
                "hydrate": "team",
                "teamId": team_id,
            },
        ).content
    )
    dates = content["dates"]
    games = list(chain(*[date["games"] for date in dates]))
    # TODO build in option for statcast data and find way to identify statcast elligibility from schedule api (via venue?)
    return [game["gamePk"] for game in games if game["status"]["codedGameState"] == "F"]


def pitchdata_from_pks(
    pks: list[str], statcast_only=True, max_net_threads: int = 10
) -> pd.DataFrame:
    """
    Uses multiple threads to gather relevant pitch by pitch data from game schedule

    Note -> Games with no statcast data have empty

    Args:
        pks: A list of pk strings to search for stats
        statcast_only: Only pull data from games with statcast data
        max_net_threads: An int representing the number of network threads to exsist at any given time

    Returns:
        A list of strings of gamePks from the specified dates
    """
    session = FuturesSession(max_workers=max_net_threads)
    futures = [
        session.get("https://baseballsavant.mlb.com/gf", params={"game_pk": pk})
        for pk in pks
    ]
    results = []

    with tqdm(total=len(futures), desc="Gathering Data", unit="Game") as pbar:
        for completed_future in as_completed(futures):
            res = json.loads(completed_future.result().content)
            velo_data = pd.DataFrame(res["exit_velocity"])
            if velo_data.size > 0 or not statcast_only:
                PA_data = pd.DataFrame(res["team_home"] + res["team_away"])
                # Only perform join if velo has size (if statcast)
                if velo_data.size > 0:
                    PA_data = PA_data.merge(
                        velo_data, on="play_id", how="left", suffixes=("", "_DROP")
                    ).filter(regex="^(?!.*_DROP)")
                results.append(PA_data)
            pbar.update(1)

    return pd.concat(results)
