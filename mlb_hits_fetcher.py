
import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

TEAMS = {
    108: "Los Angeles Angels", 109: "Arizona Diamondbacks", 110: "Baltimore Orioles",
    111: "Boston Red Sox", 112: "Chicago Cubs", 113: "Cincinnati Reds", 114: "Cleveland Guardians",
    115: "Colorado Rockies", 116: "Detroit Tigers", 117: "Houston Astros", 118: "Kansas City Royals",
    119: "Los Angeles Dodgers", 120: "Washington Nationals", 121: "New York Mets", 133: "Oakland Athletics",
    134: "Pittsburgh Pirates", 135: "San Diego Padres", 136: "Seattle Mariners", 137: "San Francisco Giants",
    138: "St. Louis Cardinals", 139: "Tampa Bay Rays", 140: "Texas Rangers", 141: "Toronto Blue Jays",
    142: "Minnesota Twins", 143: "Philadelphia Phillies", 144: "Atlanta Braves", 145: "Chicago White Sox",
    146: "Miami Marlins", 147: "New York Yankees", 158: "Milwaukee Brewers"
}

def fetch_all_hits(teams_dict, season_year):
    all_hits = []
    for team_id, team_name in teams_dict.items():
        logging.info(f"Fetching data for {team_name}...")
        try:
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&teamId={team_id}&season={season_year}&gameType=R"
            schedule = requests.get(url).json()

            for date in schedule.get("dates", []):
                for game in date.get("games", []):
                    game_id = game["gamePk"]
                    pbp_url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/playByPlay"
                    pbp = requests.get(pbp_url).json()

                    for play in pbp.get("allPlays", []):
                        batter = play.get("matchup", {}).get("batter", {})
                        for event in play.get("playEvents", []):
                            hit = event.get("hitData")
                            if hit and hit.get("coordinates"):
                                all_hits.append({
                                    "team_name": team_name,
                                    "player_name": batter.get("fullName"),
                                    "result": play["result"]["event"],
                                    "coordinates": hit.get("coordinates")
                                })
        except Exception as e:
            logging.error(f"Error fetching data for {team_name}: {e}")
    return pd.DataFrame(all_hits)

def process_hit_data(df):
    df = df[df["coordinates"].notna()].copy()
    df["x"] = df["coordinates"].apply(lambda c: c.get("coordX") if isinstance(c, dict) else None)
    df["y"] = df["coordinates"].apply(lambda c: c.get("coordY") if isinstance(c, dict) else None)
    df.dropna(subset=["x", "y"], inplace=True)
    return df
