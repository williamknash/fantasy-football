#!/usr/bin/env python3
"""
Background job to fetch NFL player scoring data from RapidAPI.

This script:
1. Connects to Google Sheets using service account credentials
2. Reads the Schedule worksheet to find active/recent games
3. Fetches player stats from RapidAPI for players in active games
4. Writes/updates scores in the Scores worksheet

Usage:
    python scoring_job.py
    python scoring_job.py --week Wildcard    # Override week (for testing)
    python scoring_job.py --week "Week 18"   # Test with specific week

Recommended cron schedule (during game days):
    */5 12-23 * * 0,6 /path/to/venv/bin/python /path/to/scoring_job.py >> scoring.log 2>&1
"""

import argparse
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RapidAPI configuration
RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"
RAPIDAPI_ENDPOINT = "/getNFLGamesForPlayer"

# Fantasy scoring rules (passed to API)
SCORING_RULES = {
    "passYards": ".04",
    "passTD": "4",
    "passInterceptions": "-2",
    "pointsPerReception": "0",
    "rushYards": ".1",
    "rushTD": "6",
    "fumbles": "-2",
    "receivingYards": ".1",
    "receivingTD": "6",
    "targets": "0",
    "defTD": "6",
    "xpMade": "1",
    "xpMissed": "-1",
    "fgMade": "3",
    "fgMissed": "-3",
}

# Rate limiting
API_CALL_DELAY_SECONDS = 1.0
MAX_RETRIES = 3


class Config:
    """Configuration loaded from secrets.toml."""

    def __init__(self):
        self.rapidapi_key: str = ""
        self.spreadsheet_url: str = ""
        self.gcp_credentials: Dict[str, Any] = {}

    @classmethod
    def from_secrets_toml(cls, secrets_path: str = ".streamlit/secrets.toml") -> "Config":
        """Load configuration from Streamlit secrets.toml file."""
        import tomli

        config = cls()
        secrets_file = Path(secrets_path)

        if not secrets_file.exists():
            raise FileNotFoundError(f"Secrets file not found: {secrets_path}")

        with open(secrets_file, "rb") as f:
            secrets = tomli.load(f)

        # Load RapidAPI key
        config.rapidapi_key = secrets.get("rapidapi", {}).get("key", "")

        # Load Google Sheets connection info
        gsheets = secrets.get("connections", {}).get("gsheets", {})
        config.spreadsheet_url = gsheets.get("spreadsheet", "")

        # Build GCP credentials dict
        config.gcp_credentials = {
            "type": gsheets.get("type", "service_account"),
            "project_id": gsheets.get("project_id", ""),
            "private_key_id": gsheets.get("private_key_id", ""),
            "private_key": gsheets.get("private_key", ""),
            "client_email": gsheets.get("client_email", ""),
            "client_id": gsheets.get("client_id", ""),
            "auth_uri": gsheets.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": gsheets.get("token_uri", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": gsheets.get(
                "auth_provider_x509_cert_url",
                "https://www.googleapis.com/oauth2/v1/certs"
            ),
            "client_x509_cert_url": gsheets.get("client_x509_cert_url", ""),
        }

        return config

    @classmethod
    def from_environment(cls) -> "Config":
        """Load configuration from environment variables (for GitHub Actions)."""
        config = cls()

        # Load RapidAPI key
        config.rapidapi_key = os.environ.get("RAPIDAPI_KEY", "")

        # Load Google Sheets connection info
        config.spreadsheet_url = os.environ.get("SPREADSHEET_URL", "")

        # Build GCP credentials dict from environment
        config.gcp_credentials = {
            "type": os.environ.get("GCP_TYPE", "service_account"),
            "project_id": os.environ.get("GCP_PROJECT_ID", ""),
            "private_key_id": os.environ.get("GCP_PRIVATE_KEY_ID", ""),
            "private_key": os.environ.get("GCP_PRIVATE_KEY", "").replace("\\n", "\n"),
            "client_email": os.environ.get("GCP_CLIENT_EMAIL", ""),
            "client_id": os.environ.get("GCP_CLIENT_ID", ""),
            "auth_uri": os.environ.get("GCP_AUTH_URI", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": os.environ.get("GCP_TOKEN_URI", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": os.environ.get(
                "GCP_AUTH_PROVIDER_CERT_URL",
                "https://www.googleapis.com/oauth2/v1/certs"
            ),
            "client_x509_cert_url": os.environ.get("GCP_CLIENT_CERT_URL", ""),
        }

        return config


class GoogleSheetsClient:
    """Client for reading/writing to Google Sheets."""

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self, credentials_dict: Dict[str, Any], spreadsheet_url: str):
        self.credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=self.SCOPES
        )
        self.client = gspread.authorize(self.credentials)
        self.spreadsheet = self.client.open_by_url(spreadsheet_url)

    def read_worksheet(self, worksheet_name: str) -> pd.DataFrame:
        """Read a worksheet into a DataFrame."""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
            data = worksheet.get_all_records()
            return pd.DataFrame(data)
        except gspread.WorksheetNotFound:
            logger.warning(f"Worksheet '{worksheet_name}' not found")
            return pd.DataFrame()

    def write_worksheet(self, worksheet_name: str, df: pd.DataFrame):
        """Write a DataFrame to a worksheet (replaces all data)."""
        try:
            worksheet = self.spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = self.spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=max(100, len(df) + 10),
                cols=len(df.columns) + 2
            )
            logger.info(f"Created new worksheet: {worksheet_name}")

        worksheet.clear()

        if not df.empty:
            # Convert all values to strings to avoid serialization issues
            df_str = df.astype(str)
            data = [df_str.columns.tolist()] + df_str.values.tolist()
            worksheet.update('A1', data)


class RapidAPIClient:
    """Client for fetching NFL stats from RapidAPI."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://{RAPIDAPI_HOST}"

    def get_player_stats(
        self,
        player_id: str,
        num_games: int = 1
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch player stats for recent games.

        Args:
            player_id: The Tank01 player ID
            num_games: Number of recent games to fetch

        Returns:
            Dict with player stats and fantasy points, or None on error
        """
        url = f"{self.base_url}{RAPIDAPI_ENDPOINT}"

        params = {
            "playerID": player_id,
            "numberOfGames": str(num_games),
            "fantasyPoints": "true",
            "twoPointConversions": "2",
            "itemFormat": "list",
            **SCORING_RULES,
        }

        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": RAPIDAPI_HOST,
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()

                if data.get("statusCode") == 200:
                    body = data.get("body", [])
                    # API returns a list, get first item if exists
                    if isinstance(body, list) and len(body) > 0:
                        return body[0]
                    return body if body else None
                else:
                    logger.warning(f"API returned status: {data.get('statusCode')}")
                    return None

            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed (attempt {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)

        return None


def update_game_statuses(
    sheets_client: GoogleSheetsClient,
    api_key: str,
    schedule_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Fetch current game statuses from API and update the schedule sheet.

    Returns the updated schedule DataFrame.
    """
    if schedule_df.empty:
        return schedule_df

    # Get unique weeks that might have active games (not all final)
    weeks_to_check = set()
    for _, row in schedule_df.iterrows():
        status = str(row.get("gameStatus", "")).lower().strip()
        if status != "final":
            week = row.get("gameWeek", "")
            if week:
                weeks_to_check.add(week)

    if not weeks_to_check:
        logger.info("All games are final, no status updates needed")
        return schedule_df

    logger.info(f"Checking game statuses for weeks: {weeks_to_check}")

    # Fetch current game data for each week
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }

    # Map week names to API params
    week_params = {
        "Week 17": {"week": "17", "seasonType": "reg", "season": "2025"},
        "Week 18": {"week": "18", "seasonType": "reg", "season": "2025"},
        "Wildcard": {"week": "1", "seasonType": "post", "season": "2025"},
        "Divisional": {"week": "2", "seasonType": "post", "season": "2025"},
        "Conference": {"week": "3", "seasonType": "post", "season": "2025"},
        "Super Bowl": {"week": "4", "seasonType": "post", "season": "2025"},
    }

    # Build map of gameID -> new status
    status_updates = {}

    for week in weeks_to_check:
        params = week_params.get(week)
        if not params:
            logger.warning(f"No API params configured for week: {week}")
            continue

        url = f"https://{RAPIDAPI_HOST}/getNFLGamesForWeek"
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()

            if data.get("statusCode") == 200:
                games = data.get("body", [])
                for game in games:
                    game_id = game.get("gameID", "")
                    api_status = game.get("gameStatus", "Scheduled")

                    # Normalize status
                    if api_status == "Scheduled":
                        normalized_status = "scheduled"
                    elif api_status in ["In Progress", "In-Progress"]:
                        normalized_status = "in_progress"
                    elif api_status in ["Final", "Completed"]:
                        normalized_status = "final"
                    else:
                        normalized_status = api_status.lower()

                    status_updates[str(game_id)] = normalized_status

                logger.info(f"  {week}: fetched status for {len(games)} games")
        except Exception as e:
            logger.error(f"Failed to fetch game statuses for {week}: {e}")

    # Update the schedule DataFrame
    if status_updates:
        updated_rows = 0
        for idx, row in schedule_df.iterrows():
            game_id = str(row.get("gameID", ""))
            if game_id in status_updates:
                old_status = row.get("gameStatus", "")
                new_status = status_updates[game_id]
                if old_status != new_status:
                    schedule_df.at[idx, "gameStatus"] = new_status
                    updated_rows += 1
                    logger.info(f"  Game {game_id}: {old_status} -> {new_status}")

        if updated_rows > 0:
            # Write updated schedule back to sheet
            sheets_client.write_worksheet("schedule", schedule_df)
            logger.info(f"Updated {updated_rows} game statuses in schedule")
        else:
            logger.info("No game status changes detected")

    return schedule_df


def get_active_games(schedule_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Determine which games should have scores fetched.

    Returns games that are:
    - Currently in progress (gameStatus == 'in_progress')
    - Recently finished (gameStatus == 'final')
    - About to start or recently started (within game window)
    """
    if schedule_df.empty:
        return []

    active_games = []
    now = datetime.now()

    for _, row in schedule_df.iterrows():
        game_status = str(row.get("gameStatus", "")).lower().strip()
        game_id = str(row.get("gameID", ""))

        if game_status == "in_progress":
            active_games.append(row.to_dict())
        elif game_status == "final":
            # Include final games to ensure we have complete data
            active_games.append(row.to_dict())
        elif game_status == "scheduled":
            # Check if game is within the active window
            try:
                game_time_str = row.get("gameTime", "")
                if game_time_str:
                    game_time = pd.to_datetime(game_time_str)
                    # Include games that started within the last 4 hours
                    # or are about to start within 15 minutes
                    if (game_time - timedelta(minutes=15)) <= now <= (game_time + timedelta(hours=4)):
                        active_games.append(row.to_dict())
            except Exception as e:
                logger.debug(f"Could not parse game time for {game_id}: {e}")

    return active_games


def get_players_to_fetch(
    players_df: pd.DataFrame,
    picks_df: pd.DataFrame,
    active_games: List[Dict[str, Any]],
    scores_df: pd.DataFrame,
    week_override: Optional[str] = None
) -> List[Dict[str, str]]:
    """
    Get list of players who need scores fetched.

    Args:
        players_df: DataFrame of players with playerName, playerID, and team
        picks_df: DataFrame of picks with Week and position columns
        active_games: List of active game dicts from schedule
        scores_df: DataFrame of existing scores to check for completed games
        week_override: If provided, fetch players for this week regardless of schedule

    Returns list of dicts with playerID, playerName, gameWeek

    Note: Only fetches players whose team's game is in_progress or final.
    Skips players whose games haven't started yet.
    """
    if players_df.empty or picks_df.empty:
        return []

    if not active_games and not week_override:
        return []

    # Build map of weeks to game status (to check if games are final)
    week_game_status = {}
    for game in active_games:
        week = game.get("gameWeek", "")
        status = game.get("gameStatus", "").lower().strip()
        if week:
            if week not in week_game_status:
                week_game_status[week] = []
            week_game_status[week].append(status)

    # Build map of (week, team) -> game status to check if player's game has started
    team_game_status = {}
    for game in active_games:
        week = str(game.get("gameWeek", "")).strip()
        status = game.get("gameStatus", "").lower().strip()
        home_team = str(game.get("homeTeam", "")).strip().upper()
        away_team = str(game.get("awayTeam", "")).strip().upper()
        if week:
            if home_team:
                team_game_status[(week, home_team)] = status
            if away_team:
                team_game_status[(week, away_team)] = status

    logger.debug(f"Teams with active/final games: {list(team_game_status.keys())}")

    # Determine which weeks to fetch
    if week_override:
        # Use override week - fetch all players picked for this week
        active_weeks = {week_override}
        logger.info(f"Using week override: {week_override}")
    else:
        # Get active game weeks from schedule
        active_weeks = set()
        for game in active_games:
            week = game.get("gameWeek", "")
            if week:
                active_weeks.add(week)

    if not active_weeks:
        logger.info("No active game weeks found")
        return []

    logger.info(f"Active weeks: {active_weeks}")

    # Get all picked player names for active weeks, tracking which week they were picked for
    picked_players_by_week = {}  # week -> set of player names
    position_cols = ['QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE']

    for _, row in picks_df.iterrows():
        pick_week = str(row.get("Week", "")).strip()
        # Check if this week matches any active week (case-insensitive)
        matching_week = None
        for aw in active_weeks:
            if str(aw).strip().lower() == pick_week.lower():
                matching_week = str(aw).strip()
                break
        if matching_week:
            # Use the matching_week from game data for consistency with team_game_status
            if matching_week not in picked_players_by_week:
                picked_players_by_week[matching_week] = set()

            for col in position_cols:
                player = row.get(col)
                if player and pd.notna(player):
                    picked_players_by_week[matching_week].add(str(player).strip())

    total_unique_players = len(set().union(*picked_players_by_week.values())) if picked_players_by_week else 0
    logger.info(f"Found {total_unique_players} unique players picked across active weeks")

    # Build player lookup with playerID and team
    player_lookup = {}
    for _, row in players_df.iterrows():
        name = str(row.get("playerName", "")).strip()
        player_id = str(row.get("playerID", "")).strip()
        team = str(row.get("team", "")).strip().upper()
        if name and player_id and player_id != "nan":
            player_lookup[name] = {"playerID": player_id, "team": team}

    # Build a map of gameID -> status for quick lookup
    game_status_map = {}
    for game in active_games:
        game_id = game.get("gameID", "")
        if game_id:
            game_status_map[str(game_id)] = game.get("gameStatus", "").lower().strip()

    # Build list of players to fetch, only for weeks they were picked
    result = []
    skipped_final = 0
    skipped_not_started = 0

    for week, player_names in picked_players_by_week.items():
        for player_name in player_names:
            if player_name in player_lookup:
                player_info = player_lookup[player_name]
                player_id = player_info["playerID"]
                player_team = player_info["team"]

                # Check if the player's team has a game in active_games
                # (active_games includes in_progress, final, and games within the time window)
                lookup_key = (week, player_team)
                if lookup_key not in team_game_status:
                    # Player's team doesn't have an active game this week - skip
                    skipped_not_started += 1
                    continue

                # Check if we already have a FINAL score for this player+week
                # Only skip if we previously fetched the score when the game was final
                should_skip = False

                if not scores_df.empty:
                    existing_score = scores_df[
                        (scores_df['playerID'].astype(str) == str(player_id)) &
                        (scores_df['gameWeek'].astype(str) == str(week))
                    ]

                    if not existing_score.empty:
                        # Check if the existing score was captured when game was final
                        existing_game_status = str(existing_score.iloc[0].get('gameStatus', '')).lower().strip()

                        if existing_game_status == 'final':
                            # We already have the final score - skip this player
                            should_skip = True
                            skipped_final += 1

                if not should_skip:
                    result.append({
                        "playerID": player_id,
                        "playerName": player_name,
                        "gameWeek": week,
                    })
            else:
                logger.warning(f"No playerID found for: {player_name}")

    if skipped_not_started > 0:
        logger.info(f"Skipped {skipped_not_started} players whose games haven't started yet")
    if skipped_final > 0:
        logger.info(f"Skipped {skipped_final} players who already have final scores recorded")

    # Deduplicate by playerID + gameWeek combination
    seen = set()
    deduplicated = []
    for item in result:
        key = (item['playerID'], item['gameWeek'])
        if key not in seen:
            seen.add(key)
            deduplicated.append(item)
    
    if len(result) != len(deduplicated):
        logger.warning(f"Removed {len(result) - len(deduplicated)} duplicate player-week combinations")
    
    return deduplicated


def parse_stats_from_response(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Extract stats from API response into flat dict."""
    result = {
        "fantasyPoints": stats.get("fantasyPoints", "0"),
        "passYards": "0",
        "passTD": "0",
        "passInt": "0",
        "rushYards": "0",
        "rushTD": "0",
        "recYards": "0",
        "recTD": "0",
        "receptions": "0",
        "targets": "0",
        "fumbles": "0",
    }

    # Extract passing stats
    passing = stats.get("Passing", {})
    if passing:
        result["passYards"] = passing.get("passYds", "0")
        result["passTD"] = passing.get("passTD", "0")
        result["passInt"] = passing.get("int", "0")

    # Extract rushing stats
    rushing = stats.get("Rushing", {})
    if rushing:
        result["rushYards"] = rushing.get("rushYds", "0")
        result["rushTD"] = rushing.get("rushTD", "0")

    # Extract receiving stats
    receiving = stats.get("Receiving", {})
    if receiving:
        result["recYards"] = receiving.get("recYds", "0")
        result["recTD"] = receiving.get("recTD", "0")
        result["receptions"] = receiving.get("receptions", "0")
        result["targets"] = receiving.get("targets", "0")

    # Extract fumbles
    result["fumbles"] = stats.get("fumbles", "0")

    # Get gameID from response
    result["gameID"] = stats.get("gameID", "")

    return result


def update_scores(
    sheets_client: GoogleSheetsClient,
    api_client: RapidAPIClient,
    players_to_fetch: List[Dict[str, str]],
    active_games: List[Dict[str, Any]]
) -> int:
    """
    Fetch and update scores for the given players.

    Returns count of successfully updated players.
    """
    if not players_to_fetch:
        logger.info("No players to fetch scores for")
        return 0
    
    # Build map of week -> list of gameIDs to validate responses
    week_to_games = {}
    # Build map of gameID -> gameStatus to track if game is final
    game_id_to_status = {}
    for game in active_games:
        week = str(game.get("gameWeek", "")).strip()
        game_id = str(game.get("gameID", "")).strip()
        game_status = game.get("gameStatus", "").lower().strip()
        if week and game_id:
            if week not in week_to_games:
                week_to_games[week] = set()
            week_to_games[week].add(game_id)
            game_id_to_status[game_id] = game_status

    logger.debug(f"Week to games map: {week_to_games}")

    # Load existing scores
    scores_df = sheets_client.read_worksheet("scores")

    # Create scores dict for updates (keyed by playerID_gameWeek)
    existing_scores = {}
    if not scores_df.empty:
        for _, row in scores_df.iterrows():
            key = f"{row.get('playerID')}_{row.get('gameWeek')}"
            existing_scores[key] = row.to_dict()

    updated_count = 0

    for player_info in players_to_fetch:
        player_id = player_info["playerID"]
        player_name = player_info["playerName"]
        game_week = player_info["gameWeek"]

        logger.info(f"Fetching stats for {player_name} (ID: {player_id})")

        # Rate limiting
        time.sleep(API_CALL_DELAY_SECONDS)

        # Fetch from API
        stats = api_client.get_player_stats(player_id)

        if stats:
            parsed = parse_stats_from_response(stats)
            returned_game_id = str(parsed.get("gameID", "")).strip()
            
            # Validate that the returned game is from the correct week
            expected_game_ids = week_to_games.get(game_week, set())
            
            if not returned_game_id:
                logger.warning(f"  -> No gameID in API response for {player_name}")
                continue
            
            if returned_game_id not in expected_game_ids:
                logger.warning(
                    f"  -> Skipping {player_name}: API returned game {returned_game_id} "
                    f"which is not in {game_week}. Expected games: {expected_game_ids}"
                )
                continue

            # Get the current game status for this game
            current_game_status = game_id_to_status.get(returned_game_id, "unknown")

            score_record = {
                "playerID": player_id,
                "playerName": player_name,
                "gameID": returned_game_id,
                "gameWeek": game_week,
                "gameStatus": current_game_status,
                "fantasyPoints": parsed["fantasyPoints"],
                "passYards": parsed["passYards"],
                "passTD": parsed["passTD"],
                "passInt": parsed["passInt"],
                "rushYards": parsed["rushYards"],
                "rushTD": parsed["rushTD"],
                "recYards": parsed["recYards"],
                "recTD": parsed["recTD"],
                "receptions": parsed["receptions"],
                "targets": parsed["targets"],
                "fumbles": parsed["fumbles"],
                "lastUpdated": datetime.utcnow().isoformat() + "Z",
            }

            key = f"{player_id}_{game_week}"
            existing_scores[key] = score_record
            updated_count += 1

            logger.info(f"  -> {player_name}: {score_record['fantasyPoints']} pts (game {returned_game_id})")
        else:
            logger.warning(f"  -> No stats returned for {player_name}")

    # Convert back to DataFrame and write
    if existing_scores:
        # Define column order
        columns = [
            "playerID", "playerName", "gameID", "gameWeek", "gameStatus", "fantasyPoints",
            "passYards", "passTD", "passInt", "rushYards", "rushTD",
            "recYards", "recTD", "receptions", "targets", "fumbles", "lastUpdated"
        ]
        updated_df = pd.DataFrame(list(existing_scores.values()))
        # Reorder columns
        updated_df = updated_df[[c for c in columns if c in updated_df.columns]]
        sheets_client.write_worksheet("scores", updated_df)
        logger.info("Update - try environment first (for GitHub Actions), fall back to secrets.toml")
        if os.environ.get("RAPIDAPI_KEY"):
            logger.info("Loading configuration from environment variables")
            config = Config.from_environment()
        else:
            logger.info("Loading configuration from secrets.toml")
    
    return updated_count


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch NFL player scoring data from RapidAPI"
    )
    parser.add_argument(
        "--week",
        type=str,
        help="Override week to fetch (e.g., 'Wildcard', 'Week 18'). "
             "Bypasses schedule-based game detection."
    )
    return parser.parse_args()


def main():
    """Main entry point for the scoring job."""
    args = parse_args()

    logger.info("=" * 50)
    logger.info("Starting scoring job")

    if args.week:
        logger.info(f"Week override: {args.week}")

    try:
        # Load configuration
        config = Config.from_secrets_toml()

        if not config.rapidapi_key:
            logger.error("RapidAPI key not configured. Add [rapidapi] key = '...' to secrets.toml")
            sys.exit(1)

        if not config.spreadsheet_url:
            logger.error("Spreadsheet URL not configured")
            sys.exit(1)

        # Initialize clients
        sheets_client = GoogleSheetsClient(
            config.gcp_credentials,
            config.spreadsheet_url
        )
        api_client = RapidAPIClient(config.rapidapi_key)

        # Read worksheets
        schedule_df = sheets_client.read_worksheet("schedule")
        players_df = sheets_client.read_worksheet("players_2")
        picks_df = sheets_client.read_worksheet("Picks")
        scores_df = sheets_client.read_worksheet("scores")

        logger.info(f"Loaded {len(schedule_df)} scheduled games")
        logger.info(f"Loaded {len(players_df)} players")
        logger.info(f"Loaded {len(picks_df)} picks")
        logger.info(f"Loaded {len(scores_df)} existing scores")

        # Update game statuses from API before processing
        schedule_df = update_game_statuses(sheets_client, config.rapidapi_key, schedule_df)

        # Get active games (still useful for logging even with override)
        active_games = get_active_games(schedule_df)
        logger.info(f"Found {len(active_games)} active/relevant games")

        if not active_games and not args.week:
            logger.info("No active games to process")
            logger.info("Scoring job completed (no work to do)")
            return

        # Get players to fetch (with optional week override)
        players_to_fetch = get_players_to_fetch(
            players_df, picks_df, active_games, scores_df, week_override=args.week
        )
        logger.info(f"Will fetch scores for {len(players_to_fetch)} player-week combinations")

        if not players_to_fetch:
            logger.info("No players need scores fetched")
            logger.info("Scoring job completed (no work to do)")
            return

        # Update scores
        updated = update_scores(sheets_client, api_client, players_to_fetch, active_games)

        logger.info(f"Successfully updated {updated} player scores")
        logger.info("Scoring job completed")

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Scoring job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
