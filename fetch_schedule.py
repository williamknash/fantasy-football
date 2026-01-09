#!/usr/bin/env python3
"""
One-time script to fetch NFL schedule from RapidAPI and populate
the schedule worksheet in Google Sheets.

Usage:
    python fetch_schedule.py

Fetches:
    - Week 18 regular season 2025
    - Wild Card week postseason 2025
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# RapidAPI configuration
RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"

# Weeks to fetch
WEEKS_TO_FETCH = [
        {"week": "17", "seasonType": "reg", "season": "2025"},
    {"week": "18", "seasonType": "reg", "season": "2025"},
    {"week": "1", "seasonType": "post", "season": "2025"},  # Wild Card
    {"week": "2", "seasonType": "post", "season": "2025"},  # Divisional
    {"week": "3", "seasonType": "post", "season": "2025"},  # Conference
    {"week": "4", "seasonType": "post", "season": "2025"},  # Super Bowl
]


def load_config():
    """Load configuration from secrets.toml."""
    import tomli

    secrets_path = Path(".streamlit/secrets.toml")
    if not secrets_path.exists():
        raise FileNotFoundError("secrets.toml not found")

    with open(secrets_path, "rb") as f:
        secrets = tomli.load(f)

    rapidapi_key = secrets.get("rapidapi", {}).get("key", "")
    gsheets = secrets.get("connections", {}).get("gsheets", {})
    spreadsheet_url = gsheets.get("spreadsheet", "")

    gcp_credentials = {
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

    return rapidapi_key, spreadsheet_url, gcp_credentials


def fetch_week_schedule(api_key: str, week: str, season_type: str, season: str) -> List[Dict]:
    """Fetch schedule for a specific week from RapidAPI."""
    url = f"https://{RAPIDAPI_HOST}/getNFLGamesForWeek"

    params = {
        "week": week,
        "seasonType": season_type,
        "season": season,
    }

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }

    print(f"Fetching week {week} ({season_type}) {season}...")
    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if data.get("statusCode") == 200:
        body = data.get("body", [])
        print(f"  Found {len(body)} games")
        return body
    else:
        print(f"  API error: {data.get('statusCode')}")
        return []


def parse_game_time(game_date: str, game_time: str, epoch: str) -> str:
    """Convert game date/time to ISO format."""
    try:
        # Try using epoch if available
        if epoch and epoch != "None":
            epoch_float = float(epoch)
            dt = datetime.fromtimestamp(epoch_float)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        pass

    # Fallback: combine gameDate and gameTime
    try:
        # gameDate format: YYYYMMDD
        # gameTime format: "8:00p" or "4:30p"
        if game_date and game_time:
            time_str = game_time.replace("p", " PM").replace("a", " AM")
            combined = f"{game_date} {time_str}"
            dt = datetime.strptime(combined, "%Y%m%d %I:%M %p")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
    except (ValueError, TypeError):
        pass

    return game_time  # Return original if parsing fails


def transform_games(games: List[Dict]) -> List[Dict]:
    """Transform API response to our schedule format."""
    schedule_rows = []

    for game in games:
        game_time_iso = parse_game_time(
            game.get("gameDate", ""),
            game.get("gameTime", ""),
            game.get("gameTime_epoch", "")
        )

        # Map gameStatus
        status = game.get("gameStatus", "Scheduled")
        if status == "Scheduled":
            status = "scheduled"
        elif status in ["In Progress", "In-Progress"]:
            status = "in_progress"
        elif status in ["Final", "Completed"]:
            status = "final"

        schedule_rows.append({
            "gameID": game.get("gameID", ""),
            "gameWeek": game.get("gameWeek", ""),
            "gameTime": game_time_iso,
            "homeTeam": game.get("home", ""),
            "awayTeam": game.get("away", ""),
            "gameStatus": status,
        })

    return schedule_rows


def write_to_sheet(spreadsheet_url: str, credentials: Dict[str, Any], schedule_data: List[Dict]):
    """Write schedule data to Google Sheet."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(credentials, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(spreadsheet_url)

    # Get or create worksheet
    try:
        worksheet = spreadsheet.worksheet("schedule")
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title="schedule", rows=100, cols=10)
        print("Created 'schedule' worksheet")

    # Convert to DataFrame
    df = pd.DataFrame(schedule_data)

    # Ensure column order
    columns = ["gameID", "gameWeek", "gameTime", "homeTeam", "awayTeam", "gameStatus"]
    df = df[[c for c in columns if c in df.columns]]

    # Clear and write
    worksheet.clear()
    data = [df.columns.tolist()] + df.values.tolist()
    worksheet.update('A1', data)

    print(f"\nWrote {len(df)} games to 'schedule' worksheet")


def main():
    print("=" * 50)
    print("Schedule Fetcher")
    print("=" * 50)

    try:
        # Load config
        api_key, spreadsheet_url, gcp_credentials = load_config()

        if not api_key:
            print("ERROR: RapidAPI key not found in secrets.toml")
            print("Add [rapidapi] key = 'your-key' to .streamlit/secrets.toml")
            sys.exit(1)

        # Fetch all weeks
        all_games = []

        for week_config in WEEKS_TO_FETCH:
            games = fetch_week_schedule(
                api_key,
                week_config["week"],
                week_config["seasonType"],
                week_config["season"]
            )
            all_games.extend(games)

        if not all_games:
            print("\nNo games found")
            sys.exit(1)

        print(f"\nTotal games fetched: {len(all_games)}")

        # Transform to our format
        schedule_data = transform_games(all_games)

        # Print preview
        print("\nSchedule preview:")
        print("-" * 80)
        for game in schedule_data[:10]:
            print(f"  {game['gameID']} | {game['gameWeek']} | {game['gameTime']} | {game['gameStatus']}")
        if len(schedule_data) > 10:
            print(f"  ... and {len(schedule_data) - 10} more games")

        # Write to Google Sheet
        print("\nWriting to Google Sheet...")
        write_to_sheet(spreadsheet_url, gcp_credentials, schedule_data)

        print("\nDone!")

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
