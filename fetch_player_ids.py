#!/usr/bin/env python3
"""
One-time script to fetch NFL player IDs from RapidAPI and match them
to existing player names in the Google Sheet.

Usage:
    python fetch_player_ids.py

Outputs:
    player_ids.csv - CSV with playerName, playerID, position for matched players
"""

import csv
import sys
from pathlib import Path
from typing import Dict, Any

import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# RapidAPI configuration
RAPIDAPI_HOST = "tank01-nfl-live-in-game-real-time-statistics-nfl.p.rapidapi.com"


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


def get_players_from_sheet(spreadsheet_url: str, credentials: Dict[str, Any]) -> pd.DataFrame:
    """Read existing players from Google Sheet."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(credentials, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(spreadsheet_url)

    try:
        worksheet = spreadsheet.worksheet("players_2")
        data = worksheet.get_all_records()
        return pd.DataFrame(data)
    except gspread.WorksheetNotFound:
        print("Worksheet 'players_2' not found")
        return pd.DataFrame()


def fetch_nfl_player_list(api_key: str) -> list:
    """Fetch full NFL player list from RapidAPI."""
    url = f"https://{RAPIDAPI_HOST}/getNFLPlayerList"

    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": RAPIDAPI_HOST,
    }

    print("Fetching NFL player list from API...")
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    data = response.json()

    if data.get("statusCode") == 200:
        body = data.get("body", [])
        print(f"Fetched {len(body)} players from API")
        return body
    else:
        print(f"API error: {data.get('statusCode')}")
        return []


def normalize_name(name: str) -> str:
    """Normalize player name for matching."""
    if not name:
        return ""
    # Remove periods, convert to lowercase, strip whitespace
    normalized = name.lower().replace(".", "").replace("'", "").strip()
    # Handle common name variations
    normalized = normalized.replace("jr", "").replace("sr", "").replace("iii", "").replace("ii", "")
    return " ".join(normalized.split())  # Normalize whitespace


def match_players(sheet_players: pd.DataFrame, api_players: list) -> list:
    """Match sheet players to API players by name."""
    # Build lookup dict from API players
    # espnName is the player name, espnID is what we want
    api_lookup = {}
    api_lookup_normalized = {}

    for player in api_players:
        espn_name = player.get("espnName", "")
        espn_id = player.get("espnID", "")
        pos = player.get("pos", "")

        if espn_name and espn_id:
            api_lookup[espn_name] = {"id": espn_id, "pos": pos}
            api_lookup_normalized[normalize_name(espn_name)] = {
                "id": espn_id,
                "pos": pos,
                "original_name": espn_name
            }

    # Match players from sheet
    matched = []
    unmatched = []

    # Get player names from sheet
    if "playerName" in sheet_players.columns:
        player_names = sheet_players["playerName"].dropna().tolist()
        positions = sheet_players.get("position", pd.Series()).tolist()
    else:
        print("No 'playerName' column found in sheet")
        return []

    for idx, player_name in enumerate(player_names):
        player_name = str(player_name).strip()
        if not player_name:
            continue

        sheet_position = positions[idx] if idx < len(positions) else ""

        # Try exact match first
        if player_name in api_lookup:
            info = api_lookup[player_name]
            matched.append({
                "playerName": player_name,
                "playerID": info["id"],
                "position": sheet_position or info["pos"],
            })
            continue

        # Try normalized match
        normalized = normalize_name(player_name)
        if normalized in api_lookup_normalized:
            info = api_lookup_normalized[normalized]
            matched.append({
                "playerName": player_name,
                "playerID": info["id"],
                "position": sheet_position or info["pos"],
            })
            continue

        # No match found
        unmatched.append(player_name)

    return matched, unmatched


def main():
    print("=" * 50)
    print("Player ID Fetcher")
    print("=" * 50)

    try:
        # Load config
        api_key, spreadsheet_url, gcp_credentials = load_config()

        if not api_key:
            print("ERROR: RapidAPI key not found in secrets.toml")
            print("Add [rapidapi] key = 'your-key' to .streamlit/secrets.toml")
            sys.exit(1)

        # Get players from sheet
        print("\nReading players from Google Sheet...")
        sheet_players = get_players_from_sheet(spreadsheet_url, gcp_credentials)

        if sheet_players.empty:
            print("No players found in sheet")
            sys.exit(1)

        print(f"Found {len(sheet_players)} players in sheet")

        # Fetch API player list
        api_players = fetch_nfl_player_list(api_key)

        if not api_players:
            print("No players returned from API")
            sys.exit(1)

        # Match players
        print("\nMatching players...")
        matched, unmatched = match_players(sheet_players, api_players)

        print(f"\nMatched: {len(matched)} players")
        print(f"Unmatched: {len(unmatched)} players")

        if unmatched:
            print("\nUnmatched players (manual lookup needed):")
            for name in unmatched:
                print(f"  - {name}")

        # Write CSV
        output_file = "player_ids.csv"
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["playerName", "playerID", "position"])
            writer.writeheader()
            writer.writerows(matched)

        print(f"\nOutput written to: {output_file}")

        # Also print matched players
        print("\nMatched players:")
        for player in matched:
            print(f"  {player['playerName']}: {player['playerID']} ({player['position']})")

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
