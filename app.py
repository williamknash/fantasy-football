import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from datetime import datetime
import pytz
from typing import List, Dict, Set, Tuple

# Page configuration
st.set_page_config(
    page_title="Fantasy Football Playoffs - One and Done",
    page_icon="🏈",
    layout="wide"
)

# Initialize connection to Google Sheets
@st.cache_resource
def init_gsheets():
    """Initialize Google Sheets connection"""
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        st.info("Please check your .streamlit/secrets.toml configuration")
        return None

# Sample player data - In production, you might load this from a separate Google Sheet
SAMPLE_PLAYERS = {
    "QB": [
        "Patrick Mahomes", "Josh Allen", "Lamar Jackson", "Jalen Hurts", 
        "Dak Prescott", "Tua Tagovailoa", "Brock Purdy", "C.J. Stroud",
        "Jared Goff", "Baker Mayfield", "Matthew Stafford", "Jordan Love"
    ],
    "RB": [
        "Christian McCaffrey", "Derrick Henry", "Josh Jacobs", "Saquon Barkley",
        "Alvin Kamara", "Aaron Jones", "Rachaad White", "Isiah Pacheco",
        "Kyren Williams", "James Cook", "De'Von Achane", "Breece Hall",
        "Travis Etienne", "Joe Mixon", "Tony Pollard", "Gus Edwards"
    ],
    "WR": [
        "Tyreek Hill", "CeeDee Lamb", "Amon-Ra St. Brown", "Mike Evans",
        "Stefon Diggs", "Davante Adams", "Keenan Allen", "Deebo Samuel",
        "Jaylen Waddle", "Terry McLaurin", "Amari Cooper", "DJ Moore",
        "Calvin Ridley", "Tyler Lockett", "Brandon Aiyuk", "Puka Nacua",
        "Rashee Rice", "Nico Collins", "Garrett Wilson", "Chris Olave"
    ],
    "TE": [
        "Travis Kelce", "Mark Andrews", "Trey McBride", "Sam LaPorta",
        "George Kittle", "Evan Engram", "David Njoku", "Dalton Kincaid",
        "Jake Ferguson", "Tyler Higbee", "Kyle Pitts", "Cole Kmet"
    ]
}

PLAYOFF_WEEKS = ["Wildcard", "Divisional", "Conference", "Super Bowl"]

# Game start times (5 minutes before actual game time) - Update these for each week
GAME_CUTOFF_TIMES = {
    "Wildcard": datetime(2026, 1, 10, 16, 25, tzinfo=pytz.timezone('US/Eastern')),  # 4:25 PM EST
    "Divisional": datetime(2026, 1, 17, 16, 25, tzinfo=pytz.timezone('US/Eastern')),  # Update as needed
    "Conference": datetime(2026, 1, 25, 16, 25, tzinfo=pytz.timezone('US/Eastern')),  # Update as needed
    "Super Bowl": datetime(2026, 2, 8, 5, 25, tzinfo=pytz.timezone('US/Eastern')),  # Update as needed
}

@st.cache_data(ttl=60)  # Cache for 60 seconds (players don't change often)
def get_all_players(_conn) -> Dict[str, List[str]]:
    """Get all available players by position from Google Sheet"""
    try:
        # Read from the "Players" worksheet
        players_df = _conn.read(worksheet="players_2", ttl=60)

        if players_df.empty:
            st.warning("Players worksheet is empty, using sample players")
            return SAMPLE_PLAYERS

        # Organize players by position
        players_by_position = {
            "QB": [],
            "RB": [],
            "WR": [],
            "TE": []
        }

        # New flattened format: playerName, playerID, position columns
        if "playerName" in players_df.columns and "position" in players_df.columns:
            for _, row in players_df.iterrows():
                position = str(row["position"]).strip().upper()
                player_name = str(row["playerName"]).strip()

                if position in players_by_position and player_name:
                    players_by_position[position].append(player_name)
        # Legacy format: Position column with Player Name
        elif "Position" in players_df.columns and "Player Name" in players_df.columns:
            for _, row in players_df.iterrows():
                position = str(row["Position"]).strip().upper()
                player_name = str(row["Player Name"]).strip()

                if position in players_by_position and player_name:
                    players_by_position[position].append(player_name)
        else:
            # Legacy format: column names as positions (QB, RB, WR, TE)
            for col in players_df.columns:
                col_upper = col.upper()
                if col_upper in ["QB", "RB", "WR", "TE"]:
                    players = players_df[col].dropna().tolist()
                    players_by_position[col_upper] = [str(p).strip() for p in players if p]

        # Filter out empty lists
        players_by_position = {k: v for k, v in players_by_position.items() if v}

        if not players_by_position:
            st.warning("No players found in Players worksheet, using sample players")
            return SAMPLE_PLAYERS

        return players_by_position
    except Exception as e:
        st.warning(f"Could not load players from sheet: {e}. Using sample players.")
        return SAMPLE_PLAYERS


@st.cache_data(ttl=60)
def get_player_id_map(_conn) -> Dict[str, str]:
    """Get mapping of player names to player IDs from Google Sheet"""
    try:
        players_df = _conn.read(worksheet="players_2", ttl=60)

        if players_df.empty:
            return {}

        # Only works with new flattened format
        if "playerName" not in players_df.columns or "playerID" not in players_df.columns:
            return {}

        player_id_map = {}
        for _, row in players_df.iterrows():
            player_name = str(row["playerName"]).strip()
            player_id = str(row["playerID"]).strip()
            if player_name and player_id:
                player_id_map[player_name] = player_id

        return player_id_map
    except Exception:
        return {}

@st.cache_data(ttl=30)  # Cache for 30 seconds
def load_users_from_sheet(_conn) -> pd.DataFrame:
    """Load all users and passwords from Google Sheet"""
    try:
        df = _conn.read(worksheet="Users", ttl=30)
        return df
    except Exception:
        # If Users worksheet doesn't exist, return empty DataFrame
        return pd.DataFrame()

@st.cache_data(ttl=30)  # Cache for 30 seconds
def load_picks_from_sheet(_conn) -> pd.DataFrame:
    """Load all picks from Google Sheet"""
    try:
        # Read from the "Picks" worksheet
        df = _conn.read(worksheet="Picks", usecols=list(range(10)), ttl=30)
        return df
    except Exception as e:
        st.warning(f"Could not load picks from sheet: {e}")
        return pd.DataFrame()


def get_used_players_for_user(df: pd.DataFrame, username: str) -> Set[str]:
    """Get set of all players already used by a specific user"""
    if df.empty:
        return set()
    
    # Filter picks for this user
    user_picks = df[df['User Name'] == username]
    
    # Collect all players from all positions
    used_players = set()
    position_columns = ['QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE']
    
    for col in position_columns:
        if col in user_picks.columns:
            players = user_picks[col].dropna().tolist()
            used_players.update([str(p).strip() for p in players if p])
    
    return used_players

def validate_lineup(qb: str, rb1: str, rb2: str, wr1: str, wr2: str, te: str, 
                   used_players: Set[str]) -> Tuple[bool, str]:
    """Validate the lineup before submission"""
    picks = {
        'QB': qb,
        'RB1': rb1,
        'RB2': rb2,
        'WR1': wr1,
        'WR2': wr2,
        'TE': te
    }
    
    # Check for empty picks
    for position, player in picks.items():
        if not player or player == "Select a player...":
            return False, f"Please select a {position}"
    
    # Check for duplicate players in current lineup
    all_picks = [picks['RB1'], picks['RB2'], picks['WR1'], picks['WR2']]
    if len(all_picks) != len(set(all_picks)):
        return False, "You cannot select the same player twice in one lineup"
    
    # Check if RB1 and RB2 are the same
    if rb1 == rb2:
        return False, "You cannot select the same running back twice"
    
    # Check if WR1 and WR2 are the same
    if wr1 == wr2:
        return False, "You cannot select the same wide receiver twice"
    
    # Check if any player has been used before
    all_current_picks = [qb, rb1, rb2, wr1, wr2, te]
    for player in all_current_picks:
        if player in used_players:
            return False, f"{player} has already been used in a previous week"
    
    return True, ""

def can_edit_lineup(week: str) -> Tuple[bool, str]:
    """Check if lineup can still be edited (5 minutes before game time)"""
    if week not in GAME_CUTOFF_TIMES:
        return True, ""  # Allow editing if week not configured
    
    cutoff_time = GAME_CUTOFF_TIMES[week]
    now = datetime.now(pytz.timezone('US/Eastern'))
    
    if now >= cutoff_time:
        return False, f"Lineup editing closed. Game starts at {cutoff_time.strftime('%I:%M %p %Z')}."
    return True, ""

def submit_lineup(conn, username: str, week: str, 
                 qb: str, rb1: str, rb2: str, wr1: str, wr2: str, te: str,
                 is_edit: bool = False):
    """Submit or update lineup in Google Sheet"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Check if editing is allowed
        can_edit, edit_message = can_edit_lineup(week)
        if not can_edit:
            return False, edit_message
        
        # Read existing data (no cache for writes to ensure we have latest data)
        existing_df = conn.read(worksheet="Picks", ttl=0)
        
        if is_edit:
            # Find and update existing row by User Name and Week
            mask = (existing_df['User Name'] == username) & (existing_df['Week'] == week)
            matching_rows = existing_df[mask]
            
            if matching_rows.empty:
                return False, "Could not find lineup to edit"
            
            # Update the first matching row
            row_idx = matching_rows.index[0]
            existing_df.loc[row_idx, 'QB'] = qb
            existing_df.loc[row_idx, 'RB1'] = rb1
            existing_df.loc[row_idx, 'RB2'] = rb2
            existing_df.loc[row_idx, 'WR1'] = wr1
            existing_df.loc[row_idx, 'WR2'] = wr2
            existing_df.loc[row_idx, 'TE'] = te
            existing_df.loc[row_idx, 'Timestamp'] = timestamp
            updated_df = existing_df
        else:
            # Create new row as a DataFrame
            new_row = pd.DataFrame([{
                'User Name': username,
                'Week': week,
                'QB': qb,
                'RB1': rb1,
                'RB2': rb2,
                'WR1': wr1,
                'WR2': wr2,
                'TE': te,
                'Timestamp': timestamp
            }])
            
            # Append new row to existing data
            if existing_df.empty:
                updated_df = new_row
            else:
                updated_df = pd.concat([existing_df, new_row], ignore_index=True)
        
        # Update the sheet with the combined data
        conn.update(worksheet="Picks", data=updated_df)
        
        # Clear cache to force refresh on next read
        load_picks_from_sheet.clear()
        
        action = "updated" if is_edit else "submitted"
        return True, f"Lineup {action} successfully!"
    except Exception as e:
        return False, f"Error submitting lineup: {str(e)}"

def authenticate_user(_conn, username: str, password: str) -> bool:
    """Authenticate user with password"""
    try:
        # Clear cache to get fresh data
        load_users_from_sheet.clear()
        users_df = load_users_from_sheet(_conn)
        
        if users_df.empty:
            # If no Users sheet exists, allow any login (backward compatibility)
            return True
        
        if "User Name" not in users_df.columns or "Password" not in users_df.columns:
            return True  # Allow if columns don't exist
        
        # Normalize username and password for comparison
        username_clean = str(username).strip()
        password_clean = str(password).strip()
        
        # Check if user exists and password matches
        # Normalize the User Name column for comparison
        users_df['User Name'] = users_df['User Name'].astype(str).str.strip()
        user_row = users_df[users_df['User Name'] == username_clean]
        
        if not user_row.empty:
            stored_password = str(user_row.iloc[0]['Password']).strip()
            # Debug: uncomment to see what's being compared (remove in production)
            # st.write(f"Debug: Comparing '{password_clean}' with stored '{stored_password}'")
            return stored_password == password_clean
        
        return False
    except Exception as e:
        st.error(f"Authentication error: {e}")
        return False

def user_exists(_conn, username: str) -> bool:
    """Check if a user already exists"""
    try:
        users_df = load_users_from_sheet(_conn)
        if users_df.empty or "User Name" not in users_df.columns:
            return False
        return username in users_df['User Name'].values
    except Exception:
        return False

def create_user(_conn, username: str, password: str) -> Tuple[bool, str]:
    """Create a new user in the Users worksheet"""
    try:
        # Check if user already exists
        if user_exists(_conn, username):
            return False, "Username already exists. Please choose a different username."
        
        # Try to read existing users
        try:
            users_df = load_users_from_sheet(_conn)
        except Exception:
            # If worksheet doesn't exist, create empty DataFrame with correct columns
            users_df = pd.DataFrame(columns=['User Name', 'Password'])
        
        # Create new user row
        new_user = pd.DataFrame([{
            'User Name': username,
            'Password': password
        }])
        
        # Append to existing data
        if users_df.empty or 'User Name' not in users_df.columns:
            # If empty or missing columns, create fresh DataFrame
            updated_df = new_user
        else:
            # Ensure Password column exists
            if "Password" not in users_df.columns:
                users_df['Password'] = ""
            updated_df = pd.concat([users_df, new_user], ignore_index=True)
        
        # Try to update the sheet (this will create it if it doesn't exist)
        try:
            _conn.update(worksheet="Users", data=updated_df)
        except Exception:
            # If update fails, try using create method
            try:
                _conn.create(worksheet="Users", data=updated_df)
            except Exception as create_error:
                # If both fail, return error
                return False, f"Could not create/update Users worksheet: {str(create_error)}"
        
        # Clear cache to force refresh
        load_users_from_sheet.clear()

        return True, "Account created successfully!"
    except Exception as e:
        return False, f"Error creating account: {str(e)}"


def main():
    st.title("🏈 Fantasy Football Playoffs - One and Done")
    st.markdown("---")
    
    # Initialize Google Sheets connection
    conn = init_gsheets()
    if conn is None:
        st.stop()
    
    # Initialize session state for authentication
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = ""
    
    # Login/Logout section
    if not st.session_state.authenticated:
        # Create tabs for Login and Create Account
        tab1, tab2 = st.tabs(["🔓 Login", "➕ Create Account"])
        
        with tab1:
            st.subheader("Login to Your Account")
            
            # Load users to get list of available usernames
            users_df = load_users_from_sheet(conn)
            picks_df = load_picks_from_sheet(conn)
            
            # Get usernames from Users sheet or Picks sheet
            if not users_df.empty and 'User Name' in users_df.columns:
                available_users = sorted(users_df['User Name'].dropna().unique().tolist())
            elif not picks_df.empty and 'User Name' in picks_df.columns:
                available_users = sorted(picks_df['User Name'].dropna().unique().tolist())
            else:
                available_users = []
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                if available_users:
                    username = st.selectbox("Select your username:", available_users, key="login_username")
                else:
                    username = st.text_input("Enter your username:", key="login_username")
            
            with col2:
                password = st.text_input("Enter your password:", type="password", key="login_password")
            
            login_button = st.button("🔓 Login", type="primary", use_container_width=True)
            
            if login_button:
                if not username:
                    st.error("Please enter a username")
                elif not password:
                    st.error("Please enter a password")
                else:
                    if authenticate_user(conn, username, password):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.success(f"Welcome, {username}!")
                        st.rerun()
                    else:
                        st.error("❌ Invalid username or password")
        
        with tab2:
            st.subheader("Create New Account")
            st.info("Create a new account to start playing. Choose a username and password.")
            
            # Check if Users worksheet exists
            try:
                test_df = load_users_from_sheet(conn)
                users_sheet_exists = True
            except Exception:
                users_sheet_exists = False
                st.warning("⚠️ **Important:** The 'Users' worksheet doesn't exist yet. Please create it in your Google Sheet:")
                st.code("""
1. Open your Google Sheet
2. Click the '+' button at the bottom to add a new worksheet
3. Name it exactly: "Users" (case-sensitive)
4. In row 1, add these headers:
   - Column A: "User Name"
   - Column B: "Password"
5. Then try creating an account again
                """)
            
            new_username = st.text_input("Choose a username:", key="new_username")
            new_password = st.text_input("Choose a password:", type="password", key="new_password")
            confirm_password = st.text_input("Confirm password:", type="password", key="confirm_password")
            
            create_button = st.button("➕ Create Account", type="primary", use_container_width=True)
            
            if create_button:
                if not new_username:
                    st.error("Please enter a username")
                elif not new_password:
                    st.error("Please enter a password")
                elif new_password != confirm_password:
                    st.error("❌ Passwords do not match")
                elif len(new_username.strip()) < 2:
                    st.error("Username must be at least 2 characters long")
                elif len(new_password.strip()) < 1:
                    st.error("Password cannot be empty")
                else:
                    success, message = create_user(conn, new_username.strip(), new_password.strip())
                    if success:
                        st.success(f"✅ {message}")
                        st.info("You can now login with your new account!")
                        # Auto-switch to login tab would be nice, but Streamlit doesn't support that
                        # So we'll just show a message
                    else:
                        st.error(f"❌ {message}")
        
        st.stop()
    
    # User is authenticated - show logout button
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.write(f"**Logged in as:** {st.session_state.username}")
    with col3:
        if st.button("🚪 Logout"):
            st.session_state.authenticated = False
            st.session_state.username = ""
            st.rerun()

    st.markdown("---")

    username = st.session_state.username

    # Load existing picks
    picks_df = load_picks_from_sheet(conn)

    # Week selection
    week = st.selectbox("Select Playoff Week:", PLAYOFF_WEEKS, key="week_select")

    # Check if editing is allowed for this week
    can_edit, edit_message = can_edit_lineup(week)
    if not can_edit:
        st.warning(f"⏰ {edit_message}")

    # Check if user has already submitted for this week
    existing_lineup = None
    existing_lineup_index = None
    if not picks_df.empty:
        user_week_picks = picks_df[
            (picks_df['User Name'] == username) &
            (picks_df['Week'] == week)
        ]
        if not user_week_picks.empty:
            existing_lineup = user_week_picks.iloc[0]
            existing_lineup_index = user_week_picks.index[0]
            st.info(f"📝 You have an existing lineup for {week} week. You can edit it below.")
            # Show existing lineup in an expander
            with st.expander("📋 View Current Lineup"):
                st.dataframe(user_week_picks[['QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE', 'Timestamp']],
                            use_container_width=True, hide_index=True)

    # Get used players for this user (excluding current week if editing)
    used_players = get_used_players_for_user(picks_df, username)

    # If editing, remove players from current lineup from used list
    if existing_lineup is not None:
        current_lineup_players = [
            existing_lineup.get('QB', ''),
            existing_lineup.get('RB1', ''),
            existing_lineup.get('RB2', ''),
            existing_lineup.get('WR1', ''),
            existing_lineup.get('WR2', ''),
            existing_lineup.get('TE', '')
        ]
        for player in current_lineup_players:
            if player in used_players:
                used_players.remove(player)

    # Display used players
    if used_players:
        with st.expander(f"📋 Players already used by {username}"):
            st.write(", ".join(sorted(used_players)))

    # Get available players from Google Sheet
    all_players = get_all_players(conn)

    st.markdown("---")
    st.subheader("Select Your Lineup")

    # Create columns for player selection
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### Quarterback (1 required)")
        # Filter available QBs (exclude used ones)
        available_qbs = [p for p in all_players.get("QB", []) if p not in used_players]
        if not available_qbs:
            st.error("No available quarterbacks! You've used them all.")
            qb = None
        else:
            # Pre-select existing QB if editing
            default_qb = existing_lineup.get('QB', '') if existing_lineup is not None else "Select a player..."
            default_index = 0
            if default_qb in available_qbs:
                default_index = available_qbs.index(default_qb) + 1

            qb = st.selectbox(
                "QB",
                ["Select a player..."] + available_qbs,
                index=default_index,
                key="qb_select"
            )

    with col2:
        st.markdown("### Running Backs (2 required)")
        # Filter available RBs
        available_rbs = [p for p in all_players.get("RB", []) if p not in used_players]
        if not available_rbs:
            st.error("No available running backs! You've used them all.")
            rb1 = None
            rb2 = None
        elif len(available_rbs) < 2:
            st.warning(f"Only {len(available_rbs)} running back(s) available!")
            default_rb1 = existing_lineup.get('RB1', '') if existing_lineup is not None else "Select a player..."
            default_index = 0
            if default_rb1 in available_rbs:
                default_index = available_rbs.index(default_rb1) + 1
            rb1 = st.selectbox(
                "RB 1",
                ["Select a player..."] + available_rbs,
                index=default_index,
                key="rb1_select"
            )
            rb2 = None
        else:
            default_rb1 = existing_lineup.get('RB1', '') if existing_lineup is not None else "Select a player..."
            default_index = 0
            if default_rb1 in available_rbs:
                default_index = available_rbs.index(default_rb1) + 1
            rb1 = st.selectbox(
                "RB 1",
                ["Select a player..."] + available_rbs,
                index=default_index,
                key="rb1_select"
            )
            # For RB2, exclude RB1 if selected
            rb2_options = [r for r in available_rbs if r != rb1 and r != "Select a player..."]
            default_rb2 = existing_lineup.get('RB2', '') if existing_lineup is not None else "Select a player..."
            default_index2 = 0
            if default_rb2 in rb2_options:
                default_index2 = rb2_options.index(default_rb2) + 1
            # Ensure index is valid
            if default_index2 >= len(["Select a player..."] + rb2_options):
                default_index2 = 0
            rb2 = st.selectbox(
                "RB 2",
                ["Select a player..."] + rb2_options,
                index=default_index2,
                key="rb2_select"
            )

    with col3:
        st.markdown("### Wide Receivers (2 required)")
        # Filter available WRs
        available_wrs = [p for p in all_players.get("WR", []) if p not in used_players]
        if not available_wrs:
            st.error("No available wide receivers! You've used them all.")
            wr1 = None
            wr2 = None
        elif len(available_wrs) < 2:
            st.warning(f"Only {len(available_wrs)} wide receiver(s) available!")
            default_wr1 = existing_lineup.get('WR1', '') if existing_lineup is not None else "Select a player..."
            default_index = 0
            if default_wr1 in available_wrs:
                default_index = available_wrs.index(default_wr1) + 1
            wr1 = st.selectbox(
                "WR 1",
                ["Select a player..."] + available_wrs,
                index=default_index,
                key="wr1_select"
            )
            wr2 = None
        else:
            default_wr1 = existing_lineup.get('WR1', '') if existing_lineup is not None else "Select a player..."
            default_index = 0
            if default_wr1 in available_wrs:
                default_index = available_wrs.index(default_wr1) + 1
            wr1 = st.selectbox(
                "WR 1",
                ["Select a player..."] + available_wrs,
                index=default_index,
                key="wr1_select"
            )
            # For WR2, exclude WR1 if selected
            wr2_options = [w for w in available_wrs if w != wr1 and w != "Select a player..."]
            default_wr2 = existing_lineup.get('WR2', '') if existing_lineup is not None else "Select a player..."
            default_index2 = 0
            if default_wr2 in wr2_options:
                default_index2 = wr2_options.index(default_wr2) + 1
            # Ensure index is valid
            if default_index2 >= len(["Select a player..."] + wr2_options):
                default_index2 = 0
            wr2 = st.selectbox(
                "WR 2",
                ["Select a player..."] + wr2_options,
                index=default_index2,
                key="wr2_select"
            )

    st.markdown("### Tight End (1 required)")
    # Filter available TEs
    available_tes = [p for p in all_players.get("TE", []) if p not in used_players]
    if not available_tes:
        st.error("No available tight ends! You've used them all.")
        te = None
    else:
        default_te = existing_lineup.get('TE', '') if existing_lineup is not None else "Select a player..."
        default_index = 0
        if default_te in available_tes:
            default_index = available_tes.index(default_te) + 1
        te = st.selectbox(
            "TE",
            ["Select a player..."] + available_tes,
            index=default_index,
            key="te_select"
        )

    st.markdown("---")

    # Submit/Update button
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        button_text = "✏️ Update Lineup" if existing_lineup is not None else "🚀 Submit Lineup"
        submit_button = st.button(button_text, type="primary", use_container_width=True, disabled=not can_edit)

    if submit_button:
        # Check if editing is still allowed
        can_edit_now, edit_msg = can_edit_lineup(week)
        if not can_edit_now:
            st.error(f"❌ {edit_msg}")
        else:
            # Validate lineup
            # Recalculate used players excluding current lineup if editing
            validation_used = get_used_players_for_user(picks_df, username)
            if existing_lineup is not None:
                current_lineup_players = [
                    existing_lineup.get('QB', ''),
                    existing_lineup.get('RB1', ''),
                    existing_lineup.get('RB2', ''),
                    existing_lineup.get('WR1', ''),
                    existing_lineup.get('WR2', ''),
                    existing_lineup.get('TE', '')
                ]
                for player in current_lineup_players:
                    if player in validation_used:
                        validation_used.remove(player)

            is_valid, error_message = validate_lineup(
                qb, rb1, rb2, wr1, wr2, te, validation_used
            )

            if not is_valid:
                st.error(f"❌ {error_message}")
            else:
                # Double-check used players one more time (in case sheet was updated)
                current_used = get_used_players_for_user(picks_df, username)
                all_picks = [qb, rb1, rb2, wr1, wr2, te]

                # Remove current lineup players from used list for validation
                if existing_lineup is not None:
                    current_lineup_players = [
                        existing_lineup.get('QB', ''),
                        existing_lineup.get('RB1', ''),
                        existing_lineup.get('RB2', ''),
                        existing_lineup.get('WR1', ''),
                        existing_lineup.get('WR2', ''),
                        existing_lineup.get('TE', '')
                    ]
                    for player in current_lineup_players:
                        if player in current_used:
                            current_used.remove(player)

                # Check again if any player was used
                conflict_players = [p for p in all_picks if p in current_used]
                if conflict_players:
                    st.error(f"❌ The following players have already been used: {', '.join(conflict_players)}")
                    st.info("Please refresh the page and try again.")
                else:
                    # Submit or update to Google Sheet
                    is_edit = existing_lineup is not None
                    success, message = submit_lineup(
                        conn, username, week, qb, rb1, rb2, wr1, wr2, te,
                        is_edit=is_edit
                    )

                    if success:
                        st.success(f"✅ {message}")
                        st.balloons()
                        st.info("🔄 Refreshing in 3 seconds...")
                        st.rerun()
                    else:
                        st.error(f"❌ {message}")

    # Display user's previous picks
    if not picks_df.empty and username:
        st.markdown("---")
        st.subheader(f"📊 {username}'s Previous Picks")
        user_picks = picks_df[picks_df['User Name'] == username]
        if not user_picks.empty:
            display_cols = ['Week', 'QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE', 'Timestamp']
            available_cols = [col for col in display_cols if col in user_picks.columns]
            st.dataframe(
                user_picks[available_cols].sort_values('Week', ascending=False),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No previous picks found for this user.")


if __name__ == "__main__":
    main()

