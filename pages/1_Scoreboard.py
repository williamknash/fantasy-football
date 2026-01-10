import streamlit as st
from streamlit_gsheets import GSheetsConnection
import pandas as pd
from typing import Dict, List
from datetime import datetime
import pytz

st.set_page_config(
    page_title="Scoreboard - Fantasy Football Playoffs",
    page_icon="📊",
    layout="wide"
)

PLAYOFF_WEEKS = ["Wildcard", "Divisional", "Conference", "Super Bowl"]

# Game start times (Eastern Time)
GAME_START_TIMES = {
    "Wildcard": datetime(2026, 1, 10, 16, 30, tzinfo=pytz.timezone('US/Eastern')),
    # Other weeks TBD - will remain hidden until scheduled
}


def games_have_started(week: str) -> bool:
    """Check if games have started for a given week"""
    if week not in GAME_START_TIMES:
        return False  # Hide if no start time defined
    
    now = datetime.now(pytz.timezone('US/Eastern'))
    return now >= GAME_START_TIMES[week]


@st.cache_resource
def init_gsheets():
    """Initialize Google Sheets connection"""
    try:
        conn = st.connection("gsheets", type=GSheetsConnection)
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Google Sheets: {e}")
        return None


@st.cache_data(ttl=30)
def load_picks_from_sheet(_conn) -> pd.DataFrame:
    """Load all picks from Google Sheet"""
    try:
        df = _conn.read(worksheet="Picks", usecols=list(range(10)), ttl=30)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_scores_from_sheet(_conn) -> pd.DataFrame:
    """Load all scores from Google Sheet"""
    try:
        df = _conn.read(worksheet="scores", ttl=30)
        return df
    except Exception:
        return pd.DataFrame()


def get_player_score(scores_df: pd.DataFrame, player_name: str, week: str) -> float:
    """Get fantasy points for a specific player in a specific week"""
    if scores_df.empty or not player_name:
        return 0.0

    player_scores = scores_df[
        (scores_df['playerName'] == player_name) &
        (scores_df['gameWeek'] == week)
    ]

    if player_scores.empty:
        return 0.0

    try:
        return float(player_scores.iloc[0].get('fantasyPoints', 0))
    except (ValueError, TypeError):
        return 0.0


def get_user_week_scores(picks_df: pd.DataFrame, scores_df: pd.DataFrame,
                         username: str, week: str) -> Dict[str, float]:
    """Get all player scores for a user's lineup in a specific week"""
    if picks_df.empty:
        return {}

    user_picks = picks_df[
        (picks_df['User Name'] == username) &
        (picks_df['Week'] == week)
    ]

    if user_picks.empty:
        return {}

    pick_row = user_picks.iloc[0]
    position_cols = ['QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE']

    scores = {}
    for col in position_cols:
        player = pick_row.get(col, '')
        if player and pd.notna(player):
            scores[col] = {
                'player': str(player),
                'points': get_player_score(scores_df, str(player), week)
            }
        else:
            scores[col] = {'player': '', 'points': 0.0}

    return scores


def get_user_total_points(picks_df: pd.DataFrame, scores_df: pd.DataFrame,
                          username: str, weeks: List[str]) -> Dict[str, float]:
    """Get total points for a user across specified weeks"""
    week_totals = {}
    grand_total = 0.0

    for week in weeks:
        week_scores = get_user_week_scores(picks_df, scores_df, username, week)
        week_total = sum(p['points'] for p in week_scores.values())
        week_totals[week] = week_total
        grand_total += week_total

    return {'weeks': week_totals, 'total': grand_total}


def render_lineup_details(week_scores: Dict, show_players: bool = True) -> None:
    """Render the lineup player rows"""
    for pos in ['QB', 'RB1', 'RB2', 'WR1', 'WR2', 'TE']:
        player_data = week_scores.get(pos, {'player': '', 'points': 0.0})
        player_name = player_data.get('player', '') or '-'

        # Hide player names if games haven't started
        if not show_players and player_name != '-':
            player_name = '🔒 Hidden'

        points = player_data.get('points', 0.0)
        points_display = f"{points:.1f}" if points > 0 else "-"

        cols = st.columns([1, 3, 1])
        with cols[0]:
            st.caption(pos)
        with cols[1]:
            st.write(player_name)
        with cols[2]:
            st.write(f"**{points_display}**")


def render_baseball_card(username: str, week_scores: Dict, week_total: float,
                         running_total: float, rank: int, selected_week: str,
                         show_players: bool = True) -> None:
    """Render a baseball card style view for a user's lineup (expanded)"""

    with st.container(border=True):
        # Header row with name and season total
        header_cols = st.columns([1, 1, 1])
        with header_cols[0]:
            st.markdown(f"**#{rank} {username}**")
        with header_cols[1]:
            st.metric(f"{selected_week}", f"{week_total:.1f}")
        with header_cols[2]:
            st.metric("Season", f"{running_total:.1f}")

        st.divider()

        render_lineup_details(week_scores, show_players)


def render_collapsible_card(username: str, week_scores: Dict, week_total: float,
                            running_total: float, rank: int, selected_week: str,
                            show_players: bool = True) -> None:
    """Render a collapsible card showing summary, with lineup revealed on click"""

    with st.container(border=True):
        # Summary header always visible
        header_cols = st.columns([2, 1, 1])
        with header_cols[0]:
            st.markdown(f"**#{rank} {username}**")
        with header_cols[1]:
            st.metric(f"{selected_week}", f"{week_total:.1f}")
        with header_cols[2]:
            st.metric("Season", f"{running_total:.1f}")

        # Collapsible lineup details
        with st.expander("View Lineup"):
            render_lineup_details(week_scores, show_players)



def render_scoreboard(picks_df: pd.DataFrame, scores_df: pd.DataFrame,
                      selected_week: str) -> None:
    """Render the full scoreboard view"""
    if picks_df.empty:
        st.info("No picks have been submitted yet.")
        return

    # Check if player names should be shown
    show_players = games_have_started(selected_week)

    all_users = picks_df['User Name'].dropna().unique().tolist()

    if not all_users:
        st.info("No users found with picks.")
        return

    # Calculate running totals for all users
    user_totals = []
    for user in all_users:
        totals = get_user_total_points(picks_df, scores_df, user, PLAYOFF_WEEKS)
        user_totals.append({
            'username': user,
            'running_total': totals['total'],
            'week_totals': totals['weeks']
        })

    # Sort by running total (descending)
    user_totals.sort(key=lambda x: x['running_total'], reverse=True)

    # Show lock notice if games haven't started
    if not show_players:
        st.info("🔒 Player lineups will be revealed when games start.")

    # Get current user if authenticated
    current_user = st.session_state.get('username', '')

    # Find current user's data and rank
    current_user_data = None
    current_user_rank = None

    for i, user_data in enumerate(user_totals):
        if user_data['username'] == current_user:
            current_user_data = user_data
            current_user_rank = i + 1
            break

    # Render current user's section if authenticated and has picks
    if current_user and current_user_data:
        st.markdown(f"### Your Lineup - {selected_week}")

        week_scores = get_user_week_scores(picks_df, scores_df, current_user, selected_week)
        week_total = sum(p['points'] for p in week_scores.values())

        # Always show current user's own lineup expanded
        render_baseball_card(
            username=current_user,
            week_scores=week_scores,
            week_total=week_total,
            running_total=current_user_data['running_total'],
            rank=current_user_rank,
            selected_week=selected_week,
            show_players=True  # User can always see their own lineup
        )

        st.markdown("---")

    # Render all users in rankings section (including current user)
    st.markdown("### Rankings")

    cols_per_row = 3
    for i in range(0, len(user_totals), cols_per_row):
        cols = st.columns(cols_per_row)
        for j, col in enumerate(cols):
            if i + j < len(user_totals):
                user_data = user_totals[i + j]
                username = user_data['username']
                rank = i + j + 1

                week_scores = get_user_week_scores(picks_df, scores_df, username, selected_week)
                week_total = sum(p['points'] for p in week_scores.values())

                # Current user can always see their own lineup
                user_show_players = True if username == current_user else show_players

                with col:
                    render_collapsible_card(
                        username=username,
                        week_scores=week_scores,
                        week_total=week_total,
                        running_total=user_data['running_total'],
                        rank=rank,
                        selected_week=selected_week,
                        show_players=user_show_players
                    )


def main():
    st.title("📊 Weekly Scoreboard")
    st.markdown("---")

    # Initialize session state for authentication (shared with main app)
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    if 'username' not in st.session_state:
        st.session_state.username = ""

    # Show login status
    if st.session_state.authenticated and st.session_state.username:
        st.caption(f"Logged in as: **{st.session_state.username}**")
    else:
        st.caption("Not logged in - [Go to Home to login](./)")

    conn = init_gsheets()
    if conn is None:
        st.stop()

    # Load data
    picks_df = load_picks_from_sheet(conn)
    scores_df = load_scores_from_sheet(conn)

    # Week selector
    selected_week = st.selectbox(
        "Select Week to View:",
        PLAYOFF_WEEKS,
        key="scoreboard_week_select"
    )

    st.markdown("---")

    render_scoreboard(picks_df, scores_df, selected_week)


if __name__ == "__main__":
    main()
