# =========================
# Imports and Configuration
# =========================
import logging
import os
import json
import random
import html
import traceback
from typing import Final
import uuid
import telegram
from telegram import Update, User, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackContext, CallbackQueryHandler, ConversationHandler
from telegram.constants import ChatMemberStatus
from functools import wraps
import time
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# =========================
# Logging Configuration
# =========================
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
# Suppress noisy library logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Debug: Print all environment variables at startup
logger.debug(f"Environment variables: {os.environ}")

# Load the Telegram bot token from environment variable
load_dotenv()
TOKEN = os.environ.get('TELEGRAM_TOKEN')
# IMPORTANT: UPDATE THESE VALUES FOR YOUR BOT
BOT_USERNAME: Final = '@YourBotUsername'  # <--- CHANGE THIS to your bot's username
OWNER_ID = 123456789  # <--- CHANGE THIS to your own Telegram User ID
# END IMPORTANT

# File paths for persistent data storage
ADMIN_DATA_FILE = 'admins.json'          # Stores admin/owner info
TOD_DATA_FILE = 'truth_or_dare.json'     # Stores truths and dares per group
ACTIVE_TOD_GAMES_FILE = 'active_tod_games.json' # Stores active truth or dare games


# =========================
# Decorators
# =========================
def command_handler_wrapper(admin_only=False):
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            # Do not process if the message is not from a user
            if not update.effective_user or not update.message:
                return

            user = update.effective_user
            cache_user_profile(user) # Cache the user who triggered the command
            chat = update.effective_chat
            message_id = update.message.message_id

            # Defer message deletion to the end
            should_delete = True

            try:
                # Check if the command is disabled
                if chat.type in ['group', 'supergroup']:
                    command_name = func.__name__.replace('_command', '')
                    disabled_cmds = set(load_disabled_commands().get(str(chat.id), []))
                    if command_name in disabled_cmds:
                        logger.info(f"Command '{command_name}' is disabled in group {chat.id}. Aborting.")
                        return # Silently abort if command is disabled

                if admin_only and chat.type in ['group', 'supergroup']:
                    member = await context.bot.get_chat_member(chat.id, user.id)
                    if member.status not in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
                        await update.message.reply_text(
                            f"Warning: {user.mention_html()}, you are not authorized to use this command.",
                            parse_mode='HTML'
                        )
                        # Still delete their command attempt
                        return

                # Execute the actual command function
                await func(update, context, *args, **kwargs)

            finally:
                # Delete the command message
                if should_delete and chat.type in ['group', 'supergroup']:
                    try:
                        await context.bot.delete_message(chat.id, message_id)
                    except Exception:
                        logger.warning(f"Failed to delete command message {message_id} in chat {chat.id}. Bot may not have delete permissions.")

        return wrapper
    return decorator


# =============================
# Admin/Owner Data Management
# =============================
USER_TITLES_FILE = 'user_titles.json'

def load_user_titles():
    if os.path.exists(USER_TITLES_FILE):
        with open(USER_TITLES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_user_titles(data):
    with open(USER_TITLES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

@command_handler_wrapper(admin_only=True)
async def title_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /title <user> <title> - Sets a title for a user. Can be used as a reply. """
    target_user = None
    title = ""

    # Case 1: Command is a reply to a message
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        if not context.args:
            await update.message.reply_text("Usage (as reply): /title <the title>")
            return
        title = " ".join(context.args)

    # Case 2: Command is not a reply, used with arguments
    else:
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /title <@username or user_id> <title>\nOr, reply to a user's message with: /title <the title>")
            return

        target_identifier = context.args[0]
        title = " ".join(context.args[1:])

        target_id = None
        if target_identifier.isdigit():
            target_id = int(target_identifier)
        else:
            target_id = await get_user_id_by_username(context, update.effective_chat.id, target_identifier)

        if not target_id:
            await update.message.reply_text(f"Could not find user {target_identifier}.")
            return

        # We have an ID, but we need the user object for the display name later
        try:
            target_user = (await context.bot.get_chat_member(update.effective_chat.id, target_id)).user
        except Exception:
            await update.message.reply_text(f"Could not retrieve user information for ID {target_id}.")
            return

    if not target_user:
        await update.message.reply_text("Could not determine target user.")
        return

    # Create the mention string BEFORE setting the title, so it uses the original name.
    original_user_mention = f'<a href="tg://user?id={target_user.id}">{html.escape(target_user.full_name)}</a>'

    titles = load_user_titles()
    group_id_str = str(update.effective_chat.id)

    # Ensure the group key exists
    if group_id_str not in titles:
        titles[group_id_str] = {}

    titles[group_id_str][str(target_user.id)] = title
    save_user_titles(titles)

    # Now, use the original mention in the confirmation message.
    await update.message.reply_text(f"Title for {original_user_mention} has been set to '{html.escape(title)}'.", parse_mode='HTML')

@command_handler_wrapper(admin_only=True)
async def removetitle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /removetitle <user> - Removes a title from a user. Can be used as a reply. """
    target_user = None

    # Case 1: Command is a reply to a message
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
        if context.args:
            await update.message.reply_text("Usage (as reply): /removetitle (no arguments needed).")
            return

    # Case 2: Command is not a reply, used with an argument
    else:
        if len(context.args) != 1:
            await update.message.reply_text("Usage: /removetitle <@username or user_id>\nOr, reply to a user's message with just: /removetitle")
            return

        target_identifier = context.args[0]
        target_id = None
        if target_identifier.isdigit():
            target_id = int(target_identifier)
        else:
            target_id = await get_user_id_by_username(context, update.effective_chat.id, target_identifier)

        if not target_id:
            await update.message.reply_text(f"Could not find user {target_identifier}.")
            return

        try:
            target_user = (await context.bot.get_chat_member(update.effective_chat.id, target_id)).user
        except Exception:
            await update.message.reply_text(f"Could not retrieve user information for ID {target_id}.")
            return

    if not target_user:
        await update.message.reply_text("Could not determine target user.")
        return

    titles = load_user_titles()
    group_id_str = str(update.effective_chat.id)
    target_id_str = str(target_user.id)
    display_name = get_display_name(target_user.id, target_user.full_name, update.effective_chat.id) # Get name before potentially removing title

    if group_id_str in titles and target_id_str in titles[group_id_str]:
        del titles[group_id_str][target_id_str]
        save_user_titles(titles)
        # Use the name *without* the title for the confirmation message.
        await update.message.reply_text(f"Title for {html.escape(target_user.full_name)} has been removed for this group.", parse_mode='HTML')
    else:
        await update.message.reply_text(f"User {display_name} does not have a title in this group.", parse_mode='HTML')

@command_handler_wrapper(admin_only=True)
async def viewstakes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /viewstakes <user> - View all media staked by a user. """
    if not context.args:
        await update.message.reply_text("Usage: /viewstakes <@username or user_id>")
        return

    if update.effective_chat.type != "private":
        await update.message.reply_text("This command can only be used in a private chat for privacy reasons.")
        return

    target_identifier = context.args[0]
    target_id = None

    if target_identifier.isdigit():
        target_id = int(target_identifier)
    elif target_identifier.startswith('@'):
        username_to_find = target_identifier[1:].lower()
        profiles = load_user_profiles()
        found = False
        for uid, uname in profiles.items():
            if uname.lower() == username_to_find:
                target_id = int(uid)
                found = True
                break
        if not found:
            await update.message.reply_text(f"Could not find user {target_identifier} in my cache. Please use their user ID instead, or ensure I have seen them in a group recently.")
            return
    else:
        await update.message.reply_text("Please provide a valid user ID or a @username starting with '@'.")
        return

    stakes = load_media_stakes()
    user_stakes = stakes.get(str(target_id))

    if not user_stakes:
        await update.message.reply_text(f"No media stakes found for user {target_id}.")
        return

    await update.message.reply_text(f"Found {len(user_stakes)} media stake(s) for user {target_id}. Sending them now...")

    for stake in user_stakes:
        opponent_text = f"against user {stake['opponent_id']}" if stake.get('opponent_id') else ""
        group_text = f"in group {stake['group_id']}" if stake.get('group_id') else ""
        time_text = datetime.fromtimestamp(stake['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

        caption = f"Stake from {time_text} {opponent_text} {group_text}".strip()

        try:
            if stake['media_type'] == 'photo':
                await context.bot.send_photo(update.effective_chat.id, stake['file_id'], caption=caption)
            elif stake['media_type'] == 'video':
                await context.bot.send_video(update.effective_chat.id, stake['file_id'], caption=caption)
            elif stake['media_type'] == 'voice':
                await context.bot.send_voice(update.effective_chat.id, stake['file_id'], caption=caption)
        except Exception as e:
            await update.message.reply_text(f"Could not send a stake (file_id: {stake['file_id']}). It might have expired. Error: {e}")
            logger.error(f"Failed to send stake for user {target_id}: {e}")

def load_admin_data():
    """Loads admin data, migrating from old list format to new dict format if necessary."""
    if not os.path.exists(ADMIN_DATA_FILE):
        return {'owner': str(OWNER_ID), 'admins': {}}

    with open(ADMIN_DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # --- Migration from old format ---
    if isinstance(data.get('admins'), list):
        logger.warning("Old admin file format detected. Migrating to new format.")
        old_admins = data['admins']
        data['admins'] = {}
        if str(OWNER_ID) in old_admins:
             old_admins.remove(str(OWNER_ID))
        if old_admins:
             # We don't know which group these admins came from, so we assign them to a legacy group.
             # The next /update in their actual group will fix this.
             for admin_id in old_admins:
                 data['admins'][str(admin_id)] = ["legacy_group"]
        save_admin_data(data)
        logger.info("Admin file migrated successfully.")
    # --- End Migration ---

    if 'owner' not in data:
        data['owner'] = str(OWNER_ID)
    if 'admins' not in data:
        data['admins'] = {}

    logger.debug(f"Loaded admin data: {data}")
    return data

def save_admin_data(data):
    """Saves admin and owner data to file."""
    with open(ADMIN_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.debug(f"Saved admin data: {data}")

def is_owner(user_id):
    """Check if the user is the owner."""
    data = load_admin_data()
    result = str(user_id) == str(data.get('owner'))
    logger.debug(f"is_owner({user_id}) -> {result}")
    return result

def get_display_name(user_id: int, full_name: str, group_id: int = None) -> str:
    """
    Determines the display name for a user, returning a taggable HTML mention.
    If a group_id is provided, it attempts to find a group-specific title.
    If no title is found, it falls back to the user's full name.
    """
    titles = load_user_titles()
    display_text = full_name

    if group_id:
        group_titles = titles.get(str(group_id), {})
        display_text = group_titles.get(str(user_id), full_name)

    return f'<a href="tg://user?id={user_id}">{html.escape(display_text)}</a>'

def is_admin(user_id):
    """Check if the user is an admin or the owner."""
    admin_data = load_admin_data()
    user_id_str = str(user_id)

    if user_id_str == admin_data.get('owner'):
        return True

    # Check if the user is a key in the admins dictionary and has at least one group.
    if user_id_str in admin_data.get('admins', {}) and admin_data['admins'][user_id_str]:
        return True

    return False

@command_handler_wrapper(admin_only=True)
async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /update - (Owner only) Syncs the admin list for the current group. """
    if not is_owner(update.effective_user.id):
        await update.message.reply_text("This command is for the bot owner only.")
        return

    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command can only be used in a group.")
        return

    chat_id_str = str(update.effective_chat.id)

    try:
        # 1. Get current admins from Telegram
        tg_admins = await context.bot.get_chat_administrators(chat_id_str)
        tg_admin_ids = {str(admin.user.id) for admin in tg_admins}

        # 2. Load our stored data
        admin_data = load_admin_data()
        stored_admins = admin_data.get('admins', {})

        # 3. Find admins for this specific group in our records
        stored_group_admin_ids = {
            uid for uid, groups in stored_admins.items() if chat_id_str in groups
        }

        # 4. Determine who was added and who was removed
        added_ids = tg_admin_ids - stored_group_admin_ids
        removed_ids = stored_group_admin_ids - tg_admin_ids

        # 5. Update the stored data
        for admin_id in added_ids:
            stored_admins.setdefault(admin_id, []).append(chat_id_str)

        for admin_id in removed_ids:
            if admin_id in stored_admins and chat_id_str in stored_admins[admin_id]:
                stored_admins[admin_id].remove(chat_id_str)
            # If the user is no longer an admin in any group, remove them entirely
            if admin_id in stored_admins and not stored_admins[admin_id]:
                del stored_admins[admin_id]

        save_admin_data(admin_data)

        # 6. Report changes
        response_lines = []
        if added_ids:
            added_mentions = []
            for admin_id in added_ids:
                user = next((a.user for a in tg_admins if str(a.user.id) == admin_id), None)
                if user:
                    added_mentions.append(get_display_name(user.id, user.full_name, update.effective_chat.id))
                    cache_user_profile(user)
            if added_mentions:
                response_lines.append("New admins added: " + ", ".join(added_mentions))

        if removed_ids:
            removed_names = []
            profiles = load_user_profiles()
            for admin_id in removed_ids:
                # Try to get the name from the cache, fall back to ID if not found
                name = profiles.get(admin_id, f"User ID {admin_id}")
                removed_names.append(html.escape(name))
            response_lines.append("Admins removed from this group: " + ", ".join(removed_names))

        if not response_lines:
            await update.message.reply_text("Admin list is already up to date for this group.")
        else:
            await update.message.reply_text("\n".join(response_lines), parse_mode='HTML')

    except Exception as e:
        logger.error(f"Failed to update admins: {e}", exc_info=True)
        await update.message.reply_text(f"An error occurred while updating admins: {e}")

async def get_user_id_by_username(context, chat_id, username) -> str:
    """Get a user's Telegram ID by their username, using the profile cache."""
    username_to_find = username.lower().lstrip('@')
    profiles = load_user_profiles()
    for uid, uname in profiles.items():
        if uname and uname.lower() == username_to_find:
            logger.debug(f"Found user ID {uid} for username {username} in cache.")
            return str(uid)

    # As a last resort, check admins if the user is not in the cache
    try:
        for member in await context.bot.get_chat_administrators(chat_id):
            if member.user.username and member.user.username.lower() == username_to_find:
                logger.debug(f"Found admin user ID {member.user.id} for username {username}")
                cache_user_profile(member.user) # Cache them for next time
                return str(member.user.id)
    except Exception as e:
        logger.error(f"Could not search chat admins for {username} in chat {chat_id}: {e}")

    logger.debug(f"Username {username} not found in cache or admin list for chat {chat_id}")
    return None

# =============================
# Reward System Storage & Helpers
# =============================
REWARDS_DATA_FILE = 'rewards.json'  # Stores rewards per group

DEFAULT_REWARD = {"name": "Other", "cost": 0}

def load_rewards_data():
    if os.path.exists(REWARDS_DATA_FILE):
        with open(REWARDS_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_rewards_data(data):
    with open(REWARDS_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_rewards_list(group_id):
    data = load_rewards_data()
    group_id = str(group_id)
    rewards = data.get(group_id, [])
    # Always include the default "Other" reward at the end
    if not any(r["name"].lower() == "other" for r in rewards):
        rewards.append(DEFAULT_REWARD)
    return rewards

def add_reward(group_id, name, cost):
    if name.strip().lower() == "other":
        return False
    data = load_rewards_data()
    group_id = str(group_id)
    if group_id not in data:
        data[group_id] = []
    # Prevent duplicates
    for r in data[group_id]:
        if r["name"].lower() == name.strip().lower():
            return False
    data[group_id].append({"name": name.strip(), "cost": int(cost)})
    save_rewards_data(data)
    logger.debug(f"Added reward '{name}' with cost {cost} to group {group_id}")
    return True

def remove_reward(group_id, name):
    if name.strip().lower() == "other":
        return False
    data = load_rewards_data()
    group_id = str(group_id)
    if group_id not in data:
        return False
    before = len(data[group_id])
    data[group_id] = [r for r in data[group_id] if r["name"].lower() != name.strip().lower()]
    after = len(data[group_id])
    save_rewards_data(data)
    logger.debug(f"Removed reward '{name}' from group {group_id}")
    return before != after

# =============================
# Point System Storage & Helpers
# =============================
POINTS_DATA_FILE = 'points.json'  # Stores user points per group

def load_points_data():
    if os.path.exists(POINTS_DATA_FILE):
        with open(POINTS_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_points_data(data):
    with open(POINTS_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_user_points(group_id, user_id):
    data = load_points_data()
    group_id = str(group_id)
    user_id = str(user_id)
    return data.get(group_id, {}).get(user_id, 0)

def set_user_points(group_id, user_id, points):
    data = load_points_data()
    group_id = str(group_id)
    user_id = str(user_id)
    if group_id not in data:
        data[group_id] = {}
    data[group_id][user_id] = points
    save_points_data(data)
    logger.debug(f"Set points for user {user_id} in group {group_id} to {points}")

async def check_for_punishment(group_id, user_id, old_points, new_points, context: ContextTypes.DEFAULT_TYPE):
    punishments_data = load_punishments_data()
    group_id_str = str(group_id)

    if group_id_str not in punishments_data:
        return

    group_punishments = punishments_data[group_id_str]
    triggered_punishments = get_triggered_punishments_for_user(group_id, user_id)

    for punishment in group_punishments:
        threshold = punishment.get("threshold")
        message = punishment.get("message")

        if threshold is None or message is None:
            continue

        # Condition 1: User's points just crossed BELOW the threshold
        if old_points >= threshold and new_points < threshold:
            if message not in triggered_punishments:
                # Punish the user
                user_member = await context.bot.get_chat_member(group_id, user_id)
                display_name = get_display_name(user_id, user_member.user.full_name, group_id)
                await context.bot.send_message(
                    chat_id=group_id,
                    text=f"🚨 <b>Punishment Issued!</b> 🚨\n{display_name} has fallen below {threshold} points. Punishment: {message}",
                    parse_mode='HTML'
                )

                chat = await context.bot.get_chat(group_id)
                admins = await context.bot.get_chat_administrators(group_id)
                for admin in admins:
                    try:
                        await context.bot.send_message(
                            chat_id=admin.user.id,
                            text=f"User {display_name} (ID: {user_id}) in group {chat.title} (ID: {group_id}) triggered punishment '{message}' by falling below {threshold} points.",
                            parse_mode='HTML'
                        )
                    except Exception:
                        logger.warning(f"Failed to notify admin {admin.user.id} about punishment.")

                add_triggered_punishment_for_user(group_id, user_id, message)

        # Condition 2: User's points are now at or above the threshold, so they are eligible again
        elif new_points >= threshold:
            if message in triggered_punishments:
                remove_triggered_punishment_for_user(group_id, user_id, message)
                logger.info(f"User {user_id} is now above threshold {threshold}, punishment '{message}' can be triggered again.")

async def add_user_points(group_id, user_id, delta, context: ContextTypes.DEFAULT_TYPE):
    old_points = get_user_points(group_id, user_id)
    new_points = old_points + delta
    set_user_points(group_id, user_id, new_points)
    logger.debug(f"Added {delta} points for user {user_id} in group {group_id} (new total: {new_points})")

    # If user's points are non-negative, reset their negative strike counter for this group.
    if new_points >= 0:
        tracker = load_negative_tracker()
        group_id_str = str(group_id)
        user_id_str = str(user_id)
        if group_id_str in tracker and user_id_str in tracker.get(group_id_str, {}):
            if tracker[group_id_str][user_id_str] != 0:
                tracker[group_id_str][user_id_str] = 0
                save_negative_tracker(tracker)
                logger.debug(f"Reset negative points tracker for user {user_id_str} in group {group_id_str}.")

    # Run all punishment checks
    await check_for_punishment(group_id, user_id, old_points, new_points, context)
    await check_for_negative_points(group_id, user_id, new_points, context)

# =============================
# Negative Points Tracker
# =============================
NEGATIVE_POINTS_TRACKER_FILE = 'negative_points_tracker.json'

def load_negative_tracker():
    if os.path.exists(NEGATIVE_POINTS_TRACKER_FILE):
        with open(NEGATIVE_POINTS_TRACKER_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_negative_tracker(data):
    with open(NEGATIVE_POINTS_TRACKER_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

async def check_for_negative_points(group_id, user_id, points, context: ContextTypes.DEFAULT_TYPE):
    if points < 0:
        tracker = load_negative_tracker()
        group_id_str = str(group_id)
        user_id_str = str(user_id)

        if group_id_str not in tracker:
            tracker[group_id_str] = {}

        current_strikes = tracker.get(group_id_str, {}).get(user_id_str, 0)
        current_strikes += 1
        tracker[group_id_str][user_id_str] = current_strikes
        save_negative_tracker(tracker)

        user_member = await context.bot.get_chat_member(group_id, user_id)
        user_mention = user_member.user.mention_html()

        if current_strikes < 3:
            # On the first and second strike, mute for 24h and reset points.
            try:
                await context.bot.restrict_chat_member(
                    chat_id=group_id,
                    user_id=user_id,
                    permissions={'can_send_messages': False},
                    until_date=time.time() + 86400  # 24 hours
                )
                set_user_points(group_id, user_id, 0) # Reset points to 0
                await context.bot.send_message(
                    chat_id=group_id,
                    text=f"{user_mention} has dropped into negative points (Strike {current_strikes}/3). They have been muted for 24 hours and their points reset to 0.",
                    parse_mode='HTML'
                )
            except Exception:
                logger.exception(f"Failed to mute user {user_id} for negative points (Strike {current_strikes}).")
        else:
            # On the third strike, send a special message and notify admins.
            tracker[group_id_str][user_id_str] = 0  # Reset strikes after 3rd strike
            save_negative_tracker(tracker)

            chat = await context.bot.get_chat(group_id)
            admins = await context.bot.get_chat_administrators(group_id)
            await context.bot.send_message(
                chat_id=group_id,
                text=f"🚨 <b>Third Strike!</b> 🚨\n{user_mention} has reached negative points for the third time. A special punishment from the admins is coming, and you are not allowed to refuse if you wish to remain in the group.",
                parse_mode='HTML'
            )
            for admin in admins:
                try:
                    await context.bot.send_message(
                        chat_id=admin.user.id,
                        text=f"User {user_mention} in group '{chat.title}' has reached negative points for the third time and requires a special punishment. Their strike counter has been reset.",
                        parse_mode='HTML'
                    )
                except Exception:
                    logger.warning(f"Failed to notify admin {admin.user.id} about 3rd strike.")

# =============================
# Chance Game Helpers
# =============================
CHANCE_COOLDOWNS_FILE = 'chance_cooldowns.json'

def load_cooldowns():
    if os.path.exists(CHANCE_COOLDOWNS_FILE):
        with open(CHANCE_COOLDOWNS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cooldowns(data):
    with open(CHANCE_COOLDOWNS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_chance_outcome():
    """
    Returns a random outcome for the chance game based on weighted probabilities.
    """
    outcomes = [
        {"name": "plus_50", "weight": 15},
        {"name": "minus_100", "weight": 15},
        {"name": "chastity_2_days", "weight": 15},
        {"name": "chastity_7_days", "weight": 5},
        {"name": "nothing", "weight": 30},
        {"name": "free_reward", "weight": 10},
        {"name": "lose_all_points", "weight": 2.5},
        {"name": "double_points", "weight": 2.5},
        {"name": "ask_task", "weight": 5},
    ]

    total_weight = sum(o['weight'] for o in outcomes)
    random_num = random.uniform(0, total_weight)

    current_weight = 0
    for outcome in outcomes:
        current_weight += outcome['weight']
        if random_num <= current_weight:
            return outcome['name']

import asyncio

# ===================================
# Truth or Dare Data Management
# ===================================
TOD_DATA_LOCK = asyncio.Lock()
ACTIVE_TOD_GAMES_LOCK = asyncio.Lock()

async def load_tod_data():
    """Asynchronously loads truth or dare data from the JSON file with a lock."""
    async with TOD_DATA_LOCK:
        if os.path.exists(TOD_DATA_FILE):
            with open(TOD_DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

async def save_tod_data(data):
    """Asynchronously saves truth or dare data to the JSON file with a lock."""
    async with TOD_DATA_LOCK:
        with open(TOD_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

async def load_active_tod_games():
    """Asynchronously loads active truth or dare games from the JSON file with a lock."""
    async with ACTIVE_TOD_GAMES_LOCK:
        if os.path.exists(ACTIVE_TOD_GAMES_FILE):
            with open(ACTIVE_TOD_GAMES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

async def save_active_tod_games(data):
    """Asynchronously saves active truth or dare games to the JSON file with a lock."""
    async with ACTIVE_TOD_GAMES_LOCK:
        with open(ACTIVE_TOD_GAMES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# =============================
# Game System Storage & Helpers
# =============================
GAMES_DATA_FILE = 'games.json'
GAMES_DATA_LOCK = asyncio.Lock()

async def load_games_data_async():
    """Asynchronously loads game data from the JSON file with a lock."""
    async with GAMES_DATA_LOCK:
        if os.path.exists(GAMES_DATA_FILE):
            with open(GAMES_DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

async def save_games_data_async(data):
    """Asynchronously saves game data to the JSON file with a lock."""
    async with GAMES_DATA_LOCK:
        with open(GAMES_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


# =============================
# Game Logic Helpers
# =============================
async def update_game_activity(game_id: str):
    """Updates the last_activity timestamp for a game and resets the warning flag."""
    games_data = await load_games_data_async()
    if game_id in games_data:
        games_data[game_id]['last_activity'] = time.time()
        games_data[game_id].pop('warning_sent', None) # Remove warning flag on any activity
        await save_games_data_async(games_data)

def create_connect_four_board_markup(board: list, game_id: str):
    """Creates the text and markup for a Connect Four board."""
    emojis = {0: '⚫️', 1: '🔴', 2: '🟡'}
    board_text = ""
    for row in board:
        board_text += " ".join([emojis.get(cell, '⚫️') for cell in row]) + "\n"

    keyboard = [
        [InlineKeyboardButton(str(i + 1), callback_data=f'c4:move:{game_id}:{i}') for i in range(7)]
    ]
    return board_text, InlineKeyboardMarkup(keyboard)


def check_connect_four_win(board: list, player_num: int) -> bool:
    """Check for a win in Connect Four."""
    # Check horizontal
    for r in range(6):
        for c in range(4):
            if all(board[r][c + i] == player_num for i in range(4)):
                return True
    # Check vertical
    for r in range(3):
        for c in range(7):
            if all(board[r + i][c] == player_num for i in range(4)):
                return True
    # Check diagonal (down-right)
    for r in range(3):
        for c in range(4):
            if all(board[r + i][c + i] == player_num for i in range(4)):
                return True
    # Check diagonal (up-right)
    for r in range(3, 6):
        for c in range(4):
            if all(board[r - i][c + i] == player_num for i in range(4)):
                return True
    return False


def check_connect_four_draw(board: list) -> bool:
    """Check for a draw in Connect Four."""
    return all(cell != 0 for cell in board[0])


async def delete_tracked_messages(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Deletes all tracked messages for a game and clears the list."""
    games_data = await load_games_data_async()
    game = games_data.get(game_id)
    if not game:
        return

    messages_to_delete = game.get('messages_to_delete', [])
    logger.debug(f"Attempting to delete {len(messages_to_delete)} messages for game {game_id}.")
    for msg in messages_to_delete:
        logger.debug(f"Deleting message {msg['message_id']} in chat {msg['chat_id']}")
        try:
            success = await context.bot.delete_message(chat_id=msg['chat_id'], message_id=msg['message_id'])
            if not success:
                logger.error(f"Deletion of message {msg['message_id']} returned False but did not raise exception.")
        except Exception as e:
            logger.error(f"Explicitly failed to delete message {msg['message_id']}", exc_info=True)
        await asyncio.sleep(0.5) # Add a small delay to avoid rate limiting

    if game_id in games_data:
        games_data[game_id]['messages_to_delete'] = []
        await save_games_data_async(games_data)


async def send_and_track_message(context, chat_id, game_id, text, **kwargs):
    """Sends a message and tracks it for later deletion."""
    sent_message = await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)
    games_data = await load_games_data_async()
    if game_id in games_data:
        logger.debug(f"Tracking message {sent_message.message_id} in chat {chat_id} for game {game_id}")
        games_data[game_id].setdefault('messages_to_delete', []).append({'chat_id': sent_message.chat_id, 'message_id': sent_message.message_id})
        await save_games_data_async(games_data)
    return sent_message

async def handle_game_over(context: ContextTypes.DEFAULT_TYPE, game_id: str, winner_id: int, loser_id: int):
    """Handles the end of a game, distributing stakes."""
    games_data = await load_games_data_async()
    game = games_data[game_id]

    if str(game['challenger_id']) == str(loser_id):
        loser_stake = game.get('challenger_stake')
    else:
        loser_stake = game.get('opponent_stake')

    if not loser_stake:
        logger.error(f"No loser stake found for game {game_id}")
        return

    loser_member = await context.bot.get_chat_member(game['group_id'], loser_id)
    winner_member = await context.bot.get_chat_member(game['group_id'], winner_id)
    loser_name = get_display_name(loser_id, loser_member.user.full_name, game['group_id'])
    winner_name = get_display_name(winner_id, winner_member.user.full_name, game['group_id'])

    # Store winner and loser IDs for the revenge handler
    game['winner_id'] = winner_id
    game['loser_id'] = loser_id

    # Add a revenge button with shortened callback_data
    keyboard = [[InlineKeyboardButton("Revenge 😈", callback_data=f"game:revenge:{game_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if loser_stake['type'] == 'points':
        points_val = loser_stake['value']
        await add_user_points(game['group_id'], winner_id, points_val, context)
        await add_user_points(game['group_id'], loser_id, -points_val, context)
        message = f"{winner_name} has won the game! {loser_name} lost {points_val} points."
        await context.bot.send_message(
            game['group_id'],
            message,
            parse_mode='HTML',
            reply_markup=reply_markup
        )
    else:  # media
        caption = f"{winner_name} won the game! This is the loser's stake from {loser_name}."
        if loser_stake['type'] == 'photo':
            await context.bot.send_photo(game['group_id'], loser_stake['value'], caption=caption, parse_mode='HTML', reply_markup=reply_markup)
        elif loser_stake['type'] == 'video':
            await context.bot.send_video(game['group_id'], loser_stake['value'], caption=caption, parse_mode='HTML', reply_markup=reply_markup)
        elif loser_stake['type'] == 'voice':
            await context.bot.send_voice(game['group_id'], loser_stake['value'], caption=caption, parse_mode='HTML', reply_markup=reply_markup)

    # Private messages to players (Battleship only)
    if game.get('game_type') == 'battleship':
        try:
            await context.bot.send_message(winner_id, "Congratulations, you won the game!")
        except Exception as e:
            logger.warning(f"Failed to send win message to {winner_id}: {e}")

        try:
            await context.bot.send_message(loser_id, "You lost the game. Better luck next time!")
        except Exception as e:
            logger.warning(f"Failed to send loss message to {loser_id}: {e}")

    game['status'] = 'complete'
    await save_games_data_async(games_data)
    await delete_tracked_messages(context, game_id)


async def revenge_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the revenge button click, allowing a loser to start a new game."""
    query = update.callback_query
    await query.answer()

    try:
        _, _, old_game_id = query.data.split(':')
    except (ValueError, IndexError):
        logger.error(f"Could not parse revenge callback data: {query.data}")
        await query.edit_message_text("An error occurred while processing the revenge request.")
        return

    games_data = await load_games_data_async()
    old_game = games_data.get(old_game_id)
    if not old_game:
        await query.edit_message_text("The original game data could not be found. It might be too old.")
        return

    loser_id = old_game.get('loser_id')
    winner_id = old_game.get('winner_id')

    if not loser_id or not winner_id:
        await query.edit_message_text("The original game data is missing winner/loser information and a revenge match cannot be started.")
        return

    if query.from_user.id != loser_id:
        await query.answer("This is not your revenge to claim!", show_alert=True)
        return

    group_id = old_game['group_id']

    for game in games_data.values():
        if game.get('group_id') == group_id and game.get('status') != 'complete':
            await context.bot.send_message(group_id, "There is already an active game in this group. Please wait for it to finish before starting a revenge match.")
            return

    challenger_user = await context.bot.get_chat_member(group_id, loser_id)
    opponent_user = await context.bot.get_chat_member(group_id, winner_id)

    game_id = str(uuid.uuid4())
    games_data[game_id] = {
        "group_id": group_id,
        "challenger_id": challenger_user.user.id,
        "opponent_id": opponent_user.user.id,
        "is_revenge": True,
        "old_game_id": old_game_id,
        "status": "pending_game_selection",
        "messages_to_delete": [],
        "last_activity": time.time()
    }
    await save_games_data_async(games_data)

    challenger_name = get_display_name(challenger_user.user.id, challenger_user.user.full_name, group_id)
    opponent_name = get_display_name(opponent_user.user.id, opponent_user.user.full_name, group_id)

    await context.bot.send_message(
        chat_id=group_id,
        text=f"{challenger_name} wants revenge against {opponent_name}! {challenger_name}, check your private messages to set up the game.",
        parse_mode='HTML'
    )

    try:
        keyboard = [[InlineKeyboardButton("Start Revenge Setup", callback_data=f"game:setup:start:{game_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_and_track_message(
            context,
            challenger_user.user.id,
            game_id,
            "Let's set up your revenge! Click the button below to begin.",
            reply_markup=reply_markup
        )
    except Exception:
        logger.exception(f"Failed to send private message to user {challenger_user.user.id}")

    await query.edit_message_reply_markup(reply_markup=None)


async def connect_four_move_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a move in a Connect Four game."""
    query = update.callback_query
    await query.answer()

    _, _, game_id, col_str = query.data.split(':')
    await update_game_activity(game_id)
    col = int(col_str)
    user_id = query.from_user.id

    games_data = await load_games_data_async()
    game = games_data.get(game_id)

    if not game or game.get('status') != 'active':
        await query.edit_message_text("This game is no longer active.")
        return

    # Check if it's the user's turn
    if game.get('turn') != user_id:
        await query.answer("It's not your turn!", show_alert=True)
        return

    # Make the move
    board = game['board']
    player_num = 1 if user_id == game['challenger_id'] else 2

    # Find the lowest empty row in the column
    move_made = False
    for r in range(5, -1, -1):
        if board[r][col] == 0:
            board[r][col] = player_num
            move_made = True
            break

    if not move_made:
        await query.answer("This column is full!", show_alert=True)
        return

    game['board'] = board

    # Check for win
    if check_connect_four_win(board, player_num):
        winner_id = user_id
        loser_id = game['opponent_id'] if user_id == game['challenger_id'] else game['challenger_id']

        winner_member = await context.bot.get_chat_member(game['group_id'], winner_id)
        winner_name = get_display_name(winner_id, winner_member.user.full_name, game['group_id'])

        board_text, _ = create_connect_four_board_markup(board, game_id)

        win_message = f"{winner_name} wins!"

        await query.edit_message_text(
            f"<b>Connect Four - Game Over!</b>\n\n{board_text}\n{win_message}",
            parse_mode='HTML'
        )
        await handle_game_over(context, game_id, winner_id, loser_id)
        return

    # Check for draw
    if check_connect_four_draw(board):
        board_text, _ = create_connect_four_board_markup(board, game_id)
        await query.edit_message_text(f"<b>Connect Four - Draw!</b>\n\n{board_text}\nThe game is a draw!")
        game['status'] = 'complete'
        await save_games_data_async(games_data)
        return

    # Switch turns
    game['turn'] = game['opponent_id'] if user_id == game['challenger_id'] else game['challenger_id']
    await save_games_data_async(games_data)

    # Update board message
    turn_player_id = game['turn']
    turn_player_member = await context.bot.get_chat_member(game['group_id'], turn_player_id)
    turn_player_name = get_display_name(turn_player_id, turn_player_member.user.full_name, game['group_id'])
    board_text, reply_markup = create_connect_four_board_markup(game['board'], game_id)

    await query.edit_message_text(
        f"<b>Connect Four</b>\n\n{board_text}\nIt's {turn_player_name}'s turn.",
        reply_markup=reply_markup,
        parse_mode='HTML'
    )


def create_tictactoe_board_markup(board: list, game_id: str):
    """Creates the text and markup for a Tic-Tac-Toe board."""
    emojis = {0: '➖', 1: '❌', 2: '⭕️'}
    keyboard = []
    for r in range(3):
        row = []
        for c in range(3):
            # If cell is empty, it's a button. Otherwise, just text.
            if board[r][c] == 0:
                row.append(InlineKeyboardButton('➖', callback_data=f'ttt:move:{game_id}:{r}:{c}'))
            else:
                row.append(InlineKeyboardButton(emojis[board[r][c]], callback_data='ttt:noop'))
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

def check_tictactoe_win(board: list, player_num: int) -> bool:
    """Check for a win in Tic-Tac-Toe."""
    # Check rows, columns, and diagonals
    for i in range(3):
        if all(board[i][j] == player_num for j in range(3)): return True
        if all(board[j][i] == player_num for j in range(3)): return True
    if all(board[i][i] == player_num for i in range(3)): return True
    if all(board[i][2-i] == player_num for i in range(3)): return True
    return False

def check_tictactoe_draw(board: list) -> bool:
    """Check for a draw in Tic-Tac-Toe."""
    return all(cell != 0 for row in board for cell in row)

async def handle_game_draw(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Handles a draw, where both players lose their stakes."""
    games_data = await load_games_data_async()
    game = games_data[game_id]

    challenger_id = game['challenger_id']
    opponent_id = game['opponent_id']
    challenger_stake = game.get('challenger_stake')
    opponent_stake = game.get('opponent_stake')

    challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
    opponent_member = await context.bot.get_chat_member(game['group_id'], opponent_id)
    challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_id, opponent_member.user.full_name, game['group_id'])

    await context.bot.send_message(
        game['group_id'],
        f"The game between {challenger_name} and {opponent_name} ended in a draw! Both players lose their stakes.",
        parse_mode='HTML'
    )

    # Handle challenger's stake
    if challenger_stake:
        if challenger_stake['type'] == 'points':
            points_val = challenger_stake['value']
            await add_user_points(game['group_id'], challenger_id, -points_val, context)
            await context.bot.send_message(
                game['group_id'],
                f"{challenger_name} lost {points_val} points in the draw.",
                parse_mode='HTML'
            )
        else: # media
            caption = f"This was {challenger_name}'s stake from the drawn game."
            if challenger_stake['type'] == 'photo':
                await context.bot.send_photo(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')
            elif challenger_stake['type'] == 'video':
                await context.bot.send_video(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')
            elif challenger_stake['type'] == 'voice':
                await context.bot.send_voice(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')

    # Handle opponent's stake
    if opponent_stake:
        if opponent_stake['type'] == 'points':
            points_val = opponent_stake['value']
            await add_user_points(game['group_id'], opponent_id, -points_val, context)
            await context.bot.send_message(
                game['group_id'],
                f"{opponent_name} lost {points_val} points in the draw.",
                parse_mode='HTML'
            )
        else: # media
            caption = f"This was {opponent_name}'s stake from the drawn game."
            if opponent_stake['type'] == 'photo':
                await context.bot.send_photo(game['group_id'], opponent_stake['value'], caption=caption, parse_mode='HTML')
            elif opponent_stake['type'] == 'video':
                await context.bot.send_video(game['group_id'], opponent_stake['value'], caption=caption, parse_mode='HTML')
            elif opponent_stake['type'] == 'voice':
                await context.bot.send_voice(game['group_id'], opponent_stake['value'], caption=caption, parse_mode='HTML')

    game['status'] = 'complete'
    await save_games_data_async(games_data)
    await delete_tracked_messages(context, game_id)


async def tictactoe_move_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a move in a Tic-Tac-Toe game."""
    query = update.callback_query

    if query.data == 'ttt:noop':
        await query.answer("This spot is already taken.", show_alert=True)
        return

    await query.answer()

    _, _, game_id, r_str, c_str = query.data.split(':')
    r, c = int(r_str), int(c_str)
    user_id = query.from_user.id

    await update_game_activity(game_id)

    games_data = await load_games_data_async()
    game = games_data.get(game_id)

    if not game or game.get('status') != 'active':
        await query.edit_message_text("This game is no longer active.")
        return

    if game.get('turn') != user_id:
        await query.answer("It's not your turn!", show_alert=True)
        return

    board = game['board']
    player_num = 1 if user_id == game['challenger_id'] else 2

    if board[r][c] != 0:
        await query.answer("This spot is already taken!", show_alert=True)
        return

    board[r][c] = player_num
    game['board'] = board

    if check_tictactoe_win(board, player_num):
        winner_id = user_id
        loser_id = game['opponent_id'] if user_id == game['challenger_id'] else game['challenger_id']
        winner_member = await context.bot.get_chat_member(game['group_id'], winner_id)
        winner_name = get_display_name(winner_id, winner_member.user.full_name, game['group_id'])

        reply_markup = create_tictactoe_board_markup(board, game_id)
        await query.edit_message_text(
            f"<b>Tic-Tac-Toe - Game Over!</b>\n\n{winner_name} wins!",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        await handle_game_over(context, game_id, winner_id, loser_id)
        return

    if check_tictactoe_draw(board):
        reply_markup = create_tictactoe_board_markup(board, game_id)
        await query.edit_message_text(
            "<b>Tic-Tac-Toe - Draw!</b>\n\nBoth players lose their stake.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        await handle_game_draw(context, game_id)
        return

    # Switch turns
    game['turn'] = game['opponent_id'] if user_id == game['challenger_id'] else game['challenger_id']
    await save_games_data_async(games_data)

    turn_player_id = game['turn']
    turn_player_member = await context.bot.get_chat_member(game['group_id'], turn_player_id)
    turn_player_name = get_display_name(turn_player_id, turn_player_member.user.full_name, game['group_id'])
    reply_markup = create_tictactoe_board_markup(board, game_id)

    await query.edit_message_text(
        f"<b>Tic-Tac-Toe</b>\n\nIt's {turn_player_name}'s turn.",
        reply_markup=reply_markup,
        parse_mode='HTML'
    )


# =============================
# Game Logic Helpers
# =============================
BATTLESHIP_SHIPS = {
    "Carrier": 5, "Battleship": 4, "Cruiser": 3,
    "Submarine": 3, "Destroyer": 2,
}
BS_AWAITING_PLACEMENT = 0

def parse_bs_coords(coord_str: str) -> tuple[int, int] | None:
    """Parses 'A1' style coordinates into (row, col) tuple."""
    coord_str = coord_str.upper().strip()
    if not (2 <= len(coord_str) <= 3): return None
    col_char = coord_str[0]
    row_str = coord_str[1:]
    if not ('A' <= col_char <= 'J' and row_str.isdigit()): return None
    row = int(row_str) - 1
    col = ord(col_char) - ord('A')
    if not (0 <= row <= 9 and 0 <= col <= 9): return None
    return row, col

def generate_bs_board_text(board: list, show_ships: bool = True) -> str:
    """Generates a text representation of a battleship board."""
    emojis = {'water': '🟦', 'ship': '🚢', 'hit': '🔥', 'miss': '❌'}

    map_values = {0: emojis['water'], 1: emojis['ship'] if show_ships else emojis['water'], 2: emojis['miss'], 3: emojis['hit']}

    header = '   ' + '  '.join('ABCDEFGHIJ') + '\n'
    board_text = header
    for r, row_data in enumerate(board):
        row_num = str(r + 1).rjust(2)
        row_str = ' '.join([map_values.get(cell, '🟦') for cell in row_data])
        board_text += f"{row_num} {row_str}\n"
    return board_text

async def handle_challenge_timeout(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Handles auto-cancellation of a challenge due to timeout. The challenger's stake is NOT lost."""
    games_data = await load_games_data_async()
    if game_id not in games_data:
        return
    game = games_data[game_id]

    # Only act on games that are actually pending acceptance to avoid race conditions
    if game.get('status') != 'pending_opponent_acceptance':
        return

    challenger_id = game['challenger_id']
    opponent_id = game['opponent_id']

    challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
    opponent_member = await context.bot.get_chat_member(game['group_id'], opponent_id)
    challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_id, opponent_member.user.full_name, game['group_id'])

    await context.bot.send_message(
        game['group_id'],
        f"The challenge from {challenger_name} to {opponent_name} expired due to inactivity and has been cancelled. No stakes were lost.",
        parse_mode='HTML'
    )

    # Clean up the game by marking it as complete and deleting tracked messages
    game['status'] = 'complete'
    await save_games_data_async(games_data)
    await delete_tracked_messages(context, game_id)


async def handle_game_cancellation(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Handles cancellation of a game due to inactivity, both players lose."""
    games_data = await load_games_data_async()
    if game_id not in games_data:
        return
    game = games_data[game_id]

    # Avoid cancelling already completed or non-active games
    if game.get('status') == 'complete':
        return

    challenger_id = game['challenger_id']
    opponent_id = game['opponent_id']
    challenger_stake = game.get('challenger_stake')
    opponent_stake = game.get('opponent_stake')

    challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
    opponent_member = await context.bot.get_chat_member(game['group_id'], opponent_id)
    challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_id, opponent_member.user.full_name, game['group_id'])

    await context.bot.send_message(
        game['group_id'],
        f"Game between {challenger_name} and {opponent_name} has been cancelled due to inactivity. Both players lose their stakes.",
        parse_mode='HTML'
    )

    # Handle challenger's stake
    if challenger_stake:
        if challenger_stake['type'] == 'points':
            await add_user_points(game['group_id'], challenger_id, -challenger_stake['value'], context)
        else: # media
            caption = f"This was {challenger_name}'s stake from the cancelled game."
            if challenger_stake['type'] == 'photo':
                await context.bot.send_photo(game['group_id'], challenger_stake['value'], caption=caption)
            elif challenger_stake['type'] == 'video':
                await context.bot.send_video(game['group_id'], challenger_stake['value'], caption=caption)
            elif challenger_stake['type'] == 'voice':
                await context.bot.send_voice(game['group_id'], challenger_stake['value'], caption=caption)

    # Handle opponent's stake
    if opponent_stake:
        if opponent_stake['type'] == 'points':
            await add_user_points(game['group_id'], opponent_id, -opponent_stake['value'], context)
        else: # media
            caption = f"This was {opponent_name}'s stake from the cancelled game."
            if opponent_stake['type'] == 'photo':
                await context.bot.send_photo(game['group_id'], opponent_stake['value'], caption=caption)
            elif opponent_stake['type'] == 'video':
                await context.bot.send_video(game['group_id'], opponent_stake['value'], caption=caption)
            elif opponent_stake['type'] == 'voice':
                await context.bot.send_voice(game['group_id'], opponent_stake['value'], caption=caption)

    game['status'] = 'complete'
    await save_games_data_async(games_data)
    await delete_tracked_messages(context, game_id)

async def handle_tod_timeout(context: ContextTypes.DEFAULT_TYPE, tod_game_id: str):
    """Handles a Truth or Dare game that has timed out while awaiting proof."""
    logger.info(f"Handling timeout for ToD game {tod_game_id}")
    active_games = await load_active_tod_games()
    game = active_games.get(tod_game_id)

    if not game:
        logger.warning(f"ToD game {tod_game_id} not found for timeout handling, it might have been completed or cancelled already.")
        return

    group_id = game['group_id']
    user_id = game['user_id']

    # Fetch user details for display name
    try:
        user_member = await context.bot.get_chat_member(group_id, user_id)
        display_name = get_display_name(user_id, user_member.user.full_name, int(group_id))
    except Exception as e:
        logger.error(f"Could not fetch user {user_id} for ToD timeout message: {e}")
        display_name = f"User {user_id}"

    # Penalize user
    await add_user_points(int(group_id), user_id, -15, context)

    # Notify group
    await context.bot.send_message(
        chat_id=group_id,
        text=f"⏰ {display_name} ran out of time to provide proof for their {game['type']} and has been penalized 15 points.",
        parse_mode='HTML'
    )

    # Notify admins (similar to refusal)
    admin_data = load_admin_data()
    group_admins = {uid for uid, groups in admin_data.get('admins', {}).items() if str(group_id) in groups}
    owner_id = admin_data.get('owner')
    if owner_id:
        group_admins.add(owner_id)

    notification_text = (
        f"🔔 <b>Timeout Notification</b> 🔔\n\n"
        f"User: {display_name}\n"
        f"Group ID: {group_id}\n"
        f"Task timed out: <i>{html.escape(game['text'])}</i>"
    )
    for admin_id in group_admins:
        try:
            await context.bot.send_message(chat_id=admin_id, text=notification_text, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Failed to notify admin {admin_id} of ToD timeout: {e}")

    # Edit original message
    try:
        original_text = f"The user's time ran out for the {game['type']}:\n\n<i>{html.escape(game['text'])}</i>"
        await context.bot.edit_message_text(
            chat_id=game['chat_id'],
            message_id=game['message_id'],
            text=original_text,
            reply_markup=None,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Could not edit original ToD message after timeout: {e}")

    # Clean up game
    if tod_game_id in active_games:
        del active_games[tod_game_id]
        await save_active_tod_games(active_games)

async def check_game_inactivity(context: ContextTypes.DEFAULT_TYPE):
    """Periodically checks for inactive games and handles them."""
    now = time.time()

    # --- Handle 2-Player Game Inactivity ---
    games_data = await load_games_data_async()
    for game_id, game in list(games_data.items()):
        if game.get('status', '') not in ['pending_opponent_acceptance', 'active', 'pending_game_selection', 'pending_opponent_stake']:
            continue

        last_activity = game.get('last_activity', 0)

        # Special 2-minute (120s) timeout for pending opponent acceptance
        if game.get('status') == 'pending_opponent_acceptance' and (now - last_activity > 120):
            logger.info(f"Game {game_id} timed out at acceptance stage. Cancelling without penalty.")
            await handle_challenge_timeout(context, game_id)
            continue

        # 7 minutes timeout -> cancel
        if now - last_activity > 420:
            logger.info(f"Game {game_id} timed out. Cancelling.")
            await handle_game_cancellation(context, game_id)
            continue

        # 5 minutes timeout -> warning
        elif now - last_activity > 300 and not game.get('warning_sent'):
            logger.info(f"Game {game_id} inactive for 5 minutes. Sending warning.")
            try:
                challenger = await context.bot.get_chat_member(game['group_id'], game['challenger_id'])
                opponent = await context.bot.get_chat_member(game['group_id'], game['opponent_id'])
                await context.bot.send_message(
                    chat_id=game['group_id'],
                    text=f"Warning: The game between {challenger.user.mention_html()} and {opponent.user.mention_html()} will be cancelled in 2 minutes due to inactivity.",
                    parse_mode='HTML'
                )
                games_data[game_id]['warning_sent'] = True
                await save_games_data_async(games_data)
            except Exception as e:
                logger.error(f"Failed to send inactivity warning for game {game_id}: {e}")

    # --- Handle Truth or Dare Inactivity ---
    active_tod_games = await load_active_tod_games()
    for tod_game_id, tod_game in list(active_tod_games.items()):
        if tod_game.get('status') != 'awaiting_proof':
            continue

        timestamp = tod_game.get('timestamp', 0)

        # 7 minutes timeout (420s) -> cancel
        if now - timestamp > 420:
            logger.info(f"ToD game {tod_game_id} timed out while awaiting proof. Cancelling.")
            await handle_tod_timeout(context, tod_game_id)
            continue

        # 5 minutes timeout (300s) -> warning
        elif now - timestamp > 300 and not tod_game.get('warning_sent'):
            logger.info(f"ToD game {tod_game_id} inactive for 5 minutes. Sending warning.")
            try:
                user = await context.bot.get_chat_member(tod_game['group_id'], tod_game['user_id'])
                await context.bot.send_message(
                    chat_id=tod_game['group_id'],
                    text=f"Warning: {user.user.mention_html()}, you have 2 minutes left to provide proof for your {tod_game['type']}!",
                    parse_mode='HTML'
                )
                active_tod_games[tod_game_id]['warning_sent'] = True
                await save_active_tod_games(active_tod_games)
            except Exception as e:
                logger.error(f"Failed to send inactivity warning for ToD game {tod_game_id}: {e}")


async def generate_public_bs_board_message(context: ContextTypes.DEFAULT_TYPE, game: dict) -> str:
    """Generates the text for the public battleship board message."""
    challenger_id = game['challenger_id']
    opponent_id = game['opponent_id']

    challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
    opponent_member = await context.bot.get_chat_member(game['group_id'], opponent_id)

    challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_id, opponent_member.user.full_name, game['group_id'])

    challenger_board_text = generate_bs_board_text(game['boards'][str(challenger_id)], show_ships=False)
    opponent_board_text = generate_bs_board_text(game['boards'][str(opponent_id)], show_ships=False)

    turn_player_id = game['turn']
    turn_player_member = await context.bot.get_chat_member(game['group_id'], turn_player_id)
    turn_player_name = get_display_name(turn_player_id, turn_player_member.user.full_name, game['group_id'])

    text = (
        f"<b>Battleship!</b>\n\n"
        f"<b>{challenger_name}'s Board:</b>\n"
        f"<pre>{challenger_board_text}</pre>\n"
        f"<b>{opponent_name}'s Board:</b>\n"
        f"<pre>{opponent_board_text}</pre>\n"
        f"It's {turn_player_name}'s turn to attack."
    )
    return text

async def bs_start_game_in_group(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Announces the start of the Battleship game in the group chat and prompts the first player."""
    games_data = await load_games_data_async()
    game = games_data[game_id]

    # Generate and send the public board message
    public_board_text = await generate_public_bs_board_message(context, game)
    public_message = await send_and_track_message(
        context,
        game['group_id'],
        game_id,
        public_board_text,
        parse_mode='HTML'
    )

    # Store the message ID for later editing
    game['group_message_id'] = public_message.message_id
    games_data[game_id] = game
    await save_games_data_async(games_data)

    # Send the private turn message with attack buttons
    await bs_send_turn_message(context, game_id)

def check_bs_ship_sunk(board: list, ship_coords: list) -> bool:
    """Checks if a ship has been completely sunk."""
    return all(board[r][c] == 3 for r, c in ship_coords)

async def bs_send_turn_message(context: ContextTypes.DEFAULT_TYPE, game_id: str, message_id: int = None, chat_id: int = None):
    """Sends the private message to the current player to make their move."""
    games_data = await load_games_data_async()
    game = games_data[game_id]

    player_id_str = str(game['turn'])
    opponent_id_str = str(game['opponent_id'] if player_id_str == str(game['challenger_id']) else game['challenger_id'])

    my_board_text = generate_bs_board_text(game['boards'][player_id_str], show_ships=True)
    tracking_board_text = generate_bs_board_text(game['boards'][opponent_id_str], show_ships=False)

    # Keyboard to select a column to attack
    keyboard = [
        [InlineKeyboardButton(chr(ord('A') + c), callback_data=f"bs:col:{game_id}:{c}") for c in range(5)],
        [InlineKeyboardButton(chr(ord('A') + c), callback_data=f"bs:col:{game_id}:{c}") for c in range(5, 10)]
    ]

    text = f"<pre>YOUR BOARD:\n{my_board_text}\nOPPONENT'S BOARD:\n{tracking_board_text}</pre>\nSelect a column to attack:"

    if message_id and chat_id:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text,
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
        )
    else:
        await send_and_track_message(
            context,
            int(player_id_str),
            game_id,
            text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='HTML'
        )

async def bs_select_col_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the player selecting a column, then asks for the row."""
    query = update.callback_query
    await query.answer()

    _, _, game_id, c_str = query.data.split(':')
    c = int(c_str)

    # Keyboard to select a row to attack
    row1 = [InlineKeyboardButton(str(i + 1), callback_data=f"bs:attack:{game_id}:{i}:{c}") for i in range(5)]
    row2 = [InlineKeyboardButton(str(i + 1), callback_data=f"bs:attack:{game_id}:{i}:{c}") for i in range(5, 10)]
    back_button = [InlineKeyboardButton("« Back", callback_data=f"bs:back_to_col_select:{game_id}")]
    keyboard = [row1, row2, back_button]

    try:
        await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))
    except telegram.error.BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug(f"Ignoring 'Message is not modified' error in bs_select_col_handler: {e}")
        else:
            raise

async def bs_back_to_col_select_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the player going back from row selection to column selection."""
    query = update.callback_query
    await query.answer()

    try:
        # Data format: bs:back_to_col_select:{game_id}
        _, _, game_id = query.data.split(':')
    except (ValueError, IndexError) as e:
        logger.error(f"Error unpacking bs_back_to_col_select_handler callback data: {query.data} | Error: {e}")
        await query.edit_message_text("An error occurred. Please try again.")
        return

    games_data = await load_games_data_async()
    game = games_data.get(game_id)
    if not game:
        await query.edit_message_text("This game has expired or is no longer valid.")
        logger.warning(f"bs_back_to_col_select_handler called with invalid or expired game_id: {game_id}")
        return

    # To revert the view, we simply call the function that sends the turn message again.
    # It will edit the existing message because we provide the message_id and chat_id.
    try:
        await bs_send_turn_message(context, game_id, message_id=query.message.message_id, chat_id=query.message.chat_id)
    except telegram.error.BadRequest as e:
        if "Message is not modified" in str(e):
            logger.debug(f"Ignoring 'Message is not modified' error in bs_back_to_col_select_handler: {e}")
        else:
            raise


async def bs_attack_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the player's final attack choice."""
    query = update.callback_query
    await query.answer()

    _, _, game_id, r_str, c_str = query.data.split(':')
    await update_game_activity(game_id)
    r, c = int(r_str), int(c_str)
    user_id_str = str(query.from_user.id)

    games_data = await load_games_data_async()
    game = games_data.get(game_id)

    if not game or game.get('status') != 'active':
        await query.edit_message_text("This game is no longer active.")
        return

    if str(game.get('turn')) != user_id_str:
        await query.answer("It's not your turn!", show_alert=True)
        return

    opponent_id_str = str(game['opponent_id'] if user_id_str == str(game['challenger_id']) else game['challenger_id'])
    opponent_board = game['boards'][opponent_id_str]
    target_val = opponent_board[r][c]

    if target_val in [2, 3]:
        await query.answer("You have already fired at this location.", show_alert=True)
        return

    result_text = ""
    if target_val == 0:
        opponent_board[r][c] = 2; result_text = "It's a MISS!"
    elif target_val == 1:
        opponent_board[r][c] = 3; result_text = "It's a HIT!"
        for ship, coords in game['ships'][opponent_id_str].items():
            if (r, c) in coords and check_bs_ship_sunk(opponent_board, coords):
                result_text += f"\nYou sunk their {ship}!"
                break

    all_sunk = all(check_bs_ship_sunk(opponent_board, coords) for coords in game['ships'][opponent_id_str].values())

    if all_sunk:
        winner_name = get_display_name(int(user_id_str), query.from_user.full_name, game['group_id'])
        win_message = f"The game is over! {winner_name} has won the battle!"
        await context.bot.send_message(
            chat_id=game['group_id'],
            text=win_message,
            parse_mode='HTML'
        )
        await query.edit_message_text("You are victorious! See the group for the result.")
        await handle_game_over(context, game_id, int(user_id_str), int(opponent_id_str))
        return

    game['turn'] = int(opponent_id_str)
    await save_games_data_async(games_data) # Save the new turn and board state

    # Update the public board message
    public_board_text = await generate_public_bs_board_message(context, game)
    try:
        await context.bot.edit_message_text(
            chat_id=game['group_id'],
            message_id=game['group_message_id'],
            text=public_board_text,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Failed to edit public battleship board for game {game_id}: {e}")

    # Notify players privately
    attacker_name = get_display_name(int(user_id_str), query.from_user.full_name, game['group_id'])
    coord_name = f"{chr(ord('A')+c)}{r+1}"
    await query.edit_message_text(f"You fired at {coord_name}. {result_text}\n\nYour turn is over. The board in the group has been updated.", parse_mode='HTML')

    try:
        await context.bot.send_message(
            chat_id=int(opponent_id_str),
            text=f"{attacker_name} fired at {coord_name}. {result_text}",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.warning(f"Failed to send attack result to victim: {e}")

    # Send the next turn prompt
    await bs_send_turn_message(context, game_id)

async def bs_start_placement(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point for the battleship ship placement conversation."""
    query = update.callback_query
    await query.answer()

    *_, game_id = query.data.split(':')
    user_id = str(query.from_user.id)

    games_data = await load_games_data_async()
    game = games_data.get(game_id)
    if not game:
        await query.edit_message_text("This game no longer exists.")
        return ConversationHandler.END

    if game.get('placement_complete', {}).get(user_id):
        await query.edit_message_text("You have already placed your ships.")
        return ConversationHandler.END

    context.user_data['bs_game_id'] = game_id
    context.user_data['bs_ships_to_place'] = list(BATTLESHIP_SHIPS.keys())

    board_text = generate_bs_board_text(game['boards'][user_id])

    ship_to_place = context.user_data['bs_ships_to_place'][0]
    ship_size = BATTLESHIP_SHIPS[ship_to_place]

    text = (
        f"<pre>Your board:\n{board_text}\n"
        f"Place your {ship_to_place}: {ship_size} spaces.\n"
        "Send coordinates in the format A1 H (for horizontal) or A1 V (for vertical).</pre>"
    )
    await query.edit_message_text(text=text, parse_mode='HTML')
    return BS_AWAITING_PLACEMENT

async def bs_handle_placement(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the user's input for placing a single ship."""
    game_id = context.user_data.get('bs_game_id')
    if not game_id: return ConversationHandler.END
    await update_game_activity(game_id)

    user_id = str(update.effective_user.id)
    games_data = await load_games_data_async()
    game = games_data[game_id]
    board = game['boards'][user_id]

    ship_name = context.user_data['bs_ships_to_place'][0]
    ship_size = BATTLESHIP_SHIPS[ship_name]

    text = update.message.text.strip().upper()
    parts = text.split()

    if len(parts) != 2:
        await send_and_track_message(context, update.effective_chat.id, game_id, "Invalid format. Please use A1 H or A1 V.")
        return BS_AWAITING_PLACEMENT

    start_coord_str, orientation = parts
    start_pos = parse_bs_coords(start_coord_str)

    if not start_pos or orientation not in ['H', 'V']:
        await send_and_track_message(context, update.effective_chat.id, game_id, "Invalid coordinate or orientation. Use A1 H or B2 V.")
        return BS_AWAITING_PLACEMENT

    r_start, c_start = start_pos
    ship_coords = []

    valid = True
    for i in range(ship_size):
        r, c = r_start, c_start
        if orientation == 'H': c += i
        else: r += i

        if not (0 <= r <= 9 and 0 <= c <= 9): valid = False; break
        if board[r][c] != 0: valid = False; break
        ship_coords.append((r, c))

    if not valid:
        await send_and_track_message(context, update.effective_chat.id, game_id, "Invalid placement: ship is out of bounds or overlaps another ship. Try again.")
        return BS_AWAITING_PLACEMENT

    for r, c in ship_coords:
        board[r][c] = 1
    game['ships'][user_id][ship_name] = ship_coords

    context.user_data['bs_ships_to_place'].pop(0)

    await save_games_data_async(games_data)
    board_text = generate_bs_board_text(board)

    if not context.user_data['bs_ships_to_place']:
        game['placement_complete'][user_id] = True
        await save_games_data_async(games_data)

        text = f"<pre>Final board:\n{board_text}\nAll ships placed! Waiting for opponent...</pre>"
        await send_and_track_message(context, update.effective_chat.id, game_id, text, parse_mode='HTML')

        opponent_id = str(game['opponent_id'] if user_id == str(game['challenger_id']) else game['challenger_id'])
        if game.get('placement_complete', {}).get(opponent_id):
            await bs_start_game_in_group(context, game_id)

        return ConversationHandler.END
    else:
        next_ship_name = context.user_data['bs_ships_to_place'][0]
        next_ship_size = BATTLESHIP_SHIPS[next_ship_name]
        text = (
            f"<pre>Your board:\n{board_text}\n"
            f"Place your {next_ship_name}: {next_ship_size} spaces. Format: A1 H or A1 V.</pre>"
        )
        await send_and_track_message(
            context,
            update.effective_chat.id,
            game_id,
            text,
            parse_mode='HTML'
        )
        return BS_AWAITING_PLACEMENT

async def bs_placement_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the ship placement conversation and aborts the game."""
    game_id = context.user_data.get('bs_game_id')
    if game_id:
        games_data = await load_games_data_async()
        if game_id in games_data:
            game = games_data[game_id]
            # Notify the other player if possible
            user_id = str(update.effective_user.id)
            other_player_id = str(game['opponent_id'] if user_id == str(game['challenger_id']) else game['challenger_id'])
            try:
                await context.bot.send_message(
                    chat_id=other_player_id,
                    text=f"{update.effective_user.full_name} has cancelled the game during ship placement."
                )
            except Exception:
                logger.warning(f"Failed to notify other player {other_player_id} of cancellation.")

            # Delete tracked messages and then the game
            await delete_tracked_messages(context, game_id)
            # The game data is deleted within delete_tracked_messages, but to be safe:
            if game_id in games_data:
                del games_data[game_id]
            await save_games_data_async(games_data)

    await update.message.reply_text("Ship placement cancelled. The game has been aborted.")
    context.user_data.clear()
    return ConversationHandler.END

# =============================
# Punishment System Storage & Helpers
# =============================
PUNISHMENTS_DATA_FILE = 'punishments.json'
PUNISHMENT_STATUS_FILE = 'punishment_status.json'
MEDIA_STAKES_FILE = 'media_stakes.json'
USER_PROFILES_FILE = 'user_profiles.json'

def load_user_profiles():
    if os.path.exists(USER_PROFILES_FILE):
        with open(USER_PROFILES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_user_profiles(data):
    with open(USER_PROFILES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def cache_user_profile(user: User):
    """Caches a user's ID and username."""
    if not user or not user.id or not user.username:
        return

    profiles = load_user_profiles()
    # Only update if the username has changed or the user is new
    if str(user.id) not in profiles or profiles[str(user.id)] != user.username:
        profiles[str(user.id)] = user.username
        save_user_profiles(profiles)
        logger.debug(f"Cached profile for user {user.id} (@{user.username})")

def load_media_stakes():
    if os.path.exists(MEDIA_STAKES_FILE):
        with open(MEDIA_STAKES_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_media_stakes(data):
    with open(MEDIA_STAKES_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_punishments_data():
    if os.path.exists(PUNISHMENTS_DATA_FILE):
        with open(PUNISHMENTS_DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_punishments_data(data):
    with open(PUNISHMENTS_DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_punishment_status_data():
    if os.path.exists(PUNISHMENT_STATUS_FILE):
        with open(PUNISHMENT_STATUS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_punishment_status_data(data):
    with open(PUNISHMENT_STATUS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_triggered_punishments_for_user(group_id, user_id) -> list:
    data = load_punishment_status_data()
    group_id = str(group_id)
    user_id = str(user_id)
    return data.get(group_id, {}).get(user_id, [])

def add_triggered_punishment_for_user(group_id, user_id, punishment_message: str):
    data = load_punishment_status_data()
    group_id = str(group_id)
    user_id = str(user_id)
    if group_id not in data:
        data[group_id] = {}
    if user_id not in data[group_id]:
        data[group_id][user_id] = []

    if punishment_message not in data[group_id][user_id]:
        data[group_id][user_id].append(punishment_message)
        save_punishment_status_data(data)
        logger.debug(f"Added triggered punishment '{punishment_message}' for user {user_id} in group {group_id}")

def remove_triggered_punishment_for_user(group_id, user_id, punishment_message: str):
    data = load_punishment_status_data()
    group_id = str(group_id)
    user_id = str(user_id)
    if group_id in data and user_id in data[group_id]:
        if punishment_message in data[group_id][user_id]:
            data[group_id][user_id].remove(punishment_message)
            save_punishment_status_data(data)
            logger.debug(f"Removed triggered punishment '{punishment_message}' for user {user_id} in group {group_id}")

# =============================
# Reward System Commands
# =============================
REWARD_STATE = 'awaiting_reward_choice'
ADDREWARD_STATE = 'awaiting_addreward_name'
ADDREWARD_COST_STATE = 'awaiting_addreward_cost'
REMOVEREWARD_STATE = 'awaiting_removereward_name'
ADDPOINTS_STATE = 'awaiting_addpoints_value'
REMOVEPOINTS_STATE = 'awaiting_removepoints_value'

@command_handler_wrapper(admin_only=False)
async def reward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /reward: Show reward list, ask user to choose, handle purchase or 'Other'.
    """
    group_id = str(update.effective_chat.id)
    rewards = get_rewards_list(group_id)
    msg = "<b>Available Rewards:</b>\n"
    for r in rewards:
        msg += f"• <b>{r['name']}</b> — {r['cost']} points\n"
    msg += "\nReply with the name of the reward you want to buy, or type /cancel to abort."
    context.user_data[REWARD_STATE] = {'group_id': group_id}
    await update.message.reply_text(msg, parse_mode='HTML')

async def conversation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles all conversation-based interactions after a command has been issued.
    This acts as a router based on the state stored in context.user_data.
    """
    # Heuristic check to see if a ConversationHandler is active.
    # Its state is stored under a tuple key, while this manual handler uses string keys.
    # If a ConversationHandler is active, we should not interfere.
    if any(isinstance(key, tuple) for key in context.user_data.keys()):
        return

    # === Add Reward Flow: Step 2 (Cost) ===
    if ADDREWARD_COST_STATE in context.user_data:
        state = context.user_data[ADDREWARD_COST_STATE]
        try:
            cost = int(update.message.text.strip())
            if cost < 0:
                raise ValueError
        except ValueError:
            await update.message.reply_text("Please reply with a valid positive integer for the cost.")
            return
        group_id = state['group_id']
        name = state['name']
        if add_reward(group_id, name, cost):
            await update.message.reply_text(f"Reward '{name}' added with cost {cost} points.")
        else:
            await update.message.reply_text(f"Could not add reward '{name}'. It may already exist or is not allowed.")
        context.user_data.pop(ADDREWARD_COST_STATE, None)
        return

    # === Add Reward Flow: Step 1 (Name) ===
    if ADDREWARD_STATE in context.user_data:
        state = context.user_data[ADDREWARD_STATE]
        name = update.message.text.strip()
        if name.lower() == "other":
            await update.message.reply_text("You cannot add the reward 'Other'.")
            context.user_data.pop(ADDREWARD_STATE, None)
            return
        state['name'] = name
        context.user_data[ADDREWARD_COST_STATE] = state
        context.user_data.pop(ADDREWARD_STATE, None)
        await update.message.reply_text(f"What is the cost (in points) for the reward '{name}'?")
        return

    # === Remove Reward Flow ===
    if REMOVEREWARD_STATE in context.user_data:
        state = context.user_data[REMOVEREWARD_STATE]
        name = update.message.text.strip()
        if name.lower() == "other":
            await update.message.reply_text("You cannot remove the reward 'Other'.")
            context.user_data.pop(REMOVEREWARD_STATE, None)
            return
        group_id = state['group_id']
        if remove_reward(group_id, name):
            await update.message.reply_text(f"Reward '{name}' removed.")
        else:
            await update.message.reply_text(f"Could not remove reward '{name}'. It may not exist or is not allowed.")
        context.user_data.pop(REMOVEREWARD_STATE, None)
        return

    # === User Reward Choice Flow ===
    if REWARD_STATE in context.user_data:
        state = context.user_data[REWARD_STATE]
        group_id = state['group_id']
        user_id = update.effective_user.id
        choice = update.message.text.strip()
        rewards = get_rewards_list(group_id)
        reward = next((r for r in rewards if r['name'].lower() == choice.lower()), None)
        if not reward:
            await update.message.reply_text("That reward does not exist. The reward selection process has been cancelled. Please start over with /reward if you wish to try again.")
            context.user_data.pop(REWARD_STATE, None)
            return
        if reward['name'].lower() == 'other':
            display_name = get_display_name(user_id, update.effective_user.full_name, group_id)
            chat_title = update.effective_chat.title

            message = f"You have selected 'Other', {display_name}. Please contact Beta or Lion to determine your reward and its cost."
            await update.message.reply_text(message, parse_mode='HTML')

            admins = await context.bot.get_chat_administrators(update.effective_chat.id)
            for admin in admins:
                try:
                    admin_message = f"The user {display_name} has selected the 'Other' reward in group {chat_title}. They will contact you to finalize the details."
                    await context.bot.send_message(
                        chat_id=admin.user.id,
                        text=admin_message,
                        parse_mode='HTML'
                    )
                except Exception:
                    pass
            context.user_data.pop(REWARD_STATE, None)
            return
        user_points = get_user_points(group_id, user_id)
        if user_points < reward['cost']:
            await update.message.reply_text(f"You do not have enough points for this reward. You have {user_points}, but it costs {reward['cost']}.")
            context.user_data.pop(REWARD_STATE, None)
            return
        await add_user_points(group_id, user_id, -reward['cost'], context)

        # Public announcement
        display_name = get_display_name(user_id, update.effective_user.full_name, group_id)
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"🎁 <b>{display_name}</b> just bought the reward: <b>{reward['name']}</b>! 🎉",
            parse_mode='HTML'
        )

        # Private message to admins
        admins = await context.bot.get_chat_administrators(update.effective_chat.id)
        for admin in admins:
            try:
                await context.bot.send_message(
                    chat_id=admin.user.id,
                    text=f"User {display_name} (ID: {user_id}) in group {update.effective_chat.title} (ID: {group_id}) just bought the reward: '{reward['name']}' for {reward['cost']} points."
                )
            except Exception:
                logger.warning(f"Failed to notify admin {admin.user.id} about reward purchase.")

        context.user_data.pop(REWARD_STATE, None)
        return

    # === Add/Remove Points Flow ===
    if ADDPOINTS_STATE in context.user_data:
        state = context.user_data[ADDPOINTS_STATE]
        try:
            value = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("Please reply with a valid integer number of points to add.")
            return
        await add_user_points(state['group_id'], state['target_id'], value, context)
        await update.message.reply_text(f"Added {value} points.")
        context.user_data.pop(ADDPOINTS_STATE, None)
        return

    if REMOVEPOINTS_STATE in context.user_data:
        state = context.user_data[REMOVEPOINTS_STATE]
        try:
            value = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("Please reply with a valid integer number of points to remove.")
            return
        await add_user_points(state['group_id'], state['target_id'], -value, context)
        await update.message.reply_text(f"Removed {value} points.")
        context.user_data.pop(REMOVEPOINTS_STATE, None)
        return

    # === Free Reward Flow ===
    if FREE_REWARD_SELECTION in context.user_data:
        state = context.user_data[FREE_REWARD_SELECTION]
        group_id = state['group_id']
        user_id = update.effective_user.id
        choice = update.message.text.strip()
        rewards = get_rewards_list(group_id)
        reward = next((r for r in rewards if r['name'].lower() == choice.lower()), None)

        if not reward:
            await update.message.reply_text("That reward does not exist. The reward selection process has been cancelled. Please try the /chance command again later.")
            context.user_data.pop(FREE_REWARD_SELECTION, None)
            return

        display_name = get_display_name(user_id, update.effective_user.full_name, group_id)
        await update.message.reply_text(f"Congratulations! You have claimed your free reward: <b>{reward['name']}</b>!", parse_mode='HTML')

        admins = await context.bot.get_chat_administrators(update.effective_chat.id)
        for admin in admins:
            try:
                await context.bot.send_message(
                    chat_id=admin.user.id,
                    text=f"User {display_name} (ID: {user_id}) in group {update.effective_chat.title} (ID: {group_id}) claimed the free reward: '{reward['name']}'."
                )
            except Exception:
                logger.warning(f"Failed to notify admin {admin.user.id} about free reward.")

        context.user_data.pop(FREE_REWARD_SELECTION, None)
        return

    # === Ask Task Flow ===
    if ASK_TASK_TARGET in context.user_data:
        state = context.user_data[ASK_TASK_TARGET]
        username = update.message.text.strip()
        if not username.startswith('@'):
            await update.message.reply_text("Please provide a valid @username.")
            return

        state['target_username'] = username
        context.user_data[ASK_TASK_DESCRIPTION] = state
        context.user_data.pop(ASK_TASK_TARGET, None)
        await update.message.reply_text("What is the simple task you want to ask of them?")
        return

    if ASK_TASK_DESCRIPTION in context.user_data:
        state = context.user_data[ASK_TASK_DESCRIPTION]
        task_description = update.message.text.strip()
        group_id = state['group_id']
        challenger_user = update.effective_user
        challenger_name = get_display_name(challenger_user.id, challenger_user.full_name, group_id)
        target_username = state['target_username']

        # Announce in group
        message = f"{challenger_name} has a task for {target_username}: {task_description}"
        await context.bot.send_message(
            chat_id=group_id,
            text=message,
            parse_mode='HTML'
        )

        await update.message.reply_text("Your task has been assigned.")
        context.user_data.pop(ASK_TASK_DESCRIPTION, None)
        return

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cancel: Cancel any pending reward selection.
    """
    if REWARD_STATE in context.user_data:
        context.user_data.pop(REWARD_STATE, None)
        await update.message.reply_text("Reward selection cancelled.")
    else:
        await update.message.reply_text("No reward selection in progress.")

@command_handler_wrapper(admin_only=True)
async def addreward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /addreward (admin only): Start add reward process
    """
    if update.effective_chat.type == "private":
        await update.message.reply_text("This command can only be used in group chats.")
        return
    context.user_data[ADDREWARD_STATE] = {'group_id': str(update.effective_chat.id)}
    await update.message.reply_text("What is the name of the reward you want to add?")

@command_handler_wrapper(admin_only=True)
async def addpunishment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /addpunishment <threshold> <message> (admin only): Adds a new punishment.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addpunishment <threshold> <message>")
        return

    try:
        threshold = int(context.args[0])
    except ValueError:
        await update.message.reply_text("Threshold must be a number.")
        return

    message = " ".join(context.args[1:])
    group_id = str(update.effective_chat.id)
    punishments_data = load_punishments_data()

    if group_id not in punishments_data:
        punishments_data[group_id] = []

    # Check for duplicates
    for p in punishments_data[group_id]:
        if p["message"].lower() == message.lower():
            await update.message.reply_text("A punishment with this message already exists.")
            return

    punishments_data[group_id].append({"threshold": threshold, "message": message})
    save_punishments_data(punishments_data)

    await update.message.reply_text(f"Punishment added: '{message}' at {threshold} points.")

@command_handler_wrapper(admin_only=True)
async def removepunishment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /removepunishment <message> (admin only): Removes a punishment.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /removepunishment <message>")
        return

    message_to_remove = " ".join(context.args)
    group_id = str(update.effective_chat.id)
    punishments_data = load_punishments_data()

    if group_id not in punishments_data:
        await update.message.reply_text("No punishments found for this group.")
        return

    initial_len = len(punishments_data[group_id])
    punishments_data[group_id] = [p for p in punishments_data[group_id] if p["message"].lower() != message_to_remove.lower()]

    if len(punishments_data[group_id]) == initial_len:
        await update.message.reply_text("Punishment not found.")
    else:
        save_punishments_data(punishments_data)
        await update.message.reply_text("Punishment removed.")

@command_handler_wrapper(admin_only=False)
async def newgame_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /newgame (as a reply): Starts a new game with the replied-to user.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    if not update.message.reply_to_message:
        await update.message.reply_text("Please use this command as a reply to the user you want to challenge.")
        return

    challenger_user = update.effective_user
    opponent_user = update.message.reply_to_message.from_user
    cache_user_profile(challenger_user)
    cache_user_profile(opponent_user)

    if challenger_user.id == opponent_user.id:
        await update.message.reply_text("You cannot challenge yourself.")
        return

    if opponent_user.id == context.bot.id:
        await update.message.reply_text("You cannot challenge me, I'm just the referee!")
        return

    games_data = await load_games_data_async()
    group_id = update.effective_chat.id
    # Concurrency Check: Only checks for active 2-player games (from games.json).
    # This does not block Truth or Dare games, which are stored in a separate file.
    for game in games_data.values():
        if game.get('group_id') == group_id and game.get('status') != 'complete':
            await update.message.reply_text("There is already an active 2-player game in this group. Please wait for it to finish.")
            return

    game_id = str(uuid.uuid4())
    games_data[game_id] = {
        "group_id": group_id,
        "challenger_id": challenger_user.id,
        "opponent_id": opponent_user.id,
        "game_type": None,
        "challenger_stake": None,
        "opponent_stake": None,
        "status": "pending_game_selection",
        "messages_to_delete": [],
        "last_activity": time.time()
    }
    await save_games_data_async(games_data)

    challenger_name = get_display_name(challenger_user.id, challenger_user.full_name, group_id)
    opponent_name = get_display_name(opponent_user.id, opponent_user.full_name, group_id)

    sent_message = await send_and_track_message(
        context,
        update.effective_chat.id,
        game_id,
        f"{challenger_name} has challenged {opponent_name}! {challenger_name}, please check your private messages to set up the game.",
        parse_mode='HTML'
    )

    try:
        keyboard = [[InlineKeyboardButton("Start Game Setup", callback_data=f"game:setup:start:{game_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await send_and_track_message(
            context,
            challenger_user.id,
            game_id,
            "Let's set up your game! Click the button below to begin.",
            reply_markup=reply_markup
        )
    except Exception:
        logger.exception(f"Failed to send private message to user {challenger_user.id}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="I couldn't send you a private message. Please make sure you have started a chat with me privately first."
        )

@command_handler_wrapper(admin_only=True)
async def loser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /loser <user> (admin only): Enacts the loser condition for the specified user.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    if not context.args:
        await update.message.reply_text("Usage: /loser <@username or user_id>")
        return

    loser_username = context.args[0]
    loser_id = await get_user_id_by_username(context, update.effective_chat.id, loser_username)
    if not loser_id:
        try:
            loser_id = int(loser_username)
        except ValueError:
            await update.message.reply_text(f"Could not find user {loser_username}.")
            return

    games_data = await load_games_data_async()

    latest_game_id = None
    for game_id, game in games_data.items():
        if str(game.get('group_id')) == str(update.effective_chat.id) and \
           game.get('status') == 'active' and \
           (str(game.get('challenger_id')) == str(loser_id) or str(game.get('opponent_id')) == str(loser_id)):
            latest_game_id = game_id

    if not latest_game_id:
        await update.message.reply_text(f"No active game found for user {loser_username}.")
        return

    game = games_data[latest_game_id]

    if str(game['challenger_id']) == str(loser_id):
        winner_id = game['opponent_id']
    else:
        winner_id = game['challenger_id']

    # Announce the loser and handle stakes/deletion via the centralized function
    await handle_game_over(context, latest_game_id, winner_id, int(loser_id))

@command_handler_wrapper(admin_only=True)
async def stopgame_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /stopgame - (Admin only) Manually stops the current game in the group. """
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command can only be used in a group.")
        return

    group_id = update.effective_chat.id
    games_data = await load_games_data_async()

    active_game_id = None
    for game_id, game in games_data.items():
        if game.get('group_id') == group_id and game.get('status') != 'complete':
            active_game_id = game_id
            break

    if not active_game_id:
        await update.message.reply_text("There is no active game in this group to stop.")
        return

    game = games_data[active_game_id]
    challenger_id = game['challenger_id']
    opponent_id = game['opponent_id']
    challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
    opponent_member = await context.bot.get_chat_member(game['group_id'], opponent_id)
    challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_id, opponent_member.user.full_name, game['group_id'])

    await context.bot.send_message(
        game['group_id'],
        f"The game between {challenger_name} and {opponent_name} has been manually stopped by an admin. No stakes were lost.",
        parse_mode='HTML'
    )

    # Clean up the game
    game['status'] = 'complete'
    await save_games_data_async(games_data)
    await delete_tracked_messages(context, active_game_id)

from datetime import datetime

@command_handler_wrapper(admin_only=False)
async def chance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /chance (3 times per day): Play a game of chance for a random outcome.
    """
    user_id = str(update.effective_user.id)
    cooldowns = load_cooldowns()
    today = datetime.utcnow().strftime('%Y-%m-%d')

    user_data = cooldowns.get(user_id, {"count": 0, "date": ""})

    if user_data["date"] == today and user_data["count"] >= 3:
        await update.message.reply_text("You have already played 3 times today. Please wait until tomorrow.")
        return

    # If it's a new day, reset the counter
    if user_data["date"] != today:
        user_data["date"] = today
        user_data["count"] = 0

    # Increment play count and save
    user_data["count"] += 1
    cooldowns[user_id] = user_data
    save_cooldowns(cooldowns)

    plays_left = 3 - user_data['count']
    await update.message.reply_text(f"You spin the wheel of fortune... (You have {plays_left} {'play' if plays_left == 1 else 'plays'} left today)")

    outcome = get_chance_outcome()
    group_id = str(update.effective_chat.id)

    if outcome == "plus_50":
        await add_user_points(group_id, user_id, 50, context)
        await update.message.reply_text("Congratulations! You won 50 points!")
    elif outcome == "minus_100":
        await add_user_points(group_id, user_id, -100, context)
        await update.message.reply_text("Ouch! You lost 100 points.")
    elif outcome == "chastity_2_days":
        await update.message.reply_text("Your fate is 2 days of chastity!")
    elif outcome == "chastity_7_days":
        await update.message.reply_text("Your fate is 7 days of chastity! Good luck.")
    elif outcome == "nothing":
        await update.message.reply_text("Nothing happened. Better luck next time!")
    elif outcome == "lose_all_points":
        points = get_user_points(group_id, user_id)
        await add_user_points(group_id, user_id, -points, context)
        await update.message.reply_text("Catastrophic failure! You lost all your points.")
    elif outcome == "double_points":
        points = get_user_points(group_id, user_id)
        await add_user_points(group_id, user_id, points, context)
        await update.message.reply_text("Jackpot! Your points have been doubled!")
    elif outcome == "free_reward":
        rewards = get_rewards_list(group_id)
        msg = "<b>You won a free reward!</b>\nChoose one of the following:\n"
        for r in rewards:
            msg += f"• <b>{r['name']}</b>\n"
        msg += "\nReply with the name of the reward you want."
        context.user_data[FREE_REWARD_SELECTION] = {'group_id': group_id}
        await update.message.reply_text(msg, parse_mode='HTML')
    elif outcome == "ask_task":
        await update.message.reply_text("You have won the right to ask a simple task from any of the other boys. Who would you like to ask? (Please provide their @username)")
        context.user_data[ASK_TASK_TARGET] = {'group_id': group_id}

@command_handler_wrapper(admin_only=True)
async def cleangames_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cleangames (admin only): Clears out completed or stale game data.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    games_data = await load_games_data_async()
    games_to_keep = {
        game_id: game for game_id, game in games_data.items()
        if game.get('status') != 'complete'
    }

    if len(games_to_keep) == len(games_data):
        await update.message.reply_text("No completed games to clean up.")
    else:
        await save_games_data_async(games_to_keep)
        await update.message.reply_text("Cleaned up completed games.")

@command_handler_wrapper(admin_only=True)
async def punishment_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /punishment (admin only): Lists all punishments for the group.
    """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    group_id = str(update.effective_chat.id)
    punishments_data = load_punishments_data()
    group_punishments = punishments_data.get(group_id, [])

    if not group_punishments:
        await update.message.reply_text("No punishments have been set for this group.")
        return

    msg = "<b>Configured Punishments:</b>\n"
    for p in sorted(group_punishments, key=lambda x: x['threshold'], reverse=True):
        msg += f"• Below <b>{p['threshold']}</b> points: <i>{p['message']}</i>\n"

    await update.message.reply_text(msg, parse_mode='HTML')

@command_handler_wrapper(admin_only=True)
async def removereward_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /removereward (admin only): Start remove reward process
    """
    context.user_data[REMOVEREWARD_STATE] = {'group_id': str(update.effective_chat.id)}
    await update.message.reply_text("What is the name of the reward you want to remove?")

@command_handler_wrapper(admin_only=True)
async def addpoints_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /addpoints <username|id> (admin only): Start add points process
    """
    group_id = str(update.effective_chat.id)
    # If used as a reply, use the replied-to user's ID
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_id = update.message.reply_to_message.from_user.id
    else:
        if not context.args:
            await update.message.reply_text("Usage: /addpoints <username|id> or reply to a user's message.")
            return
        arg = context.args[0].strip()
        # Try to resolve by ID
        target_id = None
        if arg.isdigit():
            target_id = int(arg)
        else:
            target_id = await get_user_id_by_username(context, update.effective_chat.id, arg)
            # get_chat_member with username will not work unless it's a numeric ID
    if not target_id:
        await update.message.reply_text(f"Could not resolve user. Please reply to a user's message or provide a valid user ID.")
        return
    context.user_data[ADDPOINTS_STATE] = {'group_id': group_id, 'target_id': target_id}
    await update.message.reply_text(f"How many points do you want to add to this user?")

@command_handler_wrapper(admin_only=True)
async def removepoints_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /removepoints <username|id> (admin only): Start remove points process
    """
    group_id = str(update.effective_chat.id)
    # If used as a reply, use the replied-to user's ID
    if update.message.reply_to_message and update.message.reply_to_message.from_user:
        target_id = update.message.reply_to_message.from_user.id
    else:
        if not context.args:
            await update.message.reply_text("Usage: /removepoints <username|id> or reply to a user's message.")
            return
        arg = context.args[0].strip()
        # Try to resolve by ID
        target_id = None
        if arg.isdigit():
            target_id = int(arg)
        else:
            target_id = await get_user_id_by_username(context, update.effective_chat.id, arg)
            # get_chat_member with username will not work unless it's a numeric ID
    if not target_id:
        await update.message.reply_text(f"Could not resolve user. Please reply to a user's message or provide a valid user ID.")
        return
    context.user_data[REMOVEPOINTS_STATE] = {'group_id': group_id, 'target_id': target_id}
    await update.message.reply_text(f"How many points do you want to remove from this user?")

@command_handler_wrapper(admin_only=False)
async def point_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /point (user: see own points, admin: see own or another's points)
    """
    group_id = str(update.effective_chat.id)
    user = update.effective_user
    is_admin_user = False
    if update.effective_chat.type in ["group", "supergroup"]:
        member = await context.bot.get_chat_member(update.effective_chat.id, user.id)
        is_admin_user = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]

    # Default to showing the user's own points
    target_user = user

    # Check if another user is being targeted (by reply or by argument)
    is_targeting_other = update.message.reply_to_message or context.args

    if is_targeting_other:
        if is_admin_user:
            # Admin is trying to see someone else's points
            if update.message.reply_to_message:
                target_user = update.message.reply_to_message.from_user
            else: # context.args must exist
                arg = context.args[0].strip()
                target_id = None
                if arg.isdigit():
                    target_id = int(arg)
                else:
                    target_id = await get_user_id_by_username(context, group_id, arg)

                if not target_id:
                    await update.message.reply_text(f"Could not resolve user '{arg}'.")
                    return
                try:
                    target_user = (await context.bot.get_chat_member(group_id, target_id)).user
                except Exception:
                    await update.message.reply_text(f"Could not resolve user '{arg}'.")
                    return
        else:
            # Non-admin is trying to see someone else's points
            await update.message.reply_text("Only admins can view other users' points. Showing your own points instead.")
            target_user = user # Reset to self

    # Fetch and display points for the determined target
    points = get_user_points(group_id, target_user.id)
    display_name = get_display_name(target_user.id, target_user.full_name, group_id)

    if target_user.id == user.id:
        await update.message.reply_text(f"You have {points} points.")
    else:
        await update.message.reply_text(f"{display_name} has {points} points.", parse_mode='HTML')

@command_handler_wrapper(admin_only=True)
async def top5_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /top5 (admin only): Show top 5 users by points in the group
    """
    group_id = str(update.effective_chat.id)
    data = load_points_data().get(group_id, {})
    if not data:
        await update.message.reply_text("No points data for this group yet.")
        return
    # Sort by points descending
    top5 = sorted(data.items(), key=lambda x: x[1], reverse=True)[:5]
    # Fetch usernames if possible
    lines = ["🎉 <b>Top 5 Point Leaders!</b> 🎉\n"]
    for idx, (uid, pts) in enumerate(top5, 1):
        try:
            member = await context.bot.get_chat_member(update.effective_chat.id, int(uid))
            name = get_display_name(int(uid), member.user.full_name, update.effective_chat.id)
        except Exception:
            name = f"User {uid}"
        lines.append(f"<b>{idx}.</b> <i>{name}</i> — <b>{pts} points</b> {'🏆' if idx==1 else ''}")
    msg = '\n'.join(lines)
    await update.message.reply_text(msg, parse_mode='HTML')

# =============================
# /command - List all commands
# =============================
COMMAND_MAP = {
    'start': {'is_admin': False}, 'help': {'is_admin': False},
    'command': {'is_admin': False}, 'disable': {'is_admin': True}, 'enable': {'is_admin': True}, 'addreward': {'is_admin': True},
    'removereward': {'is_admin': True}, 'addpunishment': {'is_admin': True},
    'removepunishment': {'is_admin': True}, 'punishment': {'is_admin': True},
    'newgame': {'is_admin': False}, 'loser': {'is_admin': True}, 'cleangames': {'is_admin': True}, 'stopgame': {'is_admin': True},
    'chance': {'is_admin': False}, 'reward': {'is_admin': False}, 'cancel': {'is_admin': False},
    'addpoints': {'is_admin': True}, 'removepoints': {'is_admin': True},
    'point': {'is_admin': False}, 'top5': {'is_admin': True},
    'title': {'is_admin': True}, 'removetitle': {'is_admin': True},
    'update': {'is_admin': True}, 'viewstakes': {'is_admin': True},
    'game': {'is_admin': False}, 'dareme': {'is_admin': False}, 'addtod': {'is_admin': False}, 'managetod': {'is_admin': True},
}

@command_handler_wrapper(admin_only=False)
async def command_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Dynamically lists all available commands based on user's admin status and disabled commands.
    """
    if update.effective_chat.type == "private":
        await update.message.reply_text("Please use this command in a group to see the available commands for that group.")
        return

    group_id = str(update.effective_chat.id)
    disabled_cmds = set(load_disabled_commands().get(group_id, []))

    member = await context.bot.get_chat_member(update.effective_chat.id, update.effective_user.id)
    is_admin_user = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]

    everyone_cmds = []
    admin_only_cmds = []

    # Static commands from COMMAND_MAP
    for cmd, info in sorted(COMMAND_MAP.items()):
        if cmd in ['start', 'help']:  # Don't show these in the group list
            continue

        is_disabled = cmd in disabled_cmds
        display_cmd = f"/{cmd}"
        if is_disabled:
            display_cmd += " (disabled)"

        if info['is_admin']:
            if is_admin_user:  # Admins see all admin commands
                admin_only_cmds.append(display_cmd)
        else:  # Everyone commands
            if not is_disabled:
                everyone_cmds.append(display_cmd)
            elif is_admin_user:  # Admins also see disabled everyone commands
                everyone_cmds.append(display_cmd)

    msg = '<b>Commands for everyone:</b>\n' + ('\n'.join(everyone_cmds) if everyone_cmds else 'None')
    if is_admin_user:
        msg += '\n\n<b>Commands for admins only:</b>\n' + ('\n'.join(admin_only_cmds) if admin_only_cmds else 'None')

    await update.message.reply_text(msg, parse_mode='HTML')


@command_handler_wrapper(admin_only=False)
async def game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /game: Lists available games and their rules.
    """
    game_list_text = """
<b>Available Games</b>

😈 <b>Truth or Dare</b>
- Command: `/dareme`
- Rules: Starts a solo game of Truth or Dare. You get 15 points for completing the task.
- Contributing: Anyone can add new questions using the `/addtod` command!

🎲 <b>Chance Game</b>
- Command: `/chance`
- Rules: Play a game of chance for a random outcome. You can play up to 3 times per day.

🏆 <b>Challenge Games</b>
- Command: `/newgame` (reply to a user to challenge them)
- Rules: After challenging, the bot will guide you through setting up the game type and stake in a private message.

You can choose from the following challenge games:
- <b>Dice</b>: A simple best-of-3, 5, or 9 dice rolling game.
- <b>Connect Four</b>: Try to get four of your pieces in a row, column, or diagonal.
- <b>Battleship</b>: The classic naval combat game. Place your ships and try to sink your opponent's fleet.
- <b>Tic-Tac-Toe</b>: The classic 3x3 grid game. A draw results in both players losing their stake.
    """
    await update.message.reply_text(game_list_text, parse_mode='HTML', disable_web_page_preview=True)


# =============================
# Truth or Dare Commands
# =============================
@command_handler_wrapper(admin_only=False)
async def dareme_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ /dareme - Starts a game of Truth or Dare. """
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    active_games = await load_active_tod_games()
    user_id = update.effective_user.id
    group_id = str(update.effective_chat.id)

    # Concurrency Check: Only checks for active ToD games (from active_tod_games.json).
    # This does not block 2-player games, which are stored in a separate file.
    for game in active_games.values():
        if game.get('group_id') == group_id:
            # Check if it's the same user to give a specific message
            if game.get('user_id') == user_id:
                await update.message.reply_text("You already have an active truth or dare! Please complete or refuse it first.")
            else:
                await update.message.reply_text("There is already an active Truth or Dare game in this group. Please wait for it to finish.")
            return

    keyboard = [
        [
            InlineKeyboardButton("Truth", callback_data=f'tod:choice:truth:{user_id}'),
            InlineKeyboardButton("Dare", callback_data=f'tod:choice:dare:{user_id}')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(f"{update.effective_user.mention_html()}, pick your poison:", reply_markup=reply_markup, parse_mode='HTML')


async def tod_choice_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user's choice of Truth or Dare."""
    query = update.callback_query
    await query.answer()

    try:
        _, _, choice, intended_user_id = query.data.split(':')
    except ValueError:
        logger.error(f"Could not parse tod_choice_handler callback data: {query.data}")
        return

    user = query.from_user
    if str(user.id) != intended_user_id:
        await query.answer("This is not for you!", show_alert=True)
        return

    group_id = str(query.message.chat.id)
    tod_data = await load_tod_data()
    group_data = tod_data.get(group_id, {})

    item_list = group_data.get(f'{choice}s', [])

    if not item_list:
        await query.edit_message_text(f"There are no {choice}s in the list for this group! Anyone can add some with /addtod.")
        return

    selected_item = random.choice(item_list)

    tod_game_id = str(uuid.uuid4())

    text = f"{user.mention_html()}, your {choice} is:\n\n<b>{html.escape(selected_item)}</b>"

    keyboard = [
        [
            InlineKeyboardButton("✅ I'll do it!", callback_data=f'tod:start_proof:{tod_game_id}'),
            InlineKeyboardButton("❌ Refuse", callback_data=f'tod:refuse:{tod_game_id}')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

    active_games = await load_active_tod_games()
    active_games[tod_game_id] = {
        "user_id": user.id,
        "group_id": group_id,
        "type": choice,
        "text": selected_item,
        "message_id": query.message.message_id,
        "chat_id": query.message.chat.id,
        "timestamp": time.time(),
        "status": "pending_acceptance"
    }
    await save_active_tod_games(active_games)


async def addtod_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the conversation to add a new truth or dare."""
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return ConversationHandler.END

    keyboard = [
        [
            InlineKeyboardButton("Truth", callback_data='addtod:type:truth'),
            InlineKeyboardButton("Dare", callback_data='addtod:type:dare')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("What would you like to add?", reply_markup=reply_markup)
    return CHOOSE_TOD_TYPE

async def tod_handle_type_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the user's choice of adding a truth or a dare."""
    query = update.callback_query
    await query.answer()

    _, _, choice_type = query.data.split(':')
    context.user_data['tod_add_type'] = choice_type

    await query.edit_message_text(f"Please send the {choice_type}(s) you'd like to add. You can send multiple in one message, just put each one on a new line.")

    return AWAIT_TOD_CONTENT

async def tod_handle_content_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the submission of new truths or dares."""
    add_type = context.user_data.get('tod_add_type')
    if not add_type:
        await update.message.reply_text("Something went wrong, please start over with /addtod.")
        context.user_data.clear()
        return ConversationHandler.END

    group_id = str(update.effective_chat.id)
    new_items = [item.strip() for item in update.message.text.split('\n') if item.strip()]

    if not new_items:
        await update.message.reply_text("I didn't receive any content. Please try again, or /cancel to exit.")
        return AWAIT_TOD_CONTENT

    tod_data = await load_tod_data()
    if group_id not in tod_data:
        tod_data[group_id] = {"truths": [], "dares": []}

    list_key = f"{add_type}s"
    tod_data[group_id].setdefault(list_key, []).extend(new_items)

    await save_tod_data(tod_data)

    confirmation_message = f"Successfully added {len(new_items)} new {add_type}(s)!"

    keyboard = [
        [
            InlineKeyboardButton("Add More", callback_data='addtod:more:yes'),
            InlineKeyboardButton("Done", callback_data='addtod:more:no')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(f"{confirmation_message}\n\nWould you like to add more?", reply_markup=reply_markup)

    return CHOOSE_TOD_MORE

async def tod_handle_more_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the user's choice to add more or finish."""
    query = update.callback_query
    await query.answer()

    _, _, choice = query.data.split(':')

    if choice == 'yes':
        keyboard = [
            [
                InlineKeyboardButton("Truth", callback_data='addtod:type:truth'),
                InlineKeyboardButton("Dare", callback_data='addtod:type:dare')
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("What would you like to add?", reply_markup=reply_markup)
        return CHOOSE_TOD_TYPE
    else:
        await query.edit_message_text("Great, thanks for contributing!")
        context.user_data.clear()
        return ConversationHandler.END

async def tod_add_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the /addtod conversation."""
    if 'tod_add_type' in context.user_data:
        context.user_data.clear()
        await update.message.reply_text("Process cancelled. Thanks for trying!")
    else:
        await update.message.reply_text("You are not currently adding any truths or dares.")
    return ConversationHandler.END


async def tod_refuse_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user refusing a truth or dare."""
    query = update.callback_query

    try:
        _, _, tod_game_id = query.data.split(':')
    except ValueError:
        logger.error(f"Could not parse tod_refuse_handler callback data: {query.data}")
        await query.answer("An error occurred.", show_alert=True)
        return

    active_games = await load_active_tod_games()
    game = active_games.get(tod_game_id)

    if not game:
        await query.answer("This game session has expired or is invalid.", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass # Message might be gone
        return

    if query.from_user.id != game['user_id']:
        await query.answer("This is not your dare to refuse!", show_alert=True)
        return

    await query.answer()

    group_id = game['group_id']
    user = query.from_user
    display_name = get_display_name(user.id, user.full_name, int(group_id))

    admin_data = load_admin_data()
    group_admins = {uid for uid, groups in admin_data.get('admins', {}).items() if group_id in groups}
    owner_id = admin_data.get('owner')
    if owner_id:
        group_admins.add(owner_id)

    notification_text = (
        f"🔔 <b>Refusal Notification</b> 🔔\n\n"
        f"User: {display_name}\n"
        f"Group ID: {group_id}\n"
        f"Refused {game['type']}: <i>{html.escape(game['text'])}</i>"
    )

    for admin_id in group_admins:
        try:
            await context.bot.send_message(chat_id=admin_id, text=notification_text, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"Failed to notify admin {admin_id} of dare refusal: {e}")

    original_text = query.message.text
    new_text = f"{original_text}\n\n<i>This was refused by the user.</i>"
    await query.edit_message_text(new_text, reply_markup=None, parse_mode='HTML')

    del active_games[tod_game_id]
    await save_active_tod_games(active_games)


async def tod_start_proof_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the user accepting to provide proof for a truth or dare."""
    query = update.callback_query

    try:
        _, _, tod_game_id = query.data.split(':')
    except ValueError:
        logger.error(f"Could not parse tod_start_proof_handler callback data: {query.data}")
        await query.answer("An error occurred.", show_alert=True)
        return

    active_games = await load_active_tod_games()
    game = active_games.get(tod_game_id)

    if not game:
        await query.answer("This game session has expired or is invalid.", show_alert=True)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if query.from_user.id != game['user_id']:
        await query.answer("This is not your dare to accept!", show_alert=True)
        return

    game['status'] = 'awaiting_proof'
    game['timestamp'] = time.time()
    active_games[tod_game_id] = game
    await save_active_tod_games(active_games)

    original_text = query.message.text

    proof_prompt = ""
    if game['type'] == 'truth':
        proof_prompt = "Please now send your answer as a text message or voice note."
    else:
        proof_prompt = "Please now send a photo or video as proof."

    new_text = f"{original_text}\n\n<b>Waiting for proof...</b>\n{proof_prompt}"

    await query.edit_message_text(new_text, reply_markup=None, parse_mode='HTML')
    await query.answer("Please send your proof.")


async def _create_tod_management_message(group_id: str, list_type: str, page: int = 0):
    """Helper function to generate the text and markup for the ToD management interface."""
    tod_data = await load_tod_data()
    items = tod_data.get(group_id, {}).get(list_type, [])

    if not items:
        return f"The list of {list_type} is empty for this group.", None

    ITEMS_PER_PAGE = 5
    start_index = page * ITEMS_PER_PAGE

    if start_index >= len(items) and page > 0:
        page -= 1
        start_index = page * ITEMS_PER_PAGE

    end_index = start_index + ITEMS_PER_PAGE
    paginated_items = items[start_index:end_index]

    if not paginated_items and page == 0:
        return f"The list of {list_type} is now empty.", None

    total_pages = -(-len(items) // ITEMS_PER_PAGE)
    text = f"<b>Managing {list_type.capitalize()}</b> (Page {page + 1} of {total_pages}):\n\n"
    keyboard = []
    for i, item_text in enumerate(paginated_items):
        actual_index = start_index + i
        button_text = (item_text[:20] + '...') if len(item_text) > 20 else item_text
        keyboard.append([
            InlineKeyboardButton(f"❌ {button_text}", callback_data=f'managetod:remove:{list_type}:{actual_index}:{page}')
        ])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Prev", callback_data=f'managetod:view:{list_type}:{page - 1}'))
    if end_index < len(items):
        nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data=f'managetod:view:{list_type}:{page + 1}'))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("Done", callback_data='managetod:done')])

    return text, InlineKeyboardMarkup(keyboard)

@command_handler_wrapper(admin_only=True)
async def managetod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin) /managetod - Manage the list of truths and dares for this group."""
    if update.effective_chat.type not in ["group", "supergroup"]:
        await update.message.reply_text("This command can only be used in group chats.")
        return

    keyboard = [
        [
            InlineKeyboardButton("Manage Truths", callback_data='managetod:view:truths:0'),
            InlineKeyboardButton("Manage Dares", callback_data='managetod:view:dares:0')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Please choose which list you want to manage:", reply_markup=reply_markup)

async def manage_tod_view_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin) Displays a paginated list of truths or dares with remove buttons."""
    query = update.callback_query
    await query.answer()

    parts = query.data.split(':')
    list_type = parts[2]
    page = int(parts[3]) if len(parts) > 3 else 0

    group_id = str(query.message.chat.id)

    text, reply_markup = await _create_tod_management_message(group_id, list_type, page)

    if not reply_markup:
        await query.edit_message_text(text)
    else:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def manage_tod_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin) Removes a single truth or dare and refreshes the view."""
    query = update.callback_query

    _, _, list_type, index_str, page_str = query.data.split(':')
    index_to_remove = int(index_str)
    page = int(page_str)

    group_id = str(query.message.chat.id)
    tod_data = await load_tod_data()

    items = tod_data.get(group_id, {}).get(list_type, [])

    if index_to_remove < len(items):
        removed_item = items.pop(index_to_remove)
        await save_tod_data(tod_data)
        await query.answer(f"Removed: {removed_item[:30]}...")
    else:
        await query.answer("Item not found. It might have been already removed.", show_alert=True)
        return

    text, reply_markup = await _create_tod_management_message(group_id, list_type, page)

    if not reply_markup:
        await query.edit_message_text(text)
    else:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')

async def manage_tod_done_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin) Cleans up the management message."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Management session finished.")


async def tod_handle_proof_submission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles a user's message to see if it's a valid proof for an active T-or-D game."""
    if not update.message: return

    user = update.effective_user
    chat_id = str(update.effective_chat.id)
    message = update.message

    active_games = await load_active_tod_games()

    game_to_complete_id = None
    game_details = None
    for game_id, game in active_games.items():
        if game.get('user_id') == user.id and game.get('group_id') == chat_id and game.get('status') == 'awaiting_proof':
            game_to_complete_id = game_id
            game_details = game
            break

    if not game_to_complete_id:
        return

    is_valid_proof = False
    if game_details['type'] == 'dare' and (message.photo or message.video):
        is_valid_proof = True
    elif game_details['type'] == 'truth' and (message.text or message.voice):
        is_valid_proof = True

    if is_valid_proof:
        await add_user_points(int(chat_id), user.id, 15, context)

        display_name = get_display_name(user.id, user.full_name, int(chat_id))
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🎉 {display_name} has completed their {game_details['type']} and earned 15 points! 🎉",
            parse_mode='HTML'
        )

        try:
            await context.bot.edit_message_text(
                chat_id=game_details['chat_id'],
                message_id=game_details['message_id'],
                text=f"The user completed the {game_details['type']}:\n\n<i>{html.escape(game_details['text'])}</i>",
                reply_markup=None,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"Could not edit original ToD message after proof: {e}")

        del active_games[game_to_complete_id]
        await save_active_tod_games(active_games)
    else:
        expected_proof = "a photo or video" if game_details['type'] == 'dare' else "a text message or voice note"
        await message.reply_text(f"That's not the right kind of proof for a {game_details['type']}. Please send {expected_proof}.")


# Persistent storage for disabled commands per group
DISABLED_COMMANDS_FILE = 'disabled_commands.json'

def load_disabled_commands():
    if os.path.exists(DISABLED_COMMANDS_FILE):
        with open(DISABLED_COMMANDS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_disabled_commands(data):
    with open(DISABLED_COMMANDS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# /disable - Disable a static command (admin only)
@command_handler_wrapper(admin_only=True)
async def disable_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("This command can only be used in group chats.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /disable <command>")
        return

    cmd_to_disable = context.args[0].lstrip('/').lower()

    if cmd_to_disable not in COMMAND_MAP:
        await update.message.reply_text(f"No such command: /{cmd_to_disable}")
        return

    if cmd_to_disable in ['disable', 'enable']:
        await update.message.reply_text(f"You cannot disable this command.")
        return

    group_id = str(update.effective_chat.id)
    disabled_data = load_disabled_commands()

    # Use a set for efficient add/check operations
    disabled_in_group = set(disabled_data.get(group_id, []))
    disabled_in_group.add(cmd_to_disable)
    disabled_data[group_id] = list(disabled_in_group) # Convert back to list for JSON

    save_disabled_commands(disabled_data)
    await update.message.reply_text(f"Command /{cmd_to_disable} has been disabled in this group. Admins can re-enable it with /enable {cmd_to_disable}.")

# /enable - Enable a static command (admin only)
@command_handler_wrapper(admin_only=True)
async def enable_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type == "private":
        await update.message.reply_text("This command can only be used in group chats.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /enable <command>")
        return

    cmd_to_enable = context.args[0].lstrip('/').lower()

    if cmd_to_enable not in COMMAND_MAP:
        await update.message.reply_text(f"No such command: /{cmd_to_enable}")
        return

    group_id = str(update.effective_chat.id)
    disabled_data = load_disabled_commands()

    if group_id not in disabled_data:
        await update.message.reply_text(f"Command /{cmd_to_enable} is not disabled.")
        return

    # Use a set for efficient operations
    disabled_in_group = set(disabled_data.get(group_id, []))

    if cmd_to_enable in disabled_in_group:
        disabled_in_group.remove(cmd_to_enable)
        disabled_data[group_id] = list(disabled_in_group)
        save_disabled_commands(disabled_data)
        await update.message.reply_text(f"Command /{cmd_to_enable} has been enabled.")
    else:
        await update.message.reply_text(f"Command /{cmd_to_enable} is not disabled.")

#Start command
@command_handler_wrapper(admin_only=False)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command with a detailed welcome message."""
    if context.args and context.args[0].startswith('setstake_'):
        return  # This is handled by the game setup conversation handler

    start_message = (
        "Hello! I am GameBot. I'm here to help manage games, points, and rewards in your group.\n\n"
        "Here are some commands to get you started:\n"
        "- `/help`: Shows a detailed, interactive help menu.\n"
        "- `/command`: Lists all available commands for the group you're in.\n\n"
        "If you have any suggestions or want your own version of this bot, please contact the developer: @BeansOfBeano."
    )

    # In a group, just give a prompt to start a private chat.
    if update.effective_chat.type != "private":
        await update.message.reply_text("Please message me in private to use /start.")
        try:
            # Also send the welcome message privately if possible
            await context.bot.send_message(
                chat_id=update.effective_user.id,
                text=start_message
            )
        except Exception:
            # Silently fail if user has not started a chat with the bot
            pass
        return

    # In a private chat, send the full welcome message.
    await update.message.reply_text(start_message)

#Help command
@command_handler_wrapper(admin_only=False)
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Shows the interactive help menu.
    """
    if update.effective_chat.type != "private":
        await update.message.reply_text("Please use the /help command in a private chat with me for a better experience.")
        return

    user_id = update.effective_user.id
    keyboard = [
        [InlineKeyboardButton("General Commands", callback_data='help_general')],
        [InlineKeyboardButton("Game Commands", callback_data='help_games')],
        [InlineKeyboardButton("Point System", callback_data='help_points')],
    ]
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("Admin Commands", callback_data='help_admin')])

    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "Welcome to the help menu! Please choose a category:",
        reply_markup=reply_markup
    )

async def help_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all interactions with the interactive help menu."""
    query = update.callback_query
    await query.answer()

    topic = query.data
    user_id = query.from_user.id

    text = ""
    keyboard = [[InlineKeyboardButton("« Back to Main Menu", callback_data='help_back')]]

    if topic == 'help_general':
        text = """
<b>General Commands</b>
- /help: Shows this help menu.
- /command: Lists all available commands in the current group.
- /game: Shows a list of available games and their rules.
        """
    elif topic == 'help_games':
        text = """
<b>Game Commands</b>
- /newgame (reply to user): Challenge someone to a game of Dice, Connect Four, Battleship, or Tic-Tac-Toe.
- /chance: Play a daily game of chance for points or other outcomes.
        """
    elif topic == 'help_points':
        text = """
<b>Point & Reward System</b>
- /point: Check your own points.
- /reward: View and buy available rewards with your points.
        """
    elif topic == 'help_admin':
        if not is_admin(user_id):
            await query.answer("You are not authorized to view this section.", show_alert=True)
            return
        text = """
<b>Admin Commands</b>

- /title &lt;user_id&gt; &lt;title&gt;: Sets a custom title for a user.
- /removetitle &lt;user_id&gt;: Removes a title from a user.
- /update: (Owner only, in group) Adds all admins from the current group to the bot's global admin list.
- /removeadmin &lt;user_id&gt;: (Owner only) Removes a user from the bot's global admin list.
- /viewstakes &lt;user_id or @username&gt;: (Private chat only) View all media staked by a user.
- /loser &lt;user_id&gt;: Declare a user as the loser of the current game.
- /cleangames: Cleans up old, completed game data.
- /top5: See the top 5 users with the most points.
- /addpoints &lt;user_id&gt; &lt;amount&gt;: Add points to a user.
- /removepoints &lt;user_id&gt; &lt;amount&gt;: Remove points from a user.
- /addreward &lt;name&gt; &lt;cost&gt;: Add a new reward to the group's shop.
- /removereward &lt;name&gt;: Remove a reward from the shop.
- /addpunishment &lt;threshold&gt; &lt;message&gt;: Add a punishment for falling below a point threshold.
- /removepunishment &lt;message&gt;: Remove a punishment.
- /punishment: List all configured punishments for the group.
        """
    elif topic == 'help_back':
        main_menu_keyboard = [
            [InlineKeyboardButton("General Commands", callback_data='help_general')],
            [InlineKeyboardButton("Game Commands", callback_data='help_games')],
            [InlineKeyboardButton("Point System", callback_data='help_points')],
        ]
        if is_admin(user_id):
            main_menu_keyboard.append([InlineKeyboardButton("Admin Commands", callback_data='help_admin')])

        await query.edit_message_text(
            "Welcome to the help menu! Please choose a category:",
            reply_markup=InlineKeyboardMarkup(main_menu_keyboard)
        )
        return

    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML', disable_web_page_preview=True)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a telegram message to notify the developer."""
    # Log the error before we do anything else, so we can see it even if something breaks.
    logger.error("Exception while handling an update:", exc_info=context.error)

    # traceback.format_exception returns the usual python message about an exception, but as a
    # list of strings rather than a single string, so we have to join them together.
    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)

    # Build the message with some markup and additional information about what happened.
    # You might need to add some logic to deal with messages longer than the 4096 character limit.
    update_str = update.to_dict() if isinstance(update, Update) else str(update)
    message = (
        f"An exception was raised while handling an update\n"
        f"<pre>update = {html.escape(json.dumps(update_str, indent=2, ensure_ascii=False))}</pre>\n\n"
        f"<pre>context.chat_data = {html.escape(str(context.chat_data))}</pre>\n\n"
        f"<pre>context.user_data = {html.escape(str(context.user_data))}</pre>\n\n"
        f"<pre>{html.escape(tb_string)}</pre>"
    )

    logger.error(message)


# =============================
# Game Setup Conversation
# =============================
GAME_SELECTION, ROUND_SELECTION, STAKE_TYPE_SELECTION, STAKE_SUBMISSION_POINTS, STAKE_SUBMISSION_MEDIA, OPPONENT_SELECTION, CONFIRMATION, FREE_REWARD_SELECTION, ASK_TASK_TARGET, ASK_TASK_DESCRIPTION = range(10)
CHOOSE_TOD_TYPE, AWAIT_TOD_CONTENT, CHOOSE_TOD_MORE = range(10, 13)


async def start_game_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Starts the game setup conversation."""
    query = update.callback_query
    await query.answer()
    *_, game_id = query.data.split(':')
    context.user_data['game_id'] = game_id

    keyboard = [
        [InlineKeyboardButton("Dice Game", callback_data=f'game:dice:{game_id}')],
        [InlineKeyboardButton("Connect Four", callback_data=f'game:connect_four:{game_id}')],
        [InlineKeyboardButton("Battleship", callback_data=f'game:battleship:{game_id}')],
        [InlineKeyboardButton("Tic-Tac-Toe", callback_data=f'game:tictactoe:{game_id}')],
        [InlineKeyboardButton("Cancel", callback_data=f'cancel_game:{game_id}')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        text="Please select the game you want to play:",
        reply_markup=reply_markup
    )
    return GAME_SELECTION

async def game_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the game selection."""
    query = update.callback_query
    await query.answer()
    _, game_type, game_id = query.data.split(':')
    await update_game_activity(game_id)
    games_data = await load_games_data_async()
    games_data[game_id]['game_type'] = game_type

    if game_type == 'connect_four':
        # Initialize Connect Four board (6 rows, 7 columns)
        games_data[game_id]['board'] = [[0 for _ in range(7)] for _ in range(6)]
        # Challenger goes first
        games_data[game_id]['turn'] = games_data[game_id]['challenger_id']
    elif game_type == 'tictactoe':
        # Initialize Tic-Tac-Toe board (3x3)
        games_data[game_id]['board'] = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
        # Challenger (X) goes first
        games_data[game_id]['turn'] = games_data[game_id]['challenger_id']

    await save_games_data_async(games_data)

    if game_type == 'dice':
        keyboard = [
            [InlineKeyboardButton("Best of 3", callback_data=f'rounds:3:{game_id}')],
            [InlineKeyboardButton("Best of 5", callback_data=f'rounds:5:{game_id}')],
            [InlineKeyboardButton("Best of 9", callback_data=f'rounds:9:{game_id}')],
            [InlineKeyboardButton("Cancel", callback_data=f'cancel_game:{game_id}')],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text="How many rounds would you like to play?",
            reply_markup=reply_markup
        )
        return ROUND_SELECTION
    else:
        # Placeholder for other games
        keyboard = [
            [InlineKeyboardButton("Points", callback_data=f'stake:points:{game_id}')],
            [InlineKeyboardButton("Media (Photo, Video, Voice Note)", callback_data=f'stake:media:{game_id}')],
            [InlineKeyboardButton("Cancel", callback_data=f'cancel_game:{game_id}')],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            text="What would you like to stake?",
            reply_markup=reply_markup
        )
        return STAKE_TYPE_SELECTION

async def round_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the round selection for the Dice Game."""
    query = update.callback_query
    await query.answer()
    _, rounds_str, game_id = query.data.split(':')
    await update_game_activity(game_id)
    rounds = int(rounds_str)

    context.user_data['game_id'] = game_id
    games_data = await load_games_data_async()
    games_data[game_id]['rounds_to_play'] = rounds
    await save_games_data_async(games_data)

    keyboard = [
        [InlineKeyboardButton("Points", callback_data=f'stake:points:{game_id}')],
        [InlineKeyboardButton("Media (Photo, Video, Voice Note)", callback_data=f'stake:media:{game_id}')],
        [InlineKeyboardButton("Cancel", callback_data=f'cancel_game:{game_id}')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        text="What would you like to stake?",
        reply_markup=reply_markup
    )
    return STAKE_TYPE_SELECTION

async def stake_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the stake type selection."""
    query = update.callback_query
    await query.answer()
    _, stake_type, game_id = query.data.split(':')
    context.user_data['game_id'] = game_id

    if stake_type == 'points':
        await query.edit_message_text(text="How many points would you like to stake?")
        return STAKE_SUBMISSION_POINTS
    elif stake_type == 'media':
        await query.edit_message_text(text="Please send the media file you would like to stake (photo, video, or voice note).")
        return STAKE_SUBMISSION_MEDIA

async def stake_submission_points(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the submission of points as a stake."""
    logger.debug("In stake_submission_points")
    try:
        points = int(update.message.text)
        user_id = update.effective_user.id
        game_id = context.user_data['game_id']
        await update_game_activity(game_id)
        games_data = await load_games_data_async()
        group_id = games_data[game_id]['group_id']

        user_points = get_user_points(group_id, user_id)

        if points <= 0:
            await send_and_track_message(
                context,
                update.effective_chat.id,
                game_id,
                "You must stake a positive number of points. Please enter a valid amount."
            )
            return STAKE_SUBMISSION_POINTS

        if user_points < points:
            await send_and_track_message(
                context,
                update.effective_chat.id,
                game_id,
                f"You don't have enough points. You have {user_points}, but you tried to stake {points}. Please enter a valid amount."
            )
            return STAKE_SUBMISSION_POINTS

        if context.user_data.get('player_role') == 'opponent':
            games_data[game_id]['opponent_stake'] = {"type": "points", "value": points}
        else:
            games_data[game_id]['challenger_stake'] = {"type": "points", "value": points}
        await save_games_data_async(games_data)

        # Proceed to confirmation for both players
        return await show_confirmation(update, context)

    except ValueError:
        await send_and_track_message(context, update.effective_chat.id, context.user_data['game_id'], "Please enter a valid number of points.")
        return STAKE_SUBMISSION_POINTS

async def stake_submission_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the submission of media as a stake."""
    logger.debug("In stake_submission_media")
    message = update.message
    file_id = None
    media_type = None

    if message.photo:
        file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        file_id = message.video.file_id
        media_type = 'video'
    elif message.voice:
        file_id = message.voice.file_id
        media_type = 'voice'
    else:
        await send_and_track_message(context, update.effective_chat.id, context.user_data['game_id'], "That is not a valid media file. Please send a photo, video, or voice note.")
        return STAKE_SUBMISSION_MEDIA

    game_id = context.user_data['game_id']
    await update_game_activity(game_id)
    games_data = await load_games_data_async()

    if context.user_data.get('player_role') == 'opponent':
        games_data[game_id]['opponent_stake'] = {"type": media_type, "value": file_id}
    else:
        games_data[game_id]['challenger_stake'] = {"type": media_type, "value": file_id}
    await save_games_data_async(games_data)

    # Log the media stake
    user_id = str(update.effective_user.id)
    stakes = load_media_stakes()
    if user_id not in stakes:
        stakes[user_id] = []

    stake_info = {
        "timestamp": time.time(),
        "game_id": game_id,
        "group_id": games_data[game_id]['group_id'],
        "opponent_id": games_data[game_id]['opponent_id'] if user_id == str(games_data[game_id]['challenger_id']) else games_data[game_id]['challenger_id'],
        "media_type": media_type,
        "file_id": file_id
    }
    stakes[user_id].append(stake_info)
    save_media_stakes(stakes)

    return await show_confirmation(update, context)

async def start_game(context: ContextTypes.DEFAULT_TYPE, game_id: str):
    """Initializes and starts the game after setup is complete."""
    games_data = await load_games_data_async()
    game = games_data[game_id]

    if game['game_type'] == 'dice':
        game['current_round'] = 1
        game['challenger_score'] = 0
        game['opponent_score'] = 0
        game['last_roll'] = None

    game['status'] = 'active'
    await save_games_data_async(games_data)

    challenger = await context.bot.get_chat_member(game['group_id'], game['challenger_id'])
    opponent = await context.bot.get_chat_member(game['group_id'], game['opponent_id'])

    await send_and_track_message(
        context,
        game['group_id'],
        game_id,
        f"The game between {challenger.user.mention_html()} and {opponent.user.mention_html()} is on!",
        parse_mode='HTML'
    )

    if game['game_type'] == 'connect_four':
        challenger_member = await context.bot.get_chat_member(game['group_id'], game['challenger_id'])
        board_text, reply_markup = create_connect_four_board_markup(game['board'], game_id)
        await send_and_track_message(
            context,
            game['group_id'],
            game_id,
            f"<b>Connect Four!</b>\n\n{board_text}\nIt's {challenger_member.user.mention_html()}'s turn.",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    elif game['game_type'] == 'tictactoe':
        challenger_member = await context.bot.get_chat_member(game['group_id'], game['challenger_id'])
        reply_markup = create_tictactoe_board_markup(game['board'], game_id)
        await send_and_track_message(
            context,
            game['group_id'],
            game_id,
            f"<b>Tic-Tac-Toe!</b>\n\nIt's {challenger_member.user.mention_html()}'s turn (❌).",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    elif game['game_type'] == 'battleship':
        challenger_id = str(game['challenger_id'])
        opponent_id = str(game['opponent_id'])
        game['boards'] = {
            challenger_id: [[0] * 10 for _ in range(10)],
            opponent_id: [[0] * 10 for _ in range(10)]
        }
        game['ships'] = {challenger_id: {}, opponent_id: {}}
        game['placement_complete'] = {challenger_id: False, opponent_id: False}
        game['turn'] = game['challenger_id']
        await save_games_data_async(games_data)

        placement_keyboard = [[InlineKeyboardButton("Begin Ship Placement", callback_data=f'bs:placement:start:{game_id}')]]
        placement_markup = InlineKeyboardMarkup(placement_keyboard)
        try:
            await context.bot.send_message(
                chat_id=game['challenger_id'],
                text="Your Battleship game is ready! It's time to place your ships.",
                reply_markup=placement_markup
            )
            await context.bot.send_message(
                chat_id=game['opponent_id'],
                text="Your Battleship game is ready! It's time to place your ships.",
                reply_markup=placement_markup
            )
        except Exception:
            logger.exception("Error sending battleship placement message")

async def show_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Shows the confirmation message."""
    game_id = context.user_data['game_id']
    games_data = await load_games_data_async()
    game = games_data[game_id]

    stake_dict = game.get('opponent_stake') if context.user_data.get('player_role') == 'opponent' else game.get('challenger_stake')

    stake_display_text = ""
    if stake_dict['type'] == 'points':
        stake_display_text = f"{stake_dict['value']} points"
    else:
        # Capitalize the first letter of the media type, e.g., 'photo' -> 'Photo'
        stake_display_text = f"a {stake_dict['type'].capitalize()}"

    opponent_member = await context.bot.get_chat_member(game['group_id'], game['opponent_id'])
    opponent_name = get_display_name(opponent_member.user.id, opponent_member.user.full_name, game['group_id'])

    confirmation_text = (
        f"<b>Game Setup Confirmation</b>\n\n"
        f"<b>Game:</b> {game['game_type']}\n"
        f"<b>Your Stake:</b> {stake_display_text}\n"
        f"<b>Opponent:</b> {opponent_name}\n\n"
        f"Is this correct?"
    )

    role = context.user_data.get('player_role', 'challenger')
    keyboard = [
        [InlineKeyboardButton("Confirm", callback_data=f'confirm_game:{role}:{game_id}')],
        [InlineKeyboardButton("Cancel", callback_data=f'cancel_game:{game_id}')],
        [InlineKeyboardButton("Restart", callback_data=f'restart_game:{game_id}')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(confirmation_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await send_and_track_message(context, update.effective_chat.id, game_id, confirmation_text, reply_markup=reply_markup, parse_mode='HTML')

    return CONFIRMATION

async def start_opponent_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Entry point for the opponent to set up their stake via callback."""
    query = update.callback_query
    await query.answer()
    *_, game_id = query.data.split(':')

    games_data = await load_games_data_async()
    game = games_data.get(game_id)

    if not game or game['opponent_id'] != query.from_user.id:
        await query.edit_message_text("This is not a valid game for you to set up.")
        return ConversationHandler.END

    context.user_data['game_id'] = game_id
    context.user_data['player_role'] = 'opponent'

    keyboard = [
        [InlineKeyboardButton("Points", callback_data=f'stake:points:{game_id}')],
        [InlineKeyboardButton("Media (Photo, Video, Voice Note)", callback_data=f'stake:media:{game_id}')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        text="What would you like to stake?",
        reply_markup=reply_markup
    )
    return STAKE_TYPE_SELECTION

async def cancel_game_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the game setup."""
    game_id = context.user_data.get('game_id')

    if update.callback_query:
        await update.callback_query.answer()
        # Use a try-except block in case the message is already gone
        try:
            await update.callback_query.edit_message_text("Game setup cancelled.")
        except telegram.error.BadRequest:
            logger.warning("Failed to edit message on game cancel, it might have been deleted already.")
    elif update.message:
        await update.message.reply_text("Game setup cancelled.")

    if game_id:
        await delete_tracked_messages(context, game_id)
        games_data = await load_games_data_async()
        # Ensure the game data is removed after cleaning messages
        if game_id in games_data:
            del games_data[game_id]
            await save_games_data_async(games_data)

    context.user_data.clear()
    return ConversationHandler.END

async def confirm_game_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Routes the confirmation based on the player's role."""
    query = update.callback_query
    await query.answer()
    _, role, game_id = query.data.split(':')
    await update_game_activity(game_id)

    if role == 'challenger':
        return await send_challenge_to_opponent(update, context, game_id)
    else:  # opponent
        await query.edit_message_text("Stake confirmed! The game will now begin in the group chat.")
        await start_game(context, game_id)
        return ConversationHandler.END

async def send_challenge_to_opponent(update: Update, context: ContextTypes.DEFAULT_TYPE, game_id: str) -> int:
    """Confirms the game setup and sends the challenge to the group."""
    query = update.callback_query

    games_data = await load_games_data_async()
    game = games_data[game_id]

    game['status'] = 'pending_opponent_acceptance'
    await save_games_data_async(games_data)

    challenger_member = await context.bot.get_chat_member(game['group_id'], game['challenger_id'])
    opponent_member = await context.bot.get_chat_member(game['group_id'], game['opponent_id'])
    challenger_name = get_display_name(challenger_member.user.id, challenger_member.user.full_name, game['group_id'])
    opponent_name = get_display_name(opponent_member.user.id, opponent_member.user.full_name, game['group_id'])

    challenge_text = (
        f"🚨 <b>New Challenge!</b> 🚨\n\n"
        f"{challenger_name} has challenged {opponent_name} to a game of {game['game_type']}!\n\n"
        f"{opponent_name}, do you accept?"
    )

    keyboard = [
        [
            InlineKeyboardButton("Accept", callback_data=f'challenge:accept:{game_id}'),
            InlineKeyboardButton("Refuse", callback_data=f'challenge:refuse:{game_id}'),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await send_and_track_message(
        context,
        game['group_id'],
        game_id,
        challenge_text,
        reply_markup=reply_markup,
        parse_mode='HTML'
    )

    await query.edit_message_text("Challenge has been sent!")
    return ConversationHandler.END

async def restart_game_setup(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Restarts the game setup conversation."""
    return await start_game_setup(update, context)

async def dice_roll_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles dice rolls for the Dice Game."""
    if not update.message or not update.message.dice or update.message.dice.emoji != '🎲':
        return

    user_id = update.effective_user.id
    games_data = await load_games_data_async()

    active_game_id = None
    for game_id, game in games_data.items():
        if game.get('game_type') == 'dice' and \
           game.get('status') == 'active' and \
           (game.get('challenger_id') == user_id or game.get('opponent_id') == user_id):
            active_game_id = game_id
            break

    if not active_game_id:
        return

    # Get a direct reference to the game object from the loaded data
    active_game = games_data[active_game_id]

    await update_game_activity(active_game_id)

    # Track the user's dice message for deletion
    active_game.setdefault('messages_to_delete', []).append({
        'chat_id': update.effective_chat.id,
        'message_id': update.message.message_id
    })

    last_roll = active_game.get('last_roll')

    if not last_roll: # First roll of a round
        active_game['last_roll'] = {'user_id': user_id, 'value': update.message.dice.value}
        other_player_id = active_game['challenger_id'] if user_id == active_game['opponent_id'] else active_game['opponent_id']
        other_player_member = await context.bot.get_chat_member(active_game['group_id'], other_player_id)
        other_player_name = get_display_name(other_player_id, other_player_member.user.full_name, active_game['group_id'])
        await send_and_track_message(context, update.effective_chat.id, active_game_id, f"You rolled a {update.message.dice.value}. Waiting for {other_player_name} to roll.", parse_mode='HTML')
        await save_games_data_async(games_data) # Save the updated game data
        return

    if last_roll['user_id'] == user_id:
        await send_and_track_message(context, update.effective_chat.id, active_game_id, "It's not your turn to roll.")
        return

    # Second roll of a round, determine winner
    player1_id = last_roll['user_id']
    player2_id = user_id
    player1_roll = last_roll['value']
    player2_roll = update.message.dice.value

    if player1_roll > player2_roll:
        winner_id = player1_id
    elif player2_roll > player1_roll:
        winner_id = player2_id
    else: # Tie
        await send_and_track_message(context, update.effective_chat.id, active_game_id, f"You both rolled a {player1_roll}. It's a tie! Roll again.")
        active_game['last_roll'] = None # Reset for re-roll
        await save_games_data_async(games_data)
        return

    # Update scores
    if winner_id == active_game['challenger_id']:
        active_game['challenger_score'] += 1
    else:
        active_game['opponent_score'] += 1

    winner_member = await context.bot.get_chat_member(active_game['group_id'], winner_id)
    winner_name = get_display_name(winner_id, winner_member.user.full_name, active_game['group_id'])
    win_message = f"{winner_name} wins round {active_game['current_round']}!\n" \
                  f"Score: {active_game['challenger_score']} - {active_game['opponent_score']}"
    await send_and_track_message(
        context,
        active_game['group_id'],
        active_game_id,
        win_message,
        parse_mode='HTML'
    )

    # Check for game over
    rounds_to_win = (active_game['rounds_to_play'] // 2) + 1
    if active_game['challenger_score'] >= rounds_to_win or active_game['opponent_score'] >= rounds_to_win:
        # Game over
        if active_game['challenger_score'] > active_game['opponent_score']:
            winner_id = active_game['challenger_id']
            loser_id = active_game['opponent_id']
        else:
            winner_id = active_game['opponent_id']
            loser_id = active_game['challenger_id']

        await save_games_data_async(games_data) # Save final state before game over
        await handle_game_over(context, active_game_id, winner_id, loser_id)
        return
    else:
        # Next round
        active_game['current_round'] += 1
        active_game['last_roll'] = None
        await send_and_track_message(
            context,
            active_game['group_id'],
            active_game_id,
            f"Round {active_game['current_round']}! It's anyone's turn to roll."
        )

    await save_games_data_async(games_data)

async def challenge_response_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the opponent's response to a game challenge."""
    query = update.callback_query
    await query.answer()

    _, response_type, game_id = query.data.split(':')
    await update_game_activity(game_id)

    games_data = await load_games_data_async()
    game = games_data.get(game_id)

    if not game:
        await query.edit_message_text("This game challenge is no longer valid.")
        return

    user_id = update.effective_user.id
    if user_id != game['opponent_id']:
        await query.answer("This challenge is not for you.", show_alert=True)
        return

    if response_type == 'accept':
        game['status'] = 'pending_opponent_stake'
        await save_games_data_async(games_data)

        await query.edit_message_text("Challenge accepted! Please check your private messages to set up your stake.")

        keyboard = [[InlineKeyboardButton("Set your stakes", callback_data=f"game:setup:opponent:{game_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        try:
            await send_and_track_message(
                context,
                user_id,
                game_id,
                "You have accepted the challenge! Click the button below to set up your stake.",
                reply_markup=reply_markup
            )
        except telegram.error.Forbidden:
            opponent_member = await context.bot.get_chat_member(game['group_id'], user_id)
            bot_username = context.bot.username
            await context.bot.send_message(
                chat_id=game['group_id'],
                text=f'{opponent_member.user.mention_html()}, I can\'t send you a private message because you haven\'t started a chat with me. '
                     f'Please <a href="https://t.me/{bot_username}">start a chat with me</a> and then click \'Accept\' on the challenge again.',
                parse_mode='HTML'
            )

    elif response_type == 'refuse':
        if game.get('is_revenge'):
            # --- Revenge Refusal Logic ---
            old_game_id = game.get('old_game_id')
            if not old_game_id or old_game_id not in games_data:
                # Fallback if old game data is missing
                refuser_name = get_display_name(game['opponent_id'], update.effective_user.full_name, game['group_id'])
                await context.bot.send_message(
                    chat_id=game['group_id'],
                    text=f"{refuser_name} has refused the revenge match, but the original game data was not found.",
                    parse_mode='HTML'
                )
            else:
                old_game = games_data[old_game_id]
                refuser_id = old_game['winner_id']
                challenger_id = old_game['loser_id']

                # Determine the refuser's stake from the old game
                refuser_stake = old_game.get('challenger_stake') if str(old_game['challenger_id']) == str(refuser_id) else old_game.get('opponent_stake')

                refuser_member = await context.bot.get_chat_member(game['group_id'], refuser_id)
                challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
                refuser_name = get_display_name(refuser_id, refuser_member.user.full_name, game['group_id'])
                challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])

                if refuser_stake:
                    if refuser_stake['type'] == 'points':
                        points_val = refuser_stake['value']
                        await add_user_points(game['group_id'], challenger_id, points_val, context)
                        await add_user_points(game['group_id'], refuser_id, -points_val, context)
                        await context.bot.send_message(
                            game['group_id'],
                            f"{refuser_name} has refused the revenge match and forfeited {points_val} points to {challenger_name}!",
                            parse_mode='HTML'
                        )
                    else: # media
                        media_type = refuser_stake['type']
                        file_id = refuser_stake['value']
                        caption = f"{refuser_name} has refused the revenge match and forfeited their stake to {challenger_name}!"
                        if media_type == 'photo':
                            await context.bot.send_photo(game['group_id'], file_id, caption=caption, parse_mode='HTML')
                        elif media_type == 'video':
                            await context.bot.send_video(game['group_id'], file_id, caption=caption, parse_mode='HTML')
                        elif media_type == 'voice':
                            await context.bot.send_voice(game['group_id'], file_id, caption=caption, parse_mode='HTML')
                else:
                     await context.bot.send_message(
                        game['group_id'],
                        f"{refuser_name} has refused the revenge match, but no stake was found in the original game.",
                        parse_mode='HTML'
                    )

            # Clean up the revenge game
            del games_data[game_id]
            await save_games_data_async(games_data)
            await query.edit_message_text("Revenge challenge refused.")
            return

        challenger_id = game['challenger_id']
        challenger_stake = game['challenger_stake']

        challenger_member = await context.bot.get_chat_member(game['group_id'], challenger_id)
        challenger_name = get_display_name(challenger_id, challenger_member.user.full_name, game['group_id'])

        await context.bot.send_message(
            chat_id=challenger_id,
            text=f"Your challenge was refused by {get_display_name(update.effective_user.id, update.effective_user.full_name, game['group_id'])}.",
            parse_mode='HTML'
        )

        if challenger_stake['type'] == 'points':
            await add_user_points(game['group_id'], challenger_id, -challenger_stake['value'], context)
            message = f"{challenger_name.capitalize()} is a loser for being refused! They lost {challenger_stake['value']} points."
            if 'fag' in challenger_name:
                message = f"The {challenger_name} is a loser for being refused! They lost {challenger_stake['value']} points."
            await context.bot.send_message(
                game['group_id'],
                message,
                parse_mode='HTML'
            )
        else:
            caption = f"{challenger_name.capitalize()} is a loser for being refused! This was their stake."
            if 'fag' in challenger_name:
                caption = f"The {challenger_name} is a loser for being refused! This was their stake."
            if challenger_stake['type'] == 'photo':
                await context.bot.send_photo(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')
            elif challenger_stake['type'] == 'video':
                await context.bot.send_video(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')
            elif challenger_stake['type'] == 'voice':
                await context.bot.send_voice(game['group_id'], challenger_stake['value'], caption=caption, parse_mode='HTML')

        del games_data[game_id]
        await save_games_data_async(games_data)

        await query.edit_message_text("Challenge refused.")

# =============================
# Unknown Command Handler
# =============================
async def unknown_command_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles any message that looks like a command but is not registered.
    This is a catch-all to prevent the bot from replying to unknown commands,
    especially those intended for other bots in a group.
    """
    # In groups, bots often see commands for other bots. We should ignore them.
    if update.message and (update.message.text.startswith('/') or update.message.text.startswith('.') or update.message.text.startswith('!')):
        # Basic check: if a command includes '@' but not our bot's name, ignore it.
        if '@' in update.message.text and BOT_USERNAME.lstrip('@') not in update.message.text:
            logger.info(f"Ignoring command intended for another bot: {update.message.text}")
            return

        # Otherwise, it might be a command for us that we don't recognize.
        # We will just silently ignore it to prevent noise.
        logger.info(f"Ignoring unknown command: {update.message.text}")


# =============================
# Command Registration Helper
# =============================
def add_command(app: Application, command: str, handler):
    """
    Registers a command with support for /, ., and ! prefixes.
    """
    # Wrapper for MessageHandlers to populate context.args
    async def message_handler_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.message and update.message.text:
            # Reconstruct the command without the prefix and bot name for arg parsing
            text = update.message.text
            if text.startswith('.') or text.startswith('!'):
                parts = text.split()
                # Basic split, assumes command is the first part
                context.args = parts[1:]
            else:
                context.args = update.message.text.split()[1:]

        await handler(update, context)

    # The bot's username without the '@'
    bot_username_without_at = BOT_USERNAME.lstrip('@')

    # Register for /<command> - uses the original handler as it populates args automatically
    # CommandHandler is smart enough to handle /command@botname
    app.add_handler(CommandHandler(command, handler))

    # Register for .<command> and !<command> - uses the wrapper with improved regex
    # This regex now optionally matches @botname
    dot_regex = rf'^\.{command}(?:@{bot_username_without_at})?(\s|$)'
    app.add_handler(MessageHandler(filters.Regex(dot_regex), message_handler_wrapper))

    bang_regex = rf'^!{command}(?:@{bot_username_without_at})?(\s|$)'
    app.add_handler(MessageHandler(filters.Regex(bang_regex), message_handler_wrapper))


async def post_init(application: Application) -> None:
    """Post initialization function for the application."""
    context = CallbackContext(application=application)
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_game_inactivity, 'interval', minutes=1, args=[context])
    scheduler.start()


if __name__ == '__main__':
    logger.info('Starting Telegram Bot...')
    logger.debug(f'TOKEN value: {TOKEN}')

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    #Commands
    # Register all commands using the new helper
    add_command(app, 'start', start_command)
    add_command(app, 'help', help_command)
    add_command(app, 'game', game_command)
    add_command(app, 'command', command_list_command)
    add_command(app, 'disable', disable_command)
    add_command(app, 'enable', enable_command)
    add_command(app, 'addreward', addreward_command)
    add_command(app, 'removereward', removereward_command)
    add_command(app, 'addpunishment', addpunishment_command)
    add_command(app, 'removepunishment', removepunishment_command)
    add_command(app, 'punishment', punishment_command)
    add_command(app, 'newgame', newgame_command)
    add_command(app, 'loser', loser_command)
    add_command(app, 'stopgame', stopgame_command)
    add_command(app, 'cleangames', cleangames_command)
    add_command(app, 'chance', chance_command)
    add_command(app, 'reward', reward_command)
    add_command(app, 'cancel', cancel_command)
    add_command(app, 'addpoints', addpoints_command)
    add_command(app, 'removepoints', removepoints_command)
    add_command(app, 'point', point_command)
    add_command(app, 'top5', top5_command)
    add_command(app, 'title', title_command)
    add_command(app, 'removetitle', removetitle_command)
    add_command(app, 'update', update_command)
    add_command(app, 'viewstakes', viewstakes_command)
    add_command(app, 'dareme', dareme_command)
    add_command(app, 'managetod', managetod_command)

    # Add the conversation handler with a high priority
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, conversation_handler), group=-1)

    # Handler for truth/dare proof submission
    # Note: The parentheses around the first filter group are crucial to ensure correct operator precedence.
    app.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.VIDEO | filters.VOICE) & ~filters.COMMAND, tod_handle_proof_submission), group=0)

    # Add the unknown command handler with a low priority to catch anything not handled yet
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command_handler), group=1)

    # Separate conversation handlers for challenger and opponent to avoid state conflicts.
    shared_game_setup_states = {
        GAME_SELECTION: [CallbackQueryHandler(game_selection, pattern=r'^game:(dice|connect_four|battleship|tictactoe):.*')],
        ROUND_SELECTION: [CallbackQueryHandler(round_selection, pattern=r'^rounds:\d+:.*')],
        STAKE_TYPE_SELECTION: [CallbackQueryHandler(stake_type_selection, pattern=r'^stake:(points|media):.*')],
        STAKE_SUBMISSION_POINTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, stake_submission_points)],
        STAKE_SUBMISSION_MEDIA: [MessageHandler(filters.ATTACHMENT, stake_submission_media)],
        CONFIRMATION: [
            CallbackQueryHandler(confirm_game_setup, pattern=r'^confirm_game:.*'),
            CallbackQueryHandler(restart_game_setup, pattern='^restart_game:.*'),
            CallbackQueryHandler(cancel_game_setup, pattern='^cancel_game:.*'),
        ],
    }
    shared_game_setup_fallbacks = [
        CallbackQueryHandler(cancel_game_setup, pattern='^cancel_game:.*'),
        CommandHandler('cancel', cancel_game_setup)
    ]

    challenger_game_setup_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_game_setup, pattern=r'^game:setup:start:.*')],
        states=shared_game_setup_states,
        fallbacks=shared_game_setup_fallbacks,
        per_user=True,
        per_chat=False,
    )

    opponent_game_setup_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_opponent_setup, pattern=r'^game:setup:opponent:.*')],
        states=shared_game_setup_states,
        fallbacks=shared_game_setup_fallbacks,
        per_user=True,
        per_chat=False,
    )

    # Battleship placement handler
    battleship_placement_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(bs_start_placement, pattern=r'^bs:placement:start:.*')],
        states={
            BS_AWAITING_PLACEMENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, bs_handle_placement)],
        },
        fallbacks=[CommandHandler('cancel', bs_placement_cancel)],
        conversation_timeout=600  # 10 minutes to place all ships
    )
    app.add_handler(battleship_placement_handler)

    # Truth or Dare add handler
    bot_username_without_at = BOT_USERNAME.lstrip('@')
    add_tod_handler = ConversationHandler(
        entry_points=[
            CommandHandler('addtod', addtod_command),
            MessageHandler(filters.Regex(rf'^\.addtod(?:@{bot_username_without_at})?(\s|$)'), addtod_command),
            MessageHandler(filters.Regex(rf'^!addtod(?:@{bot_username_without_at})?(\s|$)'), addtod_command),
        ],
        states={
            CHOOSE_TOD_TYPE: [CallbackQueryHandler(tod_handle_type_choice, pattern=r'^addtod:type:.*')],
            AWAIT_TOD_CONTENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, tod_handle_content_submission)],
            CHOOSE_TOD_MORE: [CallbackQueryHandler(tod_handle_more_choice, pattern=r'^addtod:more:.*')],
        },
        fallbacks=[CommandHandler('cancel', tod_add_cancel)],
        per_user=True,
        per_chat=True,
    )
    app.add_handler(add_tod_handler)

    app.add_handler(challenger_game_setup_handler)
    app.add_handler(opponent_game_setup_handler)
    app.add_handler(CallbackQueryHandler(challenge_response_handler, pattern=r'^challenge:(accept|refuse):.*'))
    app.add_handler(CallbackQueryHandler(revenge_handler, pattern=r'^game:revenge:.*'))
    app.add_handler(CallbackQueryHandler(connect_four_move_handler, pattern=r'^c4:move:.*'))
    app.add_handler(CallbackQueryHandler(tictactoe_move_handler, pattern=r'^ttt:move:.*'))
    app.add_handler(CallbackQueryHandler(bs_select_col_handler, pattern=r'^bs:col:.*'))
    app.add_handler(CallbackQueryHandler(bs_back_to_col_select_handler, pattern=r'^bs:back_to_col_select:.*'))
    app.add_handler(CallbackQueryHandler(bs_attack_handler, pattern=r'^bs:attack:.*'))
    app.add_handler(CallbackQueryHandler(tod_choice_handler, pattern=r'^tod:choice:.*'))
    app.add_handler(CallbackQueryHandler(tod_refuse_handler, pattern=r'^tod:refuse:.*'))
    app.add_handler(CallbackQueryHandler(tod_start_proof_handler, pattern=r'^tod:start_proof:.*'))
    app.add_handler(CallbackQueryHandler(manage_tod_view_handler, pattern=r'^managetod:view:.*'))
    app.add_handler(CallbackQueryHandler(manage_tod_remove_handler, pattern=r'^managetod:remove:.*'))
    app.add_handler(CallbackQueryHandler(manage_tod_done_handler, pattern=r'^managetod:done'))
    app.add_handler(CallbackQueryHandler(help_menu_handler, pattern=r'^help_'))
    app.add_handler(MessageHandler(filters.Dice(), dice_roll_handler))

    # Errors
    app.add_error_handler(error_handler)

    #Check for updates
    logger.info('Polling...')
    app.run_polling(poll_interval=0.5)
