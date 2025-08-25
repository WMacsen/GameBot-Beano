import unittest
import os
import json
from unittest.mock import AsyncMock, MagicMock, patch

# It's important to import the Main module *after* setting up mocks if needed,
# but for now, we'll import it at the top. We might need to adjust this.
import Main

class TestBot(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        """Set up for each test."""
        # Clean up any data files before each test to ensure a clean slate.
        self.cleanup_files()
        # We can also initialize some default mock objects here if they are common across tests.

    def tearDown(self):
        """Clean up after each test."""
        self.cleanup_files()

    def cleanup_files(self):
        """Deletes all known JSON data files."""
        files_to_delete = [
            'admins.json', 'user_titles.json', 'rewards.json', 'points.json',
            'punishments.json', 'punishment_status.json', 'games.json',
            'chance_cooldowns.json', 'media_stakes.json', 'user_profiles.json',
            'negative_points_tracker.json', 'disabled_commands.json'
        ]
        for f in files_to_delete:
            if os.path.exists(f):
                os.remove(f)

    # --- Mock Object Factory ---
    def _create_mock_user(self, user_id, full_name, username=None):
        """Creates a mock User object."""
        user = MagicMock()
        user.id = user_id
        user.full_name = full_name
        user.username = username
        user.mention_html.return_value = f'<a href="tg://user?id={user_id}">{full_name}</a>'
        return user

    def _create_mock_chat(self, chat_id, chat_type="group", title="Test Group"):
        """Creates a mock Chat object."""
        chat = MagicMock()
        chat.id = chat_id
        chat.type = chat_type
        chat.title = title
        return chat

    def _create_mock_message(self, user, chat, text="", message_id=123, reply_to=None):
        """Creates a mock Message object."""
        message = MagicMock()
        message.from_user = user
        message.chat = chat
        message.chat_id = chat.id # FIX: Explicitly set chat_id for serialization
        message.text = text
        message.message_id = message_id
        message.reply_to_message = reply_to
        message.reply_text = AsyncMock()
        # For media messages
        message.photo = None
        message.video = None
        message.voice = None
        return message

    def _create_mock_update(self, message, callback_query_data=None):
        """Creates a mock Update object."""
        update = MagicMock()
        update.effective_user = message.from_user
        update.effective_chat = message.chat
        update.message = message
        if callback_query_data:
            query = MagicMock()
            query.data = callback_query_data
            query.from_user = message.from_user
            query.answer = AsyncMock()
            query.edit_message_text = AsyncMock()
            query.edit_message_reply_markup = AsyncMock()
            update.callback_query = query
        else:
            update.callback_query = None
        return update

    def _create_mock_context(self, bot=None, args=None, user_data=None, chat_data=None):
        """Creates a mock Context object."""
        context = MagicMock()
        if bot:
            context.bot = bot
        else:
            context.bot = AsyncMock()
            # Common bot methods that will be called
            context.bot.send_message = AsyncMock()
            context.bot.delete_message = AsyncMock()
            context.bot.get_chat_member = AsyncMock()
            context.bot.get_chat_administrators = AsyncMock()
            context.bot.restrict_chat_member = AsyncMock()
            context.bot.edit_message_text = AsyncMock()

        context.args = args if args is not None else []
        context.user_data = user_data if user_data is not None else {}
        context.chat_data = chat_data if chat_data is not None else {}
        return context

    def _create_mock_chat_member(self, user, status="member"):
        """Creates a mock ChatMember object."""
        chat_member = MagicMock()
        chat_member.user = user
        chat_member.status = status
        return chat_member


    # --- Test Cases ---
    async def test_title_command_as_admin_by_id(self):
        """Test that an admin can set a title for a user using their ID."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin User")
        target_user = self._create_mock_user(2, "Target User")
        chat = self._create_mock_chat(101)

        context = self._create_mock_context(args=["2", "The Great"])
        # Mock the checks inside the decorator and the command
        context.bot.get_chat_member.side_effect = [
            self._create_mock_chat_member(admin_user, status="administrator"), # Decorator check
            self._create_mock_chat_member(target_user) # Command's user retrieval
        ]

        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        # 2. Execute
        await Main.title_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with(
            f"Title for {target_user.mention_html()} has been set to 'The Great'.",
            parse_mode='HTML'
        )
        titles = Main.load_user_titles()
        self.assertEqual(titles, {"2": "The Great"})

    async def test_title_command_as_non_admin(self):
        """Test that a non-admin cannot set a title."""
        # 1. Setup
        non_admin_user = self._create_mock_user(1, "Regular User")
        target_user = self._create_mock_user(2, "Target User")
        chat = self._create_mock_chat(101)

        context = self._create_mock_context(args=["2", "The Great"])
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(non_admin_user, status="member")

        update = self._create_mock_update(self._create_mock_message(non_admin_user, chat))

        # 2. Execute
        await Main.title_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with(
            f"Warning: {non_admin_user.mention_html()}, you are not authorized to use this command.",
            parse_mode='HTML'
        )
        self.assertEqual(Main.load_user_titles(), {})

    async def test_title_command_by_reply(self):
        """Test setting a title by replying to a user's message."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin User")
        target_user = self._create_mock_user(2, "Target User")
        chat = self._create_mock_chat(101)

        context = self._create_mock_context(args=["The Wise"])
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(admin_user, status="administrator")

        replied_to_message = self._create_mock_message(target_user, chat)
        command_message = self._create_mock_message(admin_user, chat, reply_to=replied_to_message)
        update = self._create_mock_update(command_message)

        # 2. Execute
        await Main.title_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with(
            f"Title for {target_user.mention_html()} has been set to 'The Wise'.",
            parse_mode='HTML'
        )
        self.assertEqual(Main.load_user_titles(), {"2": "The Wise"})

    async def test_removetitle_command_as_admin(self):
        """Test that an admin can remove a title."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin User")
        target_user = self._create_mock_user(2, "Target User")
        chat = self._create_mock_chat(101)

        Main.save_user_titles({"2": "The Great"})

        context = self._create_mock_context(args=["2"])
        context.bot.get_chat_member.side_effect = [
            self._create_mock_chat_member(admin_user, status="administrator"),
            self._create_mock_chat_member(target_user)
        ]

        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        # 2. Execute
        await Main.removetitle_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once()
        self.assertIn("Title for Target User has been removed", update.message.reply_text.call_args[0][0])
        self.assertEqual(Main.load_user_titles(), {})

    @patch('Main.OWNER_ID', 1)
    async def test_update_command_as_owner(self):
        """Test that the owner can sync the admin list."""
        # 1. Setup
        owner_user = self._create_mock_user(1, "Owner")
        admin1 = self._create_mock_user(10, "Admin One", "admin_one")
        admin2 = self._create_mock_user(11, "Admin Two", "admin_two")
        chat = self._create_mock_chat(101)

        # Pre-load data: one old admin who will be removed, and a profile for them.
        Main.save_admin_data({'owner': '1', 'admins': {'9': ['101']}})
        Main.save_user_profiles({'9': 'OldAdmin'})

        context = self._create_mock_context()
        # FIX: The decorator needs to check the owner's status first. The status for an owner is 'creator'.
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(owner_user, status="creator")
        # Simulate the API call to get the current list of admins from Telegram
        context.bot.get_chat_administrators.return_value = [
            self._create_mock_chat_member(owner_user, status="creator"),
            self._create_mock_chat_member(admin1, status="administrator"),
            self._create_mock_chat_member(admin2, status="administrator"),
        ]

        update = self._create_mock_update(self._create_mock_message(owner_user, chat))

        # 2. Execute
        await Main.update_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once()
        reply_text = update.message.reply_text.call_args[0][0]

        # Check that the reply mentions new and removed admins
        self.assertIn("New admins added", reply_text)
        self.assertIn("Admin One", reply_text)
        self.assertIn("Admin Two", reply_text)
        self.assertIn("Admins removed from this group: OldAdmin", reply_text)

        # Check that the admin data file was updated correctly
        admin_data = Main.load_admin_data()
        self.assertEqual(admin_data['owner'], '1')
        self.assertIn('10', admin_data['admins'])
        self.assertIn('11', admin_data['admins'])
        self.assertNotIn('9', admin_data['admins'])
        self.assertEqual(admin_data['admins']['10'], ['101'])

    # --- Point & Reward System Tests ---
    async def test_point_command_for_self(self):
        """Test that a user can view their own points."""
        # 1. Setup
        user = self._create_mock_user(1, "Test User")
        chat = self._create_mock_chat(101)
        Main.save_points_data({"101": {"1": 150}})

        context = self._create_mock_context()
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(user, status="member")
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute
        await Main.point_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with("You have 150 points.")

    async def test_addpoints_command(self):
        """Test the /addpoints conversation flow."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin User")
        target_user = self._create_mock_user(2, "Target User")
        chat = self._create_mock_chat(101)
        Main.save_points_data({"101": {"2": 50}})

        # --- First update: the /addpoints command ---
        context = self._create_mock_context(args=["2"])
        # FIX: The decorator will check the admin's status first.
        context.bot.get_chat_member.side_effect = [
            self._create_mock_chat_member(admin_user, status="administrator"), # For decorator
            self._create_mock_chat_member(target_user) # For command logic
        ]
        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        await Main.addpoints_command(update, context)

        update.message.reply_text.assert_called_once_with("How many points do you want to add to this user?")
        self.assertIn('awaiting_addpoints_value', context.user_data)

        # --- Second update: the user's reply with the amount ---
        update.message.reply_text.reset_mock()
        reply_update = self._create_mock_update(self._create_mock_message(admin_user, chat, text="100"))

        await Main.conversation_handler(reply_update, context)

        reply_update.message.reply_text.assert_called_once_with("Added 100 points.")
        self.assertNotIn('awaiting_addpoints_value', context.user_data)
        self.assertEqual(Main.get_user_points(chat.id, target_user.id), 150)

    async def test_reward_command_buy_success(self):
        """Test a user successfully buying a reward."""
        # 1. Setup
        user = self._create_mock_user(1, "Test User")
        chat = self._create_mock_chat(101, title="Test Group")
        Main.save_rewards_data({"101": [{"name": "Cool Hat", "cost": 50}]})
        Main.save_points_data({"101": {"1": 100}})

        # --- First update: the /reward command ---
        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))

        await Main.reward_command(update, context)

        update.message.reply_text.assert_called_once()
        self.assertIn("Available Rewards", update.message.reply_text.call_args[0][0])
        self.assertIn(Main.REWARD_STATE, context.user_data)

        # --- Second update: user replies with choice ---
        update.message.reply_text.reset_mock()
        reply_update = self._create_mock_update(self._create_mock_message(user, chat, text="Cool Hat"))

        admin_user = self._create_mock_user(99, "Admin")
        context.bot.get_chat_administrators.return_value = [self._create_mock_chat_member(admin_user)]

        await Main.conversation_handler(reply_update, context)

        # Assert public announcement
        context.bot.send_message.assert_any_call(
            chat_id=chat.id,
            text=f"🎁 <b>{user.mention_html()}</b> just bought the reward: <b>Cool Hat</b>! 🎉",
            parse_mode='HTML'
        )
        # Assert admin notification
        context.bot.send_message.assert_any_call(
            chat_id=admin_user.id,
            text=f"User {user.mention_html()} (ID: {user.id}) in group {chat.title} (ID: {chat.id}) just bought the reward: 'Cool Hat' for 50 points."
        )

        self.assertEqual(Main.get_user_points(chat.id, user.id), 50)
        self.assertNotIn(Main.REWARD_STATE, context.user_data)

    async def test_reward_command_insufficient_points(self):
        """Test a user failing to buy a reward due to insufficient points."""
        # 1. Setup
        user = self._create_mock_user(1, "Test User")
        chat = self._create_mock_chat(101)
        Main.save_rewards_data({"101": [{"name": "Expensive Sword", "cost": 500}]})
        Main.save_points_data({"101": {"1": 100}})

        # --- Start conversation with /reward ---
        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))
        await Main.reward_command(update, context)

        # --- User replies with choice ---
        reply_update = self._create_mock_update(self._create_mock_message(user, chat, text="Expensive Sword"))
        await Main.conversation_handler(reply_update, context)

        # 3. Assert
        reply_update.message.reply_text.assert_called_once_with(
            "You do not have enough points for this reward. You have 100, but it costs 500."
        )
        self.assertEqual(Main.get_user_points(chat.id, user.id), 100) # Points unchanged
        self.assertNotIn(Main.REWARD_STATE, context.user_data) # State cleared

    # --- Punishment System Tests ---
    async def test_punishment_is_triggered_on_point_loss(self):
        """Test that a punishment is triggered when a user's points fall below the threshold."""
        # 1. Setup
        user = self._create_mock_user(1, "Unlucky User")
        chat = self._create_mock_chat(101, title="Test Group")
        admin_user = self._create_mock_user(99, "Admin")

        Main.save_punishments_data({"101": [{"threshold": 0, "message": "You are grounded."}]})
        Main.save_points_data({"101": {"1": 50}})

        context = self._create_mock_context()
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(user)
        context.bot.get_chat.return_value = chat
        context.bot.get_chat_administrators.return_value = [self._create_mock_chat_member(admin_user)]

        # 2. Execute
        # Lose 60 points, bringing the total to -10, which is below the 0 threshold.
        await Main.add_user_points(chat.id, user.id, -60, context)

        # 3. Assert
        # Check that the public punishment message was sent.
        # We also expect the negative points message, so we check for any call matching the punishment.
        self.assertIn(
            f"🚨 <b>Punishment Issued!</b> 🚨\n{user.mention_html()} has fallen below 0 points. Punishment: You are grounded.",
            [call[1]['text'] for call in context.bot.send_message.call_args_list if call[1]['chat_id'] == chat.id]
        )

        # Check that the admin was notified about the specific punishment
        context.bot.send_message.assert_any_call(
            chat_id=admin_user.id,
            text=f"User {user.mention_html()} (ID: {user.id}) in group {chat.title} (ID: {chat.id}) triggered punishment 'You are grounded.' by falling below 0 points."
        )

        # Check that the punishment status was recorded
        status = Main.get_triggered_punishments_for_user(chat.id, user.id)
        self.assertEqual(status, ["You are grounded."])

    async def test_negative_points_first_strike(self):
        """Test the first strike for falling into negative points."""
        # 1. Setup
        user = self._create_mock_user(1, "Rule Breaker")
        chat = self._create_mock_chat(101)
        Main.save_points_data({"101": {"1": 10}})

        context = self._create_mock_context()
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(user)

        # 2. Execute
        await Main.add_user_points(chat.id, user.id, -20, context) # Points become -10

        # 3. Assert
        # Check that the user was muted for 24 hours
        context.bot.restrict_chat_member.assert_called_once()
        self.assertEqual(context.bot.restrict_chat_member.call_args[1]['user_id'], user.id)
        self.assertEqual(context.bot.restrict_chat_member.call_args[1]['permissions'], {'can_send_messages': False})

        # Check that a message was sent to the group
        context.bot.send_message.assert_called_once_with(
            chat_id=chat.id,
            text=f"{user.mention_html()} has dropped into negative points (Strike 1/3). They have been muted for 24 hours and their points reset to 0.",
            parse_mode='HTML'
        )

        # Check that the user's points were reset to 0
        self.assertEqual(Main.get_user_points(chat.id, user.id), 0)

        # Check that the strike counter was updated
        tracker = Main.load_negative_tracker()
        self.assertEqual(tracker["101"]["1"], 1)

    # --- Game System Tests ---
    async def test_newgame_command_success(self):
        """Test the successful initiation of a new game with /newgame."""
        # 1. Setup
        challenger = self._create_mock_user(1, "Challenger")
        opponent = self._create_mock_user(2, "Opponent")
        chat = self._create_mock_chat(101)

        context = self._create_mock_context()
        # FIX: The send_and_track_message function will call send_message, which returns a mock.
        # We need to ensure the returned mock has an integer message_id and chat.id
        context.bot.send_message.return_value = self._create_mock_message(challenger, self._create_mock_chat(chat.id), message_id=999)

        replied_to_message = self._create_mock_message(opponent, chat, text="Hi")
        command_message = self._create_mock_message(challenger, chat, text="/newgame", reply_to=replied_to_message)
        update = self._create_mock_update(command_message)

        # 2. Execute
        await Main.newgame_command(update, context)

        # 3. Assert
        # Check for public message
        context.bot.send_message.assert_any_call(
            chat_id=chat.id,
            text=f"{challenger.mention_html()} has challenged {opponent.mention_html()}! {challenger.mention_html()}, please check your private messages to set up the game.",
            parse_mode='HTML'
        )

        # Check for private message to challenger
        private_message_call = next(c for c in context.bot.send_message.call_args_list if c[1]['chat_id'] == challenger.id)
        self.assertIn("Let's set up your game!", private_message_call[1]['text'])
        self.assertIsNotNone(private_message_call[1]['reply_markup'])

        # Check game creation in DB
        games_data = await Main.load_games_data_async()
        self.assertEqual(len(games_data), 1)
        game = list(games_data.values())[0]
        self.assertEqual(game['challenger_id'], challenger.id)
        self.assertEqual(game['status'], 'pending_game_selection')

    async def test_game_setup_and_challenge_flow(self):
        """Test the full game setup conversation for the challenger."""
        # 1. Setup
        challenger = self._create_mock_user(1, "Challenger")
        opponent = self._create_mock_user(2, "Opponent")
        group_chat = self._create_mock_chat(101, title="Test Group")
        private_chat = self._create_mock_chat(challenger.id, chat_type="private")
        game_id = "test-game-123"

        Main.save_points_data({str(group_chat.id): {str(challenger.id): 100}})
        await Main.save_games_data_async({game_id: {
            "group_id": group_chat.id, "challenger_id": challenger.id, "opponent_id": opponent.id,
            "status": "pending_game_selection", "messages_to_delete": [], "last_activity": 0
        }})

        context = self._create_mock_context(user_data={'game_id': game_id})

        # --- Test conversation flow step-by-step (occurs in private chat) ---
        # Entry point: Challenger clicks "Start Game Setup"
        update = self._create_mock_update(self._create_mock_message(challenger, private_chat), callback_query_data=f"game:setup:start:{game_id}")
        next_state = await Main.start_game_setup(update, context)
        self.assertEqual(next_state, Main.GAME_SELECTION)

        # State 1: Challenger selects "Connect Four"
        update.callback_query.data = f"game:connect_four:{game_id}"
        next_state = await Main.game_selection(update, context)
        self.assertEqual(next_state, Main.STAKE_TYPE_SELECTION)

        # State 2: Challenger selects "Points"
        update.callback_query.data = f"stake:points:{game_id}"
        next_state = await Main.stake_type_selection(update, context)
        self.assertEqual(next_state, Main.STAKE_SUBMISSION_POINTS)

        # State 3: Challenger sends "50" points
        update = self._create_mock_update(self._create_mock_message(challenger, private_chat, text="50"))
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(opponent)
        context.bot.send_message.return_value = self._create_mock_message(challenger, private_chat, message_id=456)
        next_state = await Main.stake_submission_points(update, context)
        self.assertEqual(next_state, Main.CONFIRMATION)

        # State 4: Challenger confirms
        update = self._create_mock_update(self._create_mock_message(challenger, private_chat), callback_query_data=f"confirm_game:challenger:{game_id}")
        context.bot.get_chat_member.side_effect = [self._create_mock_chat_member(challenger), self._create_mock_chat_member(opponent)]
        next_state = await Main.confirm_game_setup(update, context)

        # --- Assertions ---
        # Assert challenge was sent to the group chat
        group_call = next(c for c in context.bot.send_message.call_args_list if c[1]['chat_id'] == group_chat.id)
        self.assertIn("New Challenge!", group_call[1]['text'])

        # Assert conversation ended
        from telegram.ext import ConversationHandler
        self.assertEqual(next_state, ConversationHandler.END)

        # Assert game state is correct
        game = (await Main.load_games_data_async())[game_id]
        self.assertEqual(game['status'], 'pending_opponent_acceptance')
        self.assertEqual(game['challenger_stake'], {'type': 'points', 'value': 50})

    # --- Chance Game Tests ---
    @patch('Main.get_chance_outcome', return_value="plus_50")
    async def test_chance_command_plus_50(self, mock_get_outcome):
        """Test the 'plus_50' outcome of the /chance command."""
        # 1. Setup
        user = self._create_mock_user(1, "Lucky User")
        chat = self._create_mock_chat(101)
        Main.save_points_data({"101": {"1": 100}})

        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute
        await Main.chance_command(update, context)

        # 3. Assert
        self.assertIn("You spin the wheel of fortune...", update.message.reply_text.call_args_list[0].args[0])
        self.assertIn("Congratulations! You won 50 points!", update.message.reply_text.call_args_list[1].args[0])
        self.assertEqual(Main.get_user_points(chat.id, user.id), 150)

    @patch('Main.get_chance_outcome', return_value="double_points")
    async def test_chance_command_double_points(self, mock_get_outcome):
        """Test the 'double_points' outcome of the /chance command."""
        # 1. Setup
        user = self._create_mock_user(1, "Lucky User")
        chat = self._create_mock_chat(101)
        Main.save_points_data({"101": {"1": 120}})

        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute
        await Main.chance_command(update, context)

        # 3. Assert
        self.assertIn("Jackpot! Your points have been doubled!", update.message.reply_text.call_args_list[1].args[0])
        self.assertEqual(Main.get_user_points(chat.id, user.id), 240)

    async def test_chance_command_cooldown(self):
        """Test that the /chance command respects the 3-per-day cooldown."""
        # 1. Setup
        user = self._create_mock_user(1, "Gambler")
        chat = self._create_mock_chat(101)
        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute & Assert
        # Call it 3 times successfully
        with patch('Main.get_chance_outcome', return_value="nothing"):
            for _ in range(3):
                await Main.chance_command(update, context)

        self.assertEqual(update.message.reply_text.call_count, 6)

        # Call it a 4th time and expect a cooldown message
        update.message.reply_text.reset_mock()
        await Main.chance_command(update, context)

        update.message.reply_text.assert_called_once_with("You have already played 3 times today. Please wait until tomorrow.")

    # --- Command Management Tests ---
    async def test_disable_command_as_admin(self):
        """Test that an admin can disable a command."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin")
        chat = self._create_mock_chat(101)

        context = self._create_mock_context(args=["reward"])
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(admin_user, status="administrator")
        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        # 2. Execute
        await Main.disable_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with("Command /reward has been disabled in this group. Admins can re-enable it with /enable reward.")
        disabled_data = Main.load_disabled_commands()
        self.assertEqual(disabled_data["101"], ["reward"])

    async def test_enable_command_as_admin(self):
        """Test that an admin can re-enable a command."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin")
        chat = self._create_mock_chat(101)
        Main.save_disabled_commands({"101": ["reward"]})

        context = self._create_mock_context(args=["reward"])
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(admin_user, status="administrator")
        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        # 2. Execute
        await Main.enable_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once_with("Command /reward has been enabled.")
        disabled_data = Main.load_disabled_commands()
        self.assertEqual(disabled_data["101"], [])

    async def test_disabled_command_is_ignored_for_user(self):
        """Test that a disabled command is silently ignored for regular users."""
        # 1. Setup
        user = self._create_mock_user(1, "Regular User")
        chat = self._create_mock_chat(101)
        Main.save_disabled_commands({"101": ["reward"]})

        context = self._create_mock_context()
        context.bot.get_chat_member.return_value = self._create_mock_chat_member(user, status="member")
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute
        await Main.reward_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_not_called()

    # --- Help System Tests ---
    async def test_help_command_as_user(self):
        """Test the /help command for a regular user in a private chat."""
        # 1. Setup
        user = self._create_mock_user(1, "Regular User")
        chat = self._create_mock_chat(1, chat_type="private")
        Main.save_admin_data({'owner': '999', 'admins': {}}) # Ensure user is not admin

        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(user, chat))

        # 2. Execute
        await Main.help_command(update, context)

        # 3. Assert
        update.message.reply_text.assert_called_once()
        reply_markup = update.message.reply_text.call_args[1]['reply_markup']
        buttons = reply_markup.inline_keyboard

        self.assertEqual(len(buttons), 3) # General, Games, Points
        self.assertNotIn("Admin Commands", str(buttons))

    @patch('Main.OWNER_ID', 1)
    async def test_help_command_as_admin(self):
        """Test the /help command shows the admin button for an admin."""
        # 1. Setup
        admin_user = self._create_mock_user(1, "Admin User") # Owner via patch
        chat = self._create_mock_chat(1, chat_type="private")

        context = self._create_mock_context()
        update = self._create_mock_update(self._create_mock_message(admin_user, chat))

        # 2. Execute
        await Main.help_command(update, context)

        # 3. Assert
        reply_markup = update.message.reply_text.call_args[1]['reply_markup']
        buttons = reply_markup.inline_keyboard
        self.assertEqual(len(buttons), 4) # General, Games, Points, Admin
        self.assertIn("Admin Commands", str(buttons))

    async def test_help_menu_handler_navigation(self):
        """Test navigating through the help menu via callbacks."""
        # 1. Setup
        user = self._create_mock_user(1, "Regular User")
        chat = self._create_mock_chat(1, chat_type="private")
        Main.save_admin_data({'owner': '999', 'admins': {}})
        context = self._create_mock_context()

        # --- Test "General Commands" button ---
        update = self._create_mock_update(self._create_mock_message(user, chat), callback_query_data="help_general")
        await Main.help_menu_handler(update, context)

        update.callback_query.edit_message_text.assert_called_once()
        # FIX: Access positional args [0] instead of kwargs [1]
        text = update.callback_query.edit_message_text.call_args[0][0]
        self.assertIn("General Commands", text)

        # --- Test "Back" button ---
        update.callback_query.edit_message_text.reset_mock()
        update.callback_query.data = "help_back"
        await Main.help_menu_handler(update, context)

        update.callback_query.edit_message_text.assert_called_once()
        # FIX: Access positional args [0] instead of kwargs [1]
        text = update.callback_query.edit_message_text.call_args[0][0]
        self.assertIn("Welcome to the help menu!", text)

if __name__ == '__main__':
    unittest.main()
