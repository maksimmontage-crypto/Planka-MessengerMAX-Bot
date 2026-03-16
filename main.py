#!/usr/bin/env python3
"""
Planka Bot for Max Messenger (групповые чаты)
Отправляет уведомления о задачах с Kanban-доски в мессенджер Макс.
Поддерживает HTML-форматирование (параметр format='html').
Вся конфигурация в .env файле.
"""

import os
import sys
import time
import sqlite3
import requests
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import pytz
from dotenv import load_dotenv

load_dotenv()

# Конфигурация лога
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class Config:
    """Application configuration from environment variables"""

    # Max Messenger
    MAX_TOKEN = os.getenv('MAX_TOKEN')
    MAX_API_URL = os.getenv('MAX_API_URL', 'https://platform-api.max.ru')

    # Planka
    PLANKA_URL = os.getenv('PLANKA_URL', 'http://localhost:8080')
    PLANKA_USERNAME = os.getenv('PLANKA_USERNAME', '')
    PLANKA_PASSWORD = os.getenv('PLANKA_PASSWORD', '')

    # Интервалы (секунды)
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '30'))
    DEADLINE_CHECK_INTERVAL = int(os.getenv('DEADLINE_CHECK_INTERVAL', '30'))

    # База данных
    DATABASE_PATH = os.getenv('DATABASE_PATH', '/opt/planka-telegram-bot/planka-bot.db')

    # Временная зона
    TIMEZONE = os.getenv('TIMEZONE', 'Europe/Moscow')

    # Ключевые слова для столбца "Выполнено"
    COMPLETED_KEYWORDS = [kw.strip() for kw in os.getenv('COMPLETED_KEYWORDS', 'выполнено,готово,завершен').split(',')]

    @classmethod
    def validate(cls) -> bool:
        """Validate required configuration"""
        if not cls.MAX_TOKEN:
            logger.error("❌ Отсутствует токен доступа Max!")
            return False
        if not cls.PLANKA_USERNAME or not cls.PLANKA_PASSWORD:
            logger.error("❌ Не заполнены логин/пароль Planka!")
            return False
        return True

    @classmethod
    def get_board_mappings(cls) -> Dict[str, str]:
        """Extract board to Max chat mappings from environment variables
        Format: PLANKABOARD_{BOARD_ID}_MAX_CHAT_ID={CHAT_ID}
        """
        mappings = {}
        for key, value in os.environ.items():
            if key.startswith('PLANKABOARD_') and key.endswith('_MAX_CHAT_ID'):
                parts = key.split('_')
                if len(parts) >= 2:
                    board_id = parts[1]
                    mappings[board_id] = value
        return mappings


class PlankaBotService:
    def __init__(self):
        if not Config.validate():
            sys.exit(1)

        self.boards_config = Config.get_board_mappings()
        if not self.boards_config:
            logger.error("❌ Не сконфигурированы доски!")
            logger.info("💡 Пожалуйста настройте доски в .env файле:")
            logger.info("   Формат: PLANKABOARD_{BOARD_ID}_MAX_CHAT_ID={CHAT_ID}")
            sys.exit(1)

        self.working_dir = Path('/opt/planka-telegram-bot')
        os.chdir(self.working_dir)

        self.boards_info = {}
        self.lists_cache = {}
        self.users_cache = {}
        self.local_tz = pytz.timezone(Config.TIMEZONE)

        self.init_db()
        self.auth()
        self.load_boards_info()

        logger.info(f"🚀 Planka Bot для Max (групповые чаты) запущен")
        logger.info(f"📊 Досок в работе: {len(self.boards_info)}")

    def init_db(self):
        """Initialize database with migration to max_chat_id column"""
        self.db_path = Path(Config.DATABASE_PATH)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.db_path))
        cursor = self.conn.cursor()

        # Create processed_cards table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_cards (
                card_id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # Check if tracked_tasks exists and migrate if needed
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tracked_tasks'")
        table_exists = cursor.fetchone() is not None

        if table_exists:
            cursor.execute("PRAGMA table_info(tracked_tasks)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'telegram_chat' in columns and 'max_chat_id' not in columns:
                logger.info("🔄 Миграция: telegram_chat → max_chat_id")
                self._migrate_from_telegram_chat(cursor)
            elif 'max_user_id' in columns and 'max_chat_id' not in columns:
                logger.info("🔄 Миграция: max_user_id → max_chat_id")
                self._migrate_from_max_user_id(cursor)
            elif 'max_chat_id' not in columns:
                logger.warning("⚠️ Неизвестная структура таблицы, создаём заново")
                cursor.execute("DROP TABLE tracked_tasks")
                self._create_tracked_tasks_table(cursor)
            else:
                logger.debug("✅ Таблица tracked_tasks уже в актуальной структуре")
        else:
            self._create_tracked_tasks_table(cursor)

        # Create assignee_history table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS assignee_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                card_id TEXT NOT NULL,
                board_id TEXT NOT NULL,
                old_user_id TEXT,
                old_user_name TEXT,
                new_user_id TEXT,
                new_user_name TEXT,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                notified BOOLEAN DEFAULT FALSE
            )
        ''')

        # Indexes
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_board_card ON tracked_tasks(board_id, card_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_card ON assignee_history(card_id, board_id)')

        self.conn.commit()
        logger.info("✅ База данных готова")

    def _migrate_from_telegram_chat(self, cursor):
        """Migrate from old table with telegram_chat to new with max_chat_id"""
        cursor.execute('''
            CREATE TABLE tracked_tasks_new (
                card_id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL,
                board_name TEXT NOT NULL,
                max_chat_id TEXT NOT NULL,
                card_name TEXT NOT NULL,
                list_id TEXT NOT NULL,
                list_name TEXT NOT NULL,
                due_date TIMESTAMP,
                assigned_user_id TEXT,
                assigned_user_name TEXT,
                is_completed BOOLEAN DEFAULT FALSE,
                notified_assigned BOOLEAN DEFAULT FALSE,
                notified_24h BOOLEAN DEFAULT FALSE,
                notified_3h BOOLEAN DEFAULT FALSE,
                notified_1h BOOLEAN DEFAULT FALSE,
                notified_assignee BOOLEAN DEFAULT FALSE,
                notified_overdue BOOLEAN DEFAULT FALSE,
                last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            INSERT INTO tracked_tasks_new (
                card_id, board_id, board_name, max_chat_id,
                card_name, list_id, list_name, due_date,
                assigned_user_id, assigned_user_name, is_completed,
                notified_assigned, notified_24h, notified_3h, notified_1h,
                notified_assignee, notified_overdue, last_check, last_updated
            )
            SELECT
                card_id, board_id, board_name, telegram_chat,
                card_name, list_id, list_name, due_date,
                assigned_user_id, assigned_user_name, is_completed,
                notified_assigned, notified_24h, notified_3h, notified_1h,
                notified_assignee, notified_overdue, last_check, last_updated
            FROM tracked_tasks
        ''')
        cursor.execute('DROP TABLE tracked_tasks')
        cursor.execute('ALTER TABLE tracked_tasks_new RENAME TO tracked_tasks')
        logger.info("✅ Миграция из telegram_chat завершена")

    def _migrate_from_max_user_id(self, cursor):
        """Rename max_user_id column to max_chat_id"""
        cursor.execute('''
            CREATE TABLE tracked_tasks_new (
                card_id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL,
                board_name TEXT NOT NULL,
                max_chat_id TEXT NOT NULL,
                card_name TEXT NOT NULL,
                list_id TEXT NOT NULL,
                list_name TEXT NOT NULL,
                due_date TIMESTAMP,
                assigned_user_id TEXT,
                assigned_user_name TEXT,
                is_completed BOOLEAN DEFAULT FALSE,
                notified_assigned BOOLEAN DEFAULT FALSE,
                notified_24h BOOLEAN DEFAULT FALSE,
                notified_3h BOOLEAN DEFAULT FALSE,
                notified_1h BOOLEAN DEFAULT FALSE,
                notified_assignee BOOLEAN DEFAULT FALSE,
                notified_overdue BOOLEAN DEFAULT FALSE,
                last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            INSERT INTO tracked_tasks_new (
                card_id, board_id, board_name, max_chat_id,
                card_name, list_id, list_name, due_date,
                assigned_user_id, assigned_user_name, is_completed,
                notified_assigned, notified_24h, notified_3h, notified_1h,
                notified_assignee, notified_overdue, last_check, last_updated
            )
            SELECT
                card_id, board_id, board_name, max_user_id,
                card_name, list_id, list_name, due_date,
                assigned_user_id, assigned_user_name, is_completed,
                notified_assigned, notified_24h, notified_3h, notified_1h,
                notified_assignee, notified_overdue, last_check, last_updated
            FROM tracked_tasks
        ''')
        cursor.execute('DROP TABLE tracked_tasks')
        cursor.execute('ALTER TABLE tracked_tasks_new RENAME TO tracked_tasks')
        logger.info("✅ Переименование max_user_id в max_chat_id завершено")

    def _create_tracked_tasks_table(self, cursor):
        """Create tracked_tasks table with max_chat_id column"""
        cursor.execute('''
            CREATE TABLE tracked_tasks (
                card_id TEXT PRIMARY KEY,
                board_id TEXT NOT NULL,
                board_name TEXT NOT NULL,
                max_chat_id TEXT NOT NULL,
                card_name TEXT NOT NULL,
                list_id TEXT NOT NULL,
                list_name TEXT NOT NULL,
                due_date TIMESTAMP,
                assigned_user_id TEXT,
                assigned_user_name TEXT,
                is_completed BOOLEAN DEFAULT FALSE,
                notified_assigned BOOLEAN DEFAULT FALSE,
                notified_24h BOOLEAN DEFAULT FALSE,
                notified_3h BOOLEAN DEFAULT FALSE,
                notified_1h BOOLEAN DEFAULT FALSE,
                notified_assignee BOOLEAN DEFAULT FALSE,
                notified_overdue BOOLEAN DEFAULT FALSE,
                last_check TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

    def auth(self):
        """Аутентификация в Planka"""
        try:
            auth = requests.post(
                f"{Config.PLANKA_URL}/api/access-tokens",
                json={
                    "emailOrUsername": Config.PLANKA_USERNAME,
                    "password": Config.PLANKA_PASSWORD
                },
                timeout=10
            )
            if auth.status_code == 200:
                self.token = auth.json()['item']
                self.headers = {
                    'Authorization': f'Bearer {self.token}',
                    'Content-Type': 'application/json'
                }
                logger.info("✅ Авторизация в Planka выполнена")
            else:
                logger.error(f"❌ Ошибка авторизации в Planka: {auth.status_code}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"❌ Ошибка соединения с Planka: {e}")
            sys.exit(1)

    def load_boards_info(self):
        """Load information about all boards"""
        logger.info("📊 Загрузка информации о досках...")
        for board_id, chat_id in self.boards_config.items():
            try:
                url = f"{Config.PLANKA_URL}/api/boards/{board_id}"
                response = requests.get(url, headers=self.headers, timeout=10)
                if response.status_code == 200:
                    board_data = response.json()
                    board_name = board_data.get('item', {}).get('name', f'Board {board_id}')

                    lists_url = f"{url}?include=lists"
                    lists_response = requests.get(lists_url, headers=self.headers, timeout=10)
                    completed_list_id = None

                    if lists_response.status_code == 200:
                        lists_data = lists_response.json()
                        if 'included' in lists_data and 'lists' in lists_data['included']:
                            lists = lists_data['included']['lists']
                            for lst in lists:
                                if isinstance(lst, dict):
                                    list_id = lst.get('id')
                                    list_name = lst.get('name', 'Untitled')
                                    self.lists_cache[list_id] = list_name
                                    lower_name = list_name.lower()
                                    for keyword in Config.COMPLETED_KEYWORDS:
                                        if keyword.lower() in lower_name:
                                            completed_list_id = list_id
                                            break

                    self.boards_info[board_id] = {
                        'name': board_name,
                        'chat_id': chat_id,
                        'completed_list_id': completed_list_id
                    }
                    logger.info(f"✅ Доска: '{board_name}' (ID: {board_id})")
                    logger.info(f"   📱 Чат (ID): {chat_id}")
                else:
                    logger.error(f"❌ Ошибка загрузки доски {board_id}: {response.status_code}")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки доски {board_id}: {e}")

        logger.info(f"📝 Всего колонок в кэше: {len(self.lists_cache)}")

    def get_board_cards(self, board_id: str) -> List[Dict]:
        """Get all cards from board"""
        try:
            url = f"{Config.PLANKA_URL}/api/boards/{board_id}?include=cards,cardMemberships"
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return self.extract_real_cards(response.json(), board_id)
        except Exception as e:
            logger.error(f"❌ Ошибка получения карточек с доски {board_id}: {e}")
        return []

    def extract_real_cards(self, data: Dict, board_id: str) -> List[Dict]:
        """Extract all cards with current state"""
        cards = []
        existing_card_ids = set()

        if 'included' not in data:
            return cards

        included = data['included']

        if 'cards' in included:
            for card_obj in included['cards']:
                if isinstance(card_obj, dict):
                    list_id = card_obj.get('listId')
                    card_id = card_obj.get('id')
                    if list_id and card_id and list_id in self.lists_cache:
                        existing_card_ids.add(card_id)
                        due_date_str = card_obj.get('dueDate')
                        due_date = self.parse_due_date(due_date_str) if due_date_str else None
                        cards.append({
                            'id': card_id,
                            'name': card_obj.get('name', 'Untitled'),
                            'listId': list_id,
                            'boardId': board_id,
                            'createdAt': card_obj.get('createdAt', ''),
                            'dueDate': due_date,
                            'assignedUserId': None,
                            'assignedUserName': None
                        })

        if 'cardMemberships' in included:
            for membership in included['cardMemberships']:
                if isinstance(membership, dict):
                    card_id = membership.get('cardId')
                    user_id = membership.get('userId')
                    if card_id and user_id:
                        for card in cards:
                            if card['id'] == card_id:
                                card['assignedUserId'] = user_id
                                card['assignedUserName'] = self.get_user_name(user_id)
                                break

        self.cleanup_deleted_cards(board_id, existing_card_ids)
        return cards

    def cleanup_deleted_cards(self, board_id: str, existing_card_ids: set):
        """Remove cards that no longer exist on the board"""
        cursor = self.conn.cursor()
        cursor.execute('SELECT card_id FROM tracked_tasks WHERE board_id = ?', (board_id,))
        db_cards = [row[0] for row in cursor.fetchall()]
        cards_to_delete = set(db_cards) - existing_card_ids

        if cards_to_delete:
            placeholders = ','.join(['?'] * len(cards_to_delete))
            cursor.execute(f'DELETE FROM tracked_tasks WHERE board_id = ? AND card_id IN ({placeholders})',
                           (board_id, *cards_to_delete))
            cursor.execute(f'DELETE FROM processed_cards WHERE board_id = ? AND card_id IN ({placeholders})',
                           (board_id, *cards_to_delete))
            cursor.execute(f'DELETE FROM assignee_history WHERE board_id = ? AND card_id IN ({placeholders})',
                           (board_id, *cards_to_delete))
            self.conn.commit()
            logger.info(f"🗑️ Удалено {len(cards_to_delete)} карточек с доски {board_id}")

    def get_user_name(self, user_id: str) -> Optional[str]:
        """Получение имени пользователя по ID"""
        if not user_id:
            return None
        if user_id in self.users_cache:
            return self.users_cache[user_id]
        try:
            response = requests.get(f"{Config.PLANKA_URL}/api/users/{user_id}", headers=self.headers, timeout=5)
            if response.status_code == 200:
                user_name = response.json().get('item', {}).get('name', 'Unknown')
                self.users_cache[user_id] = user_name
                return user_name
        except Exception as e:
            logger.error(f"⚠️ Ошибка получения имени пользователя {user_id}: {e}")
        return None

    def parse_due_date(self, due_date_str: str) -> Optional[datetime]:
        """Parse dueDate from various formats to UTC datetime"""
        if not due_date_str:
            return None
        try:
            if due_date_str.endswith('Z'):
                clean_str = due_date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(clean_str)
                return dt.astimezone(timezone.utc)
            elif '+' in due_date_str:
                dt = datetime.fromisoformat(due_date_str)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            else:
                dt = datetime.fromisoformat(due_date_str)
                return dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            logger.error(f"❌ Ошибка парсинга даты '{due_date_str}': {e}")
            return None

    def is_card_processed(self, card_id: str, board_id: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM processed_cards WHERE card_id = ? AND board_id = ?", (card_id, board_id))
        return cursor.fetchone() is not None

    def mark_card_processed(self, card_id: str, board_id: str):
        cursor = self.conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO processed_cards (card_id, board_id) VALUES (?, ?)", (card_id, board_id))
        self.conn.commit()

    def check_and_update_tasks(self, cards: List[Dict]):
        """Check and update all tasks for changes"""
        cursor = self.conn.cursor()
        for card in cards:
            board_id = card['boardId']
            board_info = self.boards_info.get(board_id, {})
            list_name = self.lists_cache.get(card['listId'], 'Unknown column')
            is_completed = (card['listId'] == board_info.get('completed_list_id'))
            due_date_str = card['dueDate'].isoformat() if card['dueDate'] else None

            cursor.execute('''
                SELECT card_name, list_id, due_date, assigned_user_id, assigned_user_name,
                       is_completed, notified_assignee, notified_assigned,
                       notified_24h, notified_3h, notified_1h, notified_overdue
                FROM tracked_tasks
                WHERE card_id = ? AND board_id = ?
            ''', (card['id'], board_id))
            existing = cursor.fetchone()

            if existing:
                (old_name, old_list_id, old_due_date, old_user_id, old_user_name,
                 old_completed, old_notified_assignee, old_notified_assigned,
                 old_notified_24h, old_notified_3h, old_notified_1h, old_notified_overdue) = existing

                changes = []
                if old_name != card['name']:
                    changes.append(f"name: '{old_name}' → '{card['name']}'")

                list_changed = (old_list_id != card['listId'])
                if list_changed:
                    changes.append(f"column: {old_list_id} → {card['listId']}")

                due_date_changed = False
                if due_date_str != old_due_date:
                    due_date_changed = True
                    changes.append("Срок выполнения изменён" if old_due_date else "Добавлен срок выполнения")

                assignee_changed = False
                current_user_id = card.get('assignedUserId')
                current_user_name = card.get('assignedUserName')
                if (old_user_id is None and current_user_id is not None) or \
                   (old_user_id is not None and current_user_id is None) or \
                   (old_user_id != current_user_id):
                    assignee_changed = True
                    cursor.execute('''
                        INSERT INTO assignee_history
                        (card_id, board_id, old_user_id, old_user_name, new_user_id, new_user_name)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (card['id'], board_id, old_user_id, old_user_name, current_user_id, current_user_name))
                    changes.append(f"assignee: {old_user_name or old_user_id} → {current_user_name or current_user_id}")

                status_changed = (old_completed != is_completed)
                if status_changed:
                    changes.append(f"status: {'completed' if is_completed else 'in progress'}")

                reset_deadline_flags = due_date_changed or status_changed or list_changed

                new_notified_assignee = old_notified_assignee
                if assignee_changed:
                    new_notified_assignee = False

                new_notified_assigned = old_notified_assigned
                if due_date_changed:
                    new_notified_assigned = False

                new_notified_overdue = old_notified_overdue
                if reset_deadline_flags:
                    new_notified_overdue = False

                if changes:
                    logger.info(f"🔄 Изменения в задаче '{card['name']}': {', '.join(changes)}")
                    cursor.execute('''
                        UPDATE tracked_tasks
                        SET card_name = ?, list_id = ?, list_name = ?, due_date = ?,
                            assigned_user_id = ?, assigned_user_name = ?, is_completed = ?,
                            notified_assignee = ?, notified_assigned = ?,
                            notified_24h = ?, notified_3h = ?, notified_1h = ?,
                            notified_overdue = ?, last_updated = CURRENT_TIMESTAMP
                        WHERE card_id = ? AND board_id = ?
                    ''', (
                        card['name'], card['listId'], list_name, due_date_str,
                        current_user_id, current_user_name, is_completed,
                        new_notified_assignee, new_notified_assigned,
                        False if reset_deadline_flags else old_notified_24h,
                        False if reset_deadline_flags else old_notified_3h,
                        False if reset_deadline_flags else old_notified_1h,
                        new_notified_overdue,
                        card['id'], board_id
                    ))
                    self.conn.commit()

                    if assignee_changed and current_user_id and not is_completed and not new_notified_assignee:
                        self.send_assignee_notification(card, board_info)
                else:
                    cursor.execute('UPDATE tracked_tasks SET last_check = CURRENT_TIMESTAMP WHERE card_id = ? AND board_id = ?',
                                   (card['id'], board_id))
                    self.conn.commit()
            else:
                cursor.execute('''
                    INSERT INTO tracked_tasks
                    (card_id, board_id, board_name, max_chat_id, card_name,
                     list_id, list_name, due_date, assigned_user_id, assigned_user_name,
                     is_completed, notified_assigned, notified_24h, notified_3h,
                     notified_1h, notified_assignee, notified_overdue, last_check)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    card['id'], board_id, board_info.get('name', 'Unknown board'),
                    board_info.get('chat_id', ''), card['name'], card['listId'], list_name,
                    due_date_str, card.get('assignedUserId'), card.get('assignedUserName'),
                    is_completed,
                    False, False, False, False, False, False
                ))
                self.conn.commit()
                self.mark_card_processed(card['id'], board_id)
                self.send_new_card_notification(card, board_info)

    def send_new_card_notification(self, card: Dict, board_info: Dict) -> bool:
        """Уведомление о новой задаче"""
        try:
            list_name = self.lists_cache.get(card.get('listId', ''), 'Unknown column')
            board_name = board_info.get('name', 'Unknown board')
            chat_id = board_info.get('chat_id', '')

            message = (
                f"❗️<b>Новая задача</b>\n\n"
                f"🆕<b>Заголовок:</b> {card.get('name', 'Untitled')}\n"
                f"📊<b>Категория:</b> {list_name}\n"
                f"💻<b>Доска:</b> {board_name}"
            )

            created_at = card.get('createdAt', '')
            if created_at:
                try:
                    dt_utc = self.parse_due_date(created_at)
                    if dt_utc:
                        dt_local = dt_utc.astimezone(self.local_tz)
                        message += f"\n🕠<b>Created ({self.local_tz.zone}):</b> {dt_local.strftime('%H:%M %d.%m.%Y')}"
                except:
                    pass

            if card.get('dueDate'):
                due_date_formatted = self.format_local_time(card['dueDate'])
                message += f"\n🗓<b>Срок исполнения:</b> {due_date_formatted}"

            if card.get('assignedUserName'):
                message += f"\n👤<b>Исполнитель:</b> {card['assignedUserName']}"

            return self.send_to_max(message, chat_id)
        except Exception as e:
            logger.error(f"❌ Ошибка формирования уведомления: {e}")
            return False

    def send_assignee_notification(self, card: Dict, board_info: Dict) -> bool:
        """Уведомление о назначении ответственного"""
        try:
            list_name = self.lists_cache.get(card.get('listId', ''), 'Unknown column')
            board_name = board_info.get('name', 'Unknown board')
            chat_id = board_info.get('chat_id', '')

            message = (
                f"👤 <b>Назначен исполнитель</b>\n\n"
                f"📋 <b>Задача:</b> <i>\"{card.get('name', 'Untitled')}\"</i>\n"
                f"👨‍💻 <b>Исполнитель:</b> <b>{card.get('assignedUserName', 'Unknown')}</b>\n"
                f"📂 <b>Категория:</b> {list_name}\n"
                f"💻 <b>Доска:</b> {board_name}\n"
            )

            if card.get('dueDate'):
                due_date_formatted = self.format_local_time(card['dueDate'])
                message += f"\n🗓 <b>Срок исполнения:</b> {due_date_formatted}"

            if self.send_to_max(message, chat_id):
                logger.info(f"👤 Уведомление об исполнителе отправлено: {card['name']}")
                cursor = self.conn.cursor()
                cursor.execute('UPDATE tracked_tasks SET notified_assignee = TRUE WHERE card_id = ? AND board_id = ?',
                               (card['id'], card['boardId']))
                cursor.execute('''
                    UPDATE assignee_history SET notified = TRUE
                    WHERE card_id = ? AND board_id = ? AND new_user_id = ? AND notified = FALSE
                ''', (card['id'], card['boardId'], card.get('assignedUserId')))
                self.conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Ошибка отправки уведомления об исполнителе: {e}")
        return False

    def format_local_time(self, dt_utc: datetime) -> str:
        """Convert UTC datetime to local timezone string"""
        try:
            if dt_utc:
                if isinstance(dt_utc, str):
                    dt_utc = self.parse_due_date(dt_utc)
                    if not dt_utc:
                        return "time unknown"
                dt_local = dt_utc.astimezone(self.local_tz)
                return dt_local.strftime('%H:%M %d.%m.%Y')
        except Exception as e:
            logger.error(f"⚠️ Ошибка конвертации времени: {e}")
        return "time unknown"

    def send_to_max(self, message: str, chat_id: str) -> bool:
        """Send message to Max Messenger chat with HTML formatting"""
        try:
            url = f"{Config.MAX_API_URL}/messages?chat_id={chat_id}"
            headers = {
                'Authorization': Config.MAX_TOKEN,
                'Content-Type': 'application/json'
            }
            payload = {
                'text': message,
                'format': 'html'      # ключевой параметр для поддержки HTML
            }

            response = requests.post(url, json=payload, headers=headers, timeout=10)

            if response.status_code in (200, 201):
                logger.debug(f"Сообщение отправлено в чат {chat_id}")
                return True
            else:
                logger.error(f"❌ Ошибка отправки в Max (чат {chat_id}): {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ Ошибка отправки сообщения в Max (чат {chat_id}): {e}")
            return False

    def check_deadlines(self):
        """Check approaching deadlines and overdue tasks"""
        cursor = self.conn.cursor()
        now_utc = datetime.now(timezone.utc)

        # 1. Уведомление о том, что у задачи появился срок (notified_assigned)
        cursor.execute('''
            SELECT card_id, board_id, board_name, max_chat_id, card_name,
                   list_name, due_date, is_completed, assigned_user_name, notified_assigned
            FROM tracked_tasks
            WHERE due_date IS NOT NULL AND is_completed = FALSE AND notified_assigned = FALSE
        ''')
        for task in cursor.fetchall():
            (card_id, board_id, board_name, chat_id, card_name,
             list_name, due_date_str, is_completed, assignee_name, notified_assigned) = task
            due_date_utc = self.parse_due_date(due_date_str)
            if due_date_utc and due_date_utc > now_utc:
                message = (
                    f"❗️ <b>Внимание! Установлен срок исполнения!</b>\n\n"
                    f"📋 <b>Задача:</b> <i>\"{card_name}\"</i>\n"
                    f"🗓 <b>Срок:</b> {self.format_local_time(due_date_utc)}\n"
                    f"📂 <b>Категория:</b> {list_name}\n"
                    f"💻 <b>Доска:</b> {board_name}"
                )
                if assignee_name:
                    message += f"\n👤 <b>Исполнитель:</b> {assignee_name}"
                if self.send_to_max(message, chat_id):
                    cursor.execute('UPDATE tracked_tasks SET notified_assigned = TRUE WHERE card_id = ? AND board_id = ?',
                                   (card_id, board_id))
                    self.conn.commit()
                    logger.info(f"🗓 Уведомление о сроке отправлено: {card_name}")

        # 2. Напоминания за 24/3/1 час
        cursor.execute('''
            SELECT card_id, board_id, board_name, max_chat_id, card_name,
                   list_name, due_date, is_completed,
                   notified_24h, notified_3h, notified_1h, assigned_user_name
            FROM tracked_tasks
            WHERE due_date IS NOT NULL AND is_completed = FALSE AND due_date > ?
        ''', (now_utc.isoformat(),))
        for task in cursor.fetchall():
            (card_id, board_id, board_name, chat_id, card_name,
             list_name, due_date_str, is_completed,
             notified_24h, notified_3h, notified_1h, assignee_name) = task
            due_date_utc = self.parse_due_date(due_date_str)
            if not due_date_utc:
                continue

            time_left = due_date_utc - now_utc
            hours_left = time_left.total_seconds() / 3600

            notif_type = None
            field = None
            if 0 < hours_left <= 1 and not notified_1h:
                notif_type = 1
                field = 'notified_1h'
            elif 1 < hours_left <= 3 and not notified_3h:
                notif_type = 3
                field = 'notified_3h'
            elif 3 < hours_left <= 24 and not notified_24h:
                notif_type = 24
                field = 'notified_24h'

            if notif_type and field:
                if notif_type == 24:
                    title = "⏳ <b>До конца срока менее суток!</b>"
                    time_text = "Менее суток"
                elif notif_type == 3:
                    title = "⚠️ <b>Внимание! До конца срока менее 3 часов!</b>"
                    time_text = "Менее 3 часов"
                else:
                    title = "🚨 <b>АХТУНГ! До окончания срока менее часа!</b>"
                    time_text = "Менее часа"

                message = (
                    f"{title}\n\n"
                    f"<b>\"{card_name}\"</b>\n"
                    f"⏰ <b>Осталось времени:</b> {time_text}\n"
                    f"🗓 <b>Истекает:</b> {self.format_local_time(due_date_utc)}\n"
                    f"📂 <b>Категория:</b> {list_name}\n"
                    f"💻 <b>Доска:</b> {board_name}"
                )
                if assignee_name:
                    message += f"\n👤 <b>Исполнитель:</b> {assignee_name}"
                if notif_type <= 3:
                    message += "\n#urgent"
                message += "\n#deadline"

                if self.send_to_max(message, chat_id):
                    cursor.execute(f'UPDATE tracked_tasks SET {field} = TRUE WHERE card_id = ? AND board_id = ?',
                                   (card_id, board_id))
                    self.conn.commit()
                    logger.info(f"⏰ Напоминание ({notif_type}h): {card_name}")

        # 3. Просрочка (notified_overdue)
        cursor.execute('''
            SELECT card_id, board_id, board_name, max_chat_id, card_name,
                   list_name, due_date, assigned_user_name, notified_overdue
            FROM tracked_tasks
            WHERE due_date IS NOT NULL AND is_completed = FALSE AND due_date <= ? AND notified_overdue = FALSE
        ''', (now_utc.isoformat(),))
        for task in cursor.fetchall():
            (card_id, board_id, board_name, chat_id, card_name,
             list_name, due_date_str, assignee_name, notified_overdue) = task
            due_date_utc = self.parse_due_date(due_date_str)
            if not due_date_utc:
                continue
            overdue_hours = (now_utc - due_date_utc).total_seconds() / 3600
            if 0 < overdue_hours <= 24:
                message = (
                    f"🔴 <b>Задача просрочена!</b>\n\n"
                    f"<b>\"{card_name}\"</b>\n"
                    f"⏰ <b>Просрочено:</b> {int(overdue_hours)} ч.\n"
                    f"🗓 <b>Срок:</b> {self.format_local_time(due_date_utc)}\n"
                    f"📂 <b>Категория:</b> {list_name}\n"
                    f"💻 <b>Доска:</b> {board_name}"
                )
                if assignee_name:
                    message += f"\n👤 <b>Исполнитель:</b> {assignee_name}"
                if self.send_to_max(message, chat_id):
                    cursor.execute('UPDATE tracked_tasks SET notified_overdue = TRUE WHERE card_id = ? AND board_id = ?',
                                   (card_id, board_id))
                    self.conn.commit()
                    logger.info(f"🔴 Уведомление о просрочке: {card_name}")

    def check_pending_assignee_notifications(self):
        """Проверить неотправленные уведомления о назначении исполнителя"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT h.card_id, h.board_id, h.new_user_name, t.card_name,
                   t.board_name, t.max_chat_id, t.list_name, t.due_date, t.is_completed
            FROM assignee_history h
            JOIN tracked_tasks t ON h.card_id = t.card_id AND h.board_id = t.board_id
            WHERE h.notified = FALSE AND h.new_user_id IS NOT NULL AND t.is_completed = FALSE
        ''')
        for task in cursor.fetchall():
            (card_id, board_id, user_name, card_name,
             board_name, chat_id, list_name, due_date_str, is_completed) = task
            message = (
                f"👤 <b>Назначен исполнитель</b>\n\n"
                f"📋 <b>Задача:</b> <i>\"{card_name}\"</i>\n"
                f"👨‍💻 <b>Исполнитель:</b> <b>{user_name}</b>\n"
                f"📂 <b>Категория:</b> {list_name}\n"
                f"💻 <b>Доска:</b> {board_name}"
            )
            if due_date_str:
                due_date = self.parse_due_date(due_date_str)
                if due_date:
                    message += f"\n🗓 <b>Срок:</b> {self.format_local_time(due_date)}"
            if self.send_to_max(message, chat_id):
                cursor.execute('UPDATE assignee_history SET notified = TRUE WHERE card_id = ? AND board_id = ? AND notified = FALSE',
                               (card_id, board_id))
                cursor.execute('UPDATE tracked_tasks SET notified_assignee = TRUE WHERE card_id = ? AND board_id = ?',
                               (card_id, board_id))
                self.conn.commit()
                logger.info(f"👤 Уведомление о назначении исполнителя (повтор): {card_name}")

    def run(self):
        """Основной цикл"""
        logger.info("=" * 60)
        logger.info(f"🤖 PLANKA BOT для Max Messenger (групповые чаты) v1.2.0")
        logger.info(f"📊 Досок в работе: {len(self.boards_info)}")
        logger.info(f"⏰ Проверка сроков: каждые {Config.DEADLINE_CHECK_INTERVAL} сек")
        logger.info(f"🔄 Проверка изменений: каждые {Config.POLL_INTERVAL} сек")
        logger.info("=" * 60)

        last_deadline_check = time.time()
        last_full_check = time.time()

        while True:
            try:
                current_time = time.time()

                for board_id, board_info in self.boards_info.items():
                    logger.info(f"🔍 Проверка доски: '{board_info['name']}'...")
                    cards = self.get_board_cards(board_id)
                    if cards:
                        logger.info(f"📊 Карточек найдено: {len(cards)}")
                        self.check_and_update_tasks(cards)
                    time.sleep(1)

                if current_time - last_deadline_check > Config.DEADLINE_CHECK_INTERVAL:
                    logger.info("⏰ Проверка сроков...")
                    self.check_deadlines()
                    last_deadline_check = current_time

                self.check_pending_assignee_notifications()

                if current_time - last_full_check > 3600:  # раз в час
                    cursor = self.conn.cursor()
                    cursor.execute('SELECT COUNT(*) FROM tracked_tasks WHERE is_completed = FALSE')
                    active = cursor.fetchone()[0]
                    cursor.execute('SELECT COUNT(*) FROM tracked_tasks WHERE due_date IS NOT NULL AND is_completed = FALSE')
                    with_deadlines = cursor.fetchone()[0]
                    logger.info(f"📈 Статистика: {active} активных задач, {with_deadlines} с дедлайнами")
                    last_full_check = current_time

                sleep_time = Config.POLL_INTERVAL
                logger.info(f"⏳ Ожидание {sleep_time} сек...")
                time.sleep(sleep_time)

            except KeyboardInterrupt:
                logger.info("🛑 Остановка сервиса")
                break
            except Exception as e:
                logger.error(f"💥 Ошибка в основном цикле: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(10)

        self.conn.close()


def main():
    service = PlankaBotService()
    service.run()


if __name__ == '__main__':
    main()