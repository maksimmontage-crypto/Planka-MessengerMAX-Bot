# Planka мессенджер МАХ Bot

Бот уведомлений о задачах из [Planka](https://planka.app/). 

## Функции

Уведомления в реальном времени о новых задачах, дедлайнах (24ч, 3ч, 1ч, просрочка), смене исполнителя
Поддержка нескольких досок с отдельными чатами
Автоматическое восстановление после сетевых/API ошибок
База данных SQLite для сохранения состояния
Мониторинг состояния и статистика
Поддержка настраиваемых часовых поясов
Автоматическая очистка удалённых/архивированных задач

## Быстрый старт

### 1. Установка

```bash
git clone https://github.com/yourusername/planka-messengermax-bot.git
cd planka-messengermax-bot
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Настройка

```bash 
nano .env
# Отредактируйте .env своими настройками:
# - Данные для доступа к Planka
# - Токен Telegram-бота
# - Сопоставление досок с чатами
```

Ключевые переменные:

    PLANKA_URL, PLANKA_USERNAME, PLANKA_PASSWORD - доступ к API Planka

    MAX_BOT_TOKEN - токен бота в мессенджере МАХ

    BOARD_CHAT_MAP - сопоставление ID досок с чатами 

    TIMEZONE - Часовой пояс для уведомлений (по умолчанию: Europe/Moscow)

### 3. Запуск

```bash
python3 planka_bot.py
```

### 4. Запуск через systemd

# Создайте файл конфигурации службы
sudo nano /etc/systemd/system/planka-bot.service
# (пример конфигурации для systemd прилагается)

# Затем выполните:
sudo systemctl daemon-reload
sudo systemctl enable planka-bot
sudo systemctl start planka-bot
# Проверьте статус:
sudo systemctl status planka-bot
