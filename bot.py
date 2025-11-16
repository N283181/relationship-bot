import json
import os
import random
from datetime import datetime, timedelta
import sqlite3
import logging

# Настройка логирования
logger = logging.getLogger()

# База данных в памяти (для serverless)
def get_db():
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # Таблица браков
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS marriages (
            id INTEGER PRIMARY KEY,
            user1_id INTEGER,
            user2_id INTEGER, 
            user1_name TEXT,
            user2_name TEXT,
            married_at TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        )
    ''')
    
    # Таблица действий
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS actions (
            id INTEGER PRIMARY KEY,
            from_user_id INTEGER,
            to_user_id INTEGER,
            action_type TEXT,
            created_at TIMESTAMP
        )
    ''')
    
    conn.commit()
    return conn

# СЛОВАРИ ДЕЙСТВИЙ
SEX_ACTIONS = [
    "выебал", "оттрахал", "занялся сексом с", "устроил ночь любви с",
    "испытал койку с", "занялся аналом с", "отсосал у", "отлизал у",
    "устроил оргию с", "поиграл в игрушки с", "устроил BDSM с",
    "снял на камеру секс с", "устроил групповуху с", "поцеловал в губы",
    "ласкал грудь у", "сделал минет у", "устроил стриптиз для"
]

VIOLENCE_ACTIONS = [
    "ударил", "отпиздил", "избил", "поколотил", "навалял", "выебал в жопу",
    "опустил", "унизил", "разобрал", "размазал", "уделал", "уничтожил",
    "обосрал", "затроллил", "поставил на место", "дал в рот", "отшлепал",
    "пнул", "дал по щам", "сломал ребра"
]

LOVE_ACTIONS = [
    "поженился на", "обручился с", "встречается с", "влюбился в",
    "сделал предложение", "поцеловал", "обнял", "признался в любви",
    "подарил цветы", "устроил свидание с", "пошел в кино с",
    "устроил романтический ужин с", "подарил кольцо", "снял комнату с"
]

FRIENDSHIP_ACTIONS = [
    "подружился с", "запездюлил", "затусил с", "выпил с", 
    "поиграл в игры с", "сходил в бар с", "устроил вечеринку с",
    "посмотрел фильм с", "заказал пиццу с", "сыграл в карты с"
]

WEIRD_ACTIONS = [
    "запездюлил", "закопал на даче", "продал в рабство", 
    "украл трусы у", "подмешал слабительное", "подставил",
    "сдал ментам", "устроил засаду на", "взорвал машину",
    "поджег дом", "отравил", "загипнотизировал"
]

# КОМАНДЫ БОТА
COMMANDS = {
    "💍 ОТНОШЕНИЯ": {
        "поженить @юзер": "Создать брак с пользователем",
        "развестись": "Расторгнуть текущий брак", 
        "отношения": "Показать ваши отношения",
        "парочки": "Показать все браки в чате",
        "статистика": "Статистика по бракам"
    },
    "🔞 СЕКС": {
        "выебать @юзер": "Совершить секс-действие",
        "отсосать @юзер": "Оральные ласки",
        "секс лидеры": "Топ самых сексуальных",
        "мой секс стат": "Твоя секс-статистика"
    },
    "👊 НАСИЛИЕ": {
        "ударить @юзер": "Применить физическое воздействие",
        "отпиздить @юзер": "Сильно избить",
        "унизить @юзер": "Морально уничтожить",
        "топ насилия": "Топ самых жестоких"
    },
    "❤️ ЛЮБОВЬ": {
        "влюбиться @юзер": "Начать отношения",
        "обнять @юзер": "Проявить нежность", 
        "подарить @юзер": "Сделать подарок"
    },
    "🎉 ДРУЖБА": {
        "запездюлить @юзер": "Совместные развлечения",
        "затусить @юзер": "Провести время вместе"
    },
    "🤪 ПРИКОЛЫ": {
        "закопить @юзер": "Спрятать пользователя",
        "продать @юзер": "Отправить в рабство"
    },
    "📊 ИНФО": {
        "команды": "Показать все команды",
        "статус": "Проверить работу бота",
        "топ активности": "Общий топ по действиям"
    }
}

class RelationshipBot:
    def __init__(self):
        self.db = get_db()
    
    def get_user_name(self, user):
        """Получить имя пользователя"""
        if user.get('username'):
            return f"@{user['username']}"
        elif user.get('first_name'):
            return user['first_name']
        else:
            return f"User{user['id']}"
    
    def get_marriage(self, user_id):
        """Найти активный брак пользователя"""
        cursor = self.db.cursor()
        cursor.execute('''
            SELECT * FROM marriages 
            WHERE (user1_id = ? OR user2_id = ?) AND is_active = TRUE
        ''', (user_id, user_id))
        return cursor.fetchone()
    
    def create_marriage(self, user1, user2):
        """Создать брак"""
        cursor = self.db.cursor()
        
        # Проверим, не женат ли кто-то уже
        existing1 = self.get_marriage(user1['id'])
        existing2 = self.get_marriage(user2['id'])
        
        if existing1 or existing2:
            return False, "❌ Один из пользователей уже в браке!"
        
        cursor.execute('''
            INSERT INTO marriages (user1_id, user2_id, user1_name, user2_name, married_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (user1['id'], user2['id'], self.get_user_name(user1), 
              self.get_user_name(user2), datetime.now()))
        
        self.db.commit()
        return True, f"💍 {self.get_user_name(user1)} и {self.get_user_name(user2)} теперь муж и жена!"
    
    def divorce(self, user_id):
        """Развестись"""
        marriage = self.get_marriage(user_id)
        if not marriage:
            return False, "❌ Ты не в браке!"
        
        cursor = self.db.cursor()
        cursor.execute('''
            UPDATE marriages SET is_active = FALSE WHERE id = ?
        ''', (marriage[0],))
        
        self.db.commit()
        return True, f"💔 Брак между {marriage[3]} и {marriage[4]} расторгнут!"
    
    def log_action(self, from_user, to_user, action_type):
        """Записать действие в базу"""
        cursor = self.db.cursor()
        cursor.execute('''
            INSERT INTO actions (from_user_id, to_user_id, action_type, created_at)
            VALUES (?, ?, ?, ?)
        ''', (from_user['id'], to_user['id'], action_type, datetime.now()))
        self.db.commit()
    
    def get_relationship_info(self, user_id):
        """Информация об отношениях пользователя"""
        marriage = self.get_marriage(user_id)
        if not marriage:
            return "💔 Ты одинок как пердеж в ветреную погоду"
        
        married_at = datetime.strptime(marriage[5], '%Y-%m-%d %H:%M:%S.%f')
        duration = datetime.now() - married_at
        
        partner_name = marriage[4] if marriage[1] == user_id else marriage[3]
        
        return f"""💍 Ты в браке с {partner_name}
📅 Вместе уже: {duration.days} дней
💕 Состоялись: {married_at.strftime('%d.%m.%Y')}"""
    
    def get_all_marriages(self):
        """Список всех активных браков"""
        cursor = self.db.cursor()
        cursor.execute('''
            SELECT * FROM marriages WHERE is_active = TRUE
        ''')
        marriages = cursor.fetchall()
        
        if not marriages:
            return "💔 В чате пока нет браков"
        
        result = "💍 АКТИВНЫЕ БРАКИ:\n\n"
        for marriage in marriages:
            married_at = datetime.strptime(marriage[5], '%Y-%m-%d %H:%M:%S.%f')
            duration = datetime.now() - married_at
            result += f"{marriage[3]} + {marriage[4]} - {duration.days} дней\n"
        
        return result
    
    def get_action_stats(self, action_type=None):
        """Статистика по действиям"""
        cursor = self.db.cursor()
        
        if action_type:
            cursor.execute('''
                SELECT from_user_id, COUNT(*) as count 
                FROM actions 
                WHERE action_type = ?
                GROUP BY from_user_id 
                ORDER BY count DESC 
                LIMIT 10
            ''', (action_type,))
        else:
            cursor.execute('''
                SELECT from_user_id, COUNT(*) as count 
                FROM actions 
                GROUP BY from_user_id 
                ORDER BY count DESC 
                LIMIT 10
            ''')
        
        return cursor.fetchall()

def handle_event(event):
    """Основной обработчик событий"""
    try:
        bot = RelationshipBot()
        
        # Парсим событие от Telegram
        body = json.loads(event['body'])
        message = body.get('message', {})
        text = message.get('text', '')
        from_user = message.get('from', {})
        chat = message.get('chat', {})
        reply_to = message.get('reply_to_message', {})
        
        # Игнорируем сообщения не из чатов
        if chat.get('type') not in ['group', 'supergroup']:
            return {'statusCode': 200}
        
        # Обработка команд
        if text.startswith('/'):
            command = text.split('@')[0].lower()  # Убираем упоминание бота
            
            if command == '/команды':
                response = format_commands()
                
            elif command == '/поженить':
                if reply_to:
                    success, msg = bot.create_marriage(from_user, reply_to['from'])
                    response = msg
                else:
                    response = "❌ Ответь на сообщение пользователя, на котором хочешь жениться!"
                    
            elif command == '/развестись':
                success, msg = bot.divorce(from_user['id'])
                response = msg
                
            elif command == '/отношения':
                response = bot.get_relationship_info(from_user['id'])
                
            elif command == '/парочки':
                response = bot.get_all_marriages()
                
            # Секс команды
            elif command in ['/выебать', '/отсосать']:
                if reply_to:
                    action = random.choice(SEX_ACTIONS)
                    response = f"🔞 {bot.get_user_name(from_user)} {action} {bot.get_user_name(reply_to['from'])}"
                    bot.log_action(from_user, reply_to['from'], 'sex')
                else:
                    response = "❌ Ответь на сообщение пользователя!"
                    
            # Насилие команды        
            elif command in ['/ударить', '/отпиздить', '/унизить']:
                if reply_to:
                    action = random.choice(VIOLENCE_ACTIONS)
                    response = f"👊 {bot.get_user_name(from_user)} {action} {bot.get_user_name(reply_to['from'])}"
                    bot.log_action(from_user, reply_to['from'], 'violence')
                else:
                    response = "❌ Ответь на сообщение пользователя!"
                    
                    # Дружба команды
            elif command in ['/запездюлить', '/затусить']:
                if reply_to:
                    action = random.choice(FRIENDSHIP_ACTIONS)
                    response = f"🎉 {bot.get_user_name(from_user)} {action} {bot.get_user_name(reply_to['from'])}"
                    bot.log_action(from_user, reply_to['from'], 'friendship')
                else:
                    response = "❌ Ответь на сообщение пользователя!"
                    
            # Любовь команды
            elif command in ['/влюбиться', '/обнять', '/подарить']:
                if reply_to:
                    action = random.choice(LOVE_ACTIONS)
                    response = f"❤️ {bot.get_user_name(from_user)} {action} {bot.get_user_name(reply_to['from'])}"
                    bot.log_action(from_user, reply_to['from'], 'love')
                else:
                    response = "❌ Ответь на сообщение пользователя!"
                    
            # Приколы команды
            elif command in ['/закопать', '/продать']:
                if reply_to:
                    action = random.choice(WEIRD_ACTIONS)
                    response = f"🤪 {bot.get_user_name(from_user)} {action} {bot.get_user_name(reply_to['from'])}"
                    bot.log_action(from_user, reply_to['from'], 'weird')
                else:
                    response = "❌ Ответь на сообщение пользователя!"
                    
            # Статистика команды
            elif command == '/секс лидеры':
                stats = bot.get_action_stats('sex')
                response = format_stats(stats, "🔞 ТОП СЕКСУАЛЬНЫХ", bot)
                
            elif command == '/топ насилия':
                stats = bot.get_action_stats('violence')
                response = format_stats(stats, "👊 ТОП НАСИЛЬНИКОВ", bot)
                
            elif command == '/топ активности':
                stats = bot.get_action_stats()
                response = format_stats(stats, "📊 ТОП АКТИВНОСТИ", bot)
                
            elif command == '/мой секс стат':
                user_stats = get_user_action_stats(from_user['id'], 'sex', bot)
                response = f"🔞 Твоя секс-статистика:\n{user_stats}"
                
            elif command == '/статистика':
                cursor = bot.db.cursor()
                cursor.execute('SELECT COUNT(*) FROM marriages WHERE is_active = TRUE')
                active_marriages = cursor.fetchone()[0]
                cursor.execute('SELECT COUNT(*) FROM marriages WHERE is_active = FALSE')
                divorced = cursor.fetchone()[0]
                cursor.execute('SELECT COUNT(*) FROM actions')
                total_actions = cursor.fetchone()[0]
                
                response = f"""📊 СТАТИСТИКА ЧАТА:
💍 Активных браков: {active_marriages}
💔 Расторгнуто: {divorced}
🎭 Всего действий: {total_actions}"""
                
            elif command == '/статус':
                response = "✅ Бот работает исправно! Используй /команды"
                
            else:
                response = "❌ Неизвестная команда. Используй /команды"
            
            # Отправляем ответ
            return send_telegram_message(chat['id'], response)
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500}
    
    return {'statusCode': 200}

def format_commands():
    """Форматирует список команд"""
    result = "📋 ДОСТУПНЫЕ КОМАНДЫ:\n\n"
    for category, commands in COMMANDS.items():
        result += f"{category}:\n"
        for cmd, desc in commands.items():
            result += f"/{cmd} - {desc}\n"
        result += "\n"
    return result

def format_stats(stats, title, bot):
    """Форматирует статистику"""
    if not stats:
        return f"{title}:\n📊 Пока нет статистики"
    
    result = f"{title}:\n\n"
    for i, (user_id, count) in enumerate(stats, 1):
        # В реальном боте нужно хранить имена пользователей
        result += f"{i}. User{user_id}: {count} раз\n"
    
    return result

def get_user_action_stats(user_id, action_type, bot):
    """Статистика действий пользователя"""
    cursor = bot.db.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM actions 
        WHERE from_user_id = ? AND action_type = ?
    ''', (user_id, action_type))
    count = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT COUNT(*) FROM actions 
        WHERE to_user_id = ? AND action_type = ?
    ''', (user_id, action_type))
    received = cursor.fetchone()[0]
    
    return f"👤 Совершено: {count} раз\n🎯 Получено: {received} раз"

def send_telegram_message(chat_id, text):
    """Отправляет сообщение в Telegram"""
    import requests
    
    bot_token = os.environ['BOT_TOKEN']
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    
    payload = {
        'chat_id': chat_id,
        'text': text,
        'parse_mode': 'HTML'
    }
    
    requests.post(url, json=payload)
    return {'statusCode': 200}

# Lambda handler
def lambda_handler(event, context):
    return handle_event(event)

# Для локального тестирования
if __name__ == "__main__":
    # Тестовое событие
    test_event = {
        'body': json.dumps({
            'message': {
                'chat': {'id': 123, 'type': 'group'},
                'from': {'id': 1, 'first_name': 'TestUser', 'username': 'testuser'},
                'text': '/команды'
            }
        })
    }
    print(handle_event(test_event))