import time
import requests
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import threading
from threading import Thread
import logging
import signal
import msvcrt
import time
import platform
import select
import sys
from datetime import datetime

shutdown_event = threading.Event()

# для ввода команды из консаоли используется механизм не-блокирующего ввода
# для разных платформ решения отличаются

def check_input():
    if platform.system() == 'Windows':
        windows_check_input()
    else:
        unix_check_input()

# не-блокирующий ввод на Windows

def windows_check_input():
    print("Запущен поток thread_export_to_excel (windows)")
    print("Консоль ожидает ввода буквы 'e' для экспорта в Excel или q для выхода")
    while not shutdown_event.is_set():
        if msvcrt.kbhit():
            try :
                command = msvcrt.getch().decode()
                if command.lower() == 'e':  # 'e' для 'export'
                    export_to_excel()
                if command.lower() == 'q':  # 'q' для 'quit'
                    shutdown_event.set()
            except :
                logging.info("Ошибка в команде")

        time.sleep(1) 


# не-блокирующий ввод на Unix

def unix_check_input():
    print("Запущен поток thread_export_to_excel (unix)")
    print("Консоль ожидает ввода буквы 'e' для экспорта в Excel или q для выхода")
    while not shutdown_event.is_set():
        readable, _, _ = select.select([sys.stdin], [], [], 1)
        if readable:
            command = sys.stdin.readline().strip()
            if command.lower() == 'e':   # 'e' для 'export'
                export_to_excel()
            elif command.lower() == 'q':  # 'q' для 'quit'
                shutdown_event.set()
            else :
                logging.info("Ошибка в команде")



def signal_handler(signum, frame):
    logging.info("Принят сигнал для завершения работы.")
    shutdown_event.set()

# обработка внешних сигналов
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# инициализация базы 
Base = declarative_base()

# формат записи в базу
class WeatherData(Base):

    __tablename__ = 'weather'

    # уникальный идентификатор записи
    id = Column(Integer, primary_key=True)

    # время записи данных о погоде
    timestamp = Column(DateTime, nullable=False)

    # температура воздуха в градусах Цельсия
    temperature = Column(Float, nullable=False)

    # направление ветра (например, С, СВ, ЮВ)
    wind_direction = Column(String(10), nullable=False)

    # скорость ветра в метрах в секунду
    wind_speed = Column(Float, nullable=False)

    # атмосферное давление в мм рт. ст.
    pressure = Column(Float, nullable=False) 

    # осадки (тип и количество, например "снег, 2 мм")
    precipitation = Column(String(50), nullable=False)


def init_db(uri='sqlite:///weather.db'):
    engine = create_engine(uri)
    Base.metadata.create_all(engine)
    return engine


# создание движка SQLAlchemy и сессии для работы с БД
engine = init_db()
Session = sessionmaker(bind=engine)

def wind_direction_from_angle(angle):
    directions = ['С', 'СВ', 'В', 'ЮВ', 'Ю', 'ЮЗ', 'З', 'СЗ', 'С']
    idx = round(angle / 45) % 8
    return directions[idx]

# преобразование строки времени в объект `datetime`
def convert_to_datetime(timestamp_str):
    return datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M")


# преобразование давления из гПа в мм рт. ст.
def convert_pressure_to_mm_hg(pressure_hpa):
    return pressure_hpa * 0.75006375541921

# запись в базу 
def save_request(response):
    try:
        current_weather = response['current_weather']
        wind_direction_angle = current_weather['winddirection']
        wind_direction_name = wind_direction_from_angle(wind_direction_angle)

        new_record = WeatherData(
            
            timestamp=convert_to_datetime(current_weather['time']),
            temperature=current_weather['temperature'], 
            wind_direction=wind_direction_name,
            wind_speed=current_weather['windspeed'],
            # не получены в JSON, устаревшая документация
            pressure='0', # convert_pressure_to_mm_hg(current_weather['pressure_msl']),
            precipitation="no"  
        )

        # Использование контекстного менеджера для управления сессией
        with Session() as session:
            session.add(new_record)
            session.commit()
            logging.info("Новая запись успешно добавлена.")
    except SQLAlchemyError as e:
        logging.error(f"Ошибка при добавлении записи в базу данных: {e}")
    except KeyError as e:
        logging.error(f"Недостающее поле в ответе API: {e}")

# сетевой запрос погоды
def fetch_weather_data(api_url):

    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            #print(response.json())
            return response.json()
        else:
            logging.warning(f"Ошибка. Код ответа: {response.status_code}")
            return None
    except requests.RequestException as e:
        logging.error(f"Исключение при запросе к API: {e}")
        return None

# функция погодного потока  
def thread_weather_data():

    lat = "55.69674"
    lon = "37.35283"
    timezone = "Europe/Moscow"
    initial_sleep = 180

    API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&timezone={timezone}&current_weather=true&pressure"
    error_count = 0
    sleep = initial_sleep
    print("Запущен поток thread_weather_data")
    while not shutdown_event.is_set():
        data = fetch_weather_data(API_URL)
        if data:
            save_request(data)
            error_count = 0
            sleep = initial_sleep
        else:
            error_count += 1
            if error_count >= 5:
                sleep += int(sleep / 10)
                logging.info(
                    f"Увеличение интервала опроса до {sleep} секунд из-за ошибок.")

        # спать весь интервал sleep нельзя, т.к. может быть команда на завершение
        # ожидаем 1 секунду и проверяем shutdown_event
        sleep_counter = sleep
        while not shutdown_event.is_set() and sleep_counter:
            sleep_counter -=1
            time.sleep(1)


# экспорт данных в Excel


def export_to_excel():

    try:
        with Session() as session:
            data = session.query(WeatherData).order_by(
                WeatherData.id.desc()).limit(10).all()
            df = pd.DataFrame([{
                'Timestamp': record.timestamp,
                'Temperature': record.temperature,
                'Wind Direction': record.wind_direction,
                'Wind Speed': record.wind_speed,
                'Pressure': record.pressure,
                'Precipitation': record.precipitation
            } for record in data])
            df.to_excel('weather_data.xlsx', index=False)
            logging.info("Данные успешно экспортированы в файл 'weather_data.xlsx'")
            
    except Exception as e:
        logging.error(f"Ошибка при экспорте данных: {e}")



# запуск асинхронных задач

# 
weather_thread = Thread(target=thread_weather_data)
weather_thread.start()

check_thread = Thread(target=check_input)
check_thread.start()



# Ожидание завершения потоков
weather_thread.join()
check_thread.join()
print("Все потоки завершили работу.")