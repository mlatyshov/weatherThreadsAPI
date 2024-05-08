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
    print("Программа weatherThreads выполняет сбор погодной информации.")
    print("Ожидается нажатия буквы латинской 'e' для экспорта в Excel или 'q' для выхода")
    while not shutdown_event.is_set():
        if msvcrt.kbhit():
            try :
                command = msvcrt.getch().decode()
                if command.lower() == 'e':  # 'e' для 'export'
                    print("Экспортирую..")
                    export_to_excel()
                if command.lower() == 'q':  # 'q' для 'quit'
                    print("Завершаю работу..")
                    shutdown_event.set()
            except :
                logging.info("Ошибка в команде")

        time.sleep(1) 


# не-блокирующий ввод на Unix

def unix_check_input():
    print("Запущен поток thread_export_to_excel (unix)")
    print("Программа weatherThreads выполняет сбор погодной информации.")
    print("Ожидается нажатия буквы латинской 'e' для экспорта в Excel или 'q' для выхода")
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
def convert_pressure_to_mm_hg(pressure_hpa, orig_unit):
    if orig_unit == 'hPa' :
        return pressure_hpa * 0.75006375541921
    else :
        return pressure_hpa


# Преобразование скорости ветра из км/ч в м/с
def convert_wind_speed_to_m_s(wind_speed_kmh, orig_unit):
    if orig_unit == 'km/h' :
        return wind_speed_kmh / 3.6
    else : 
        return wind_speed_kmh

# запись в базу 
def save_request(response1, response2):
    try:
        weather = response1['current_weather']
        weather_units = response1['current_weather_units']

        pressure = response2['current']
        pressure_units = response2['current_units']
        
        wind_direction_angle = weather['winddirection']
        wind_direction_name = wind_direction_from_angle(wind_direction_angle)
        
        new_record = WeatherData(
            
            timestamp=convert_to_datetime(weather['time']),
            temperature=weather['temperature'], 
            wind_direction=wind_direction_name,
            wind_speed=convert_wind_speed_to_m_s(weather['windspeed'], weather_units['windspeed']),
            pressure=convert_pressure_to_mm_hg(pressure['surface_pressure'], pressure_units['surface_pressure']),
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

    API_URL = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&timezone={timezone}"
    error_count = 0
    sleep = initial_sleep
    print("Запущен поток thread_weather_data")
    while not shutdown_event.is_set():
        data1 = fetch_weather_data(API_URL + '&current_weather=true')
        data2 = fetch_weather_data(API_URL + '&current=surface_pressure')
        if data1 and data2:
            save_request(data1, data2)
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