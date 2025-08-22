

import requests, json, os, logging, glob, time, sys, argparse
from datetime import datetime, timedelta, timezone
from tzlocal import get_localzone
import configparser
import psycopg2
from psycopg2.extras import RealDictCursor

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import smtplib
from email.message import EmailMessage

"""
скрипт автоматической выгрузки данных
использование: get_grader_info.py [-h] [--start START] [--end END]
опциональные параметры:
  -h, --help     show this help message and exit
  --start START  Дата-время начала выгрузки данных в формате YYYY-MM-DD HH:MM:SS (default: None)
  --end END      Дата-время конца выгрузки данных в формате YYYY-MM-DD HH:MM:SS (default: None)
"""

class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                      argparse.ArgumentDefaultsHelpFormatter):
    pass


def parse_args(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    parser.add_argument("--start", type=str, help="Дата-время начала выгрузки данных в формате YYYY-MM-DD HH:MM:SS ")
    parser.add_argument("--end", type=str,  help="Дата-время конца выгрузки данных в формате YYYY-MM-DD HH:MM:SS")
    return parser.parse_args(args)

def drop_logging():
    logger = logging.getLogger() # получение корневого объекта logger
    # Удаляем все предыдущие обработчики
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)
        # Также удаляем обработчики у всех дочерних логгеров
        for handler in logger.handlers:
            if handler in logger.handlers:
                logger.removeHandler(handler)

def start_logging(work_path = os.getcwd()): 
    os.chdir(work_path)
    log_path = os.path.abspath('log') 
    dt = datetime.now().date()
    log_file = f'log{datetime.strftime(dt, '%Y%m%d')}.log'
    date_3d = (dt - timedelta(days = 3)) # дата проверки
    # настройка логирования
    logging.basicConfig(level=logging.INFO, filename=os.path.join(log_path, log_file), filemode="a", format='%(asctime)s: %(name)s - %(levelname)s: %(message)s')
    logging.info("Начало работы скрипта ...")

    # создаём папку если её нет, получаем список log файлов, проверяем дату создания и удаляем если старше ...
    if not os.path.exists(log_path):
        os.mkdir('log')
    os.chdir('log')
    filelist = glob.glob('*.log')  
    for file in filelist:
        try:
            file_crtime = datetime.fromtimestamp(os.path.getctime(file)).date()
            if file_crtime < date_3d:
                os.remove(file)
                logging.info(f"Лог файл {file} : дата создания - {file_crtime} удален")
        except Exception as err:
            logging.error(f'Ошибка удаления лог файла {file}')
    os.chdir(work_path)

def load_config(filename='config.ini', section='Database'):
    parser = configparser.ConfigParser()
    parser.read(filename)
    # считываем секцию, секция по умолчанию Database
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception('Секция {0} не найдена в файле {1} '.format(section, filename))
    return config

class DataBase:
    __instance = None
    connection = None
    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = super().__new__(cls)
        return cls.__instance
            
    def __init__(self, **params): # host, port, database, user, password, autocommit=False):
        self.connection = psycopg2.connect(
            host=params['host'],
            port=params['port'],
            database=params['database'],
            user=params['user'],
            password=params['password'],
        )
        DataBase.__instance = self    
        if params['autocommit'].strip().lower() == 'true':
            self.connection.autocommit = True   
        self.cursor = self.connection.cursor()      

    @classmethod
    def get_instance(cls):
        if cls.__instance is None :    
            cls.__instance = super().__new__(cls)
        else:
            return cls.__instance    
        return cls.__instance           
     
    def close_connection(self):
        if self.connection:
            try:
                self.cursor.close()
            except:
                pass    
            self.connection.close()  
        DataBase.__instance = None

    def select(self, query, vars=None, cursor_factory = None):          
        if cursor_factory:
            self.cursor.close()
            self.cursor = self.connection.cursor(cursor_factory=cursor_factory)
        self.cursor.execute(query, vars)
        rs = self.cursor.fetchall()
        return rs
    
    def insert(self, query, vars=None):
        self.cursor.execute(query, vars)
        if not self.connection.autocommit:
            self.connection.commit()        


def get_user_activity(url, **params):
    try:
        rsp = requests.get(url,params=params).json()
        tbl = []
        for row in rsp:
            if row['passback_params']:
                pp = json.loads(row['passback_params'].replace('\'', '\"'))
            if row['lti_user_id']: 
                tbl.append((row['lti_user_id'], pp.get('oauth_consumer_key', None), pp.get('lis_result_sourcedid', None),pp.get('lis_outcome_service_url', None), row['is_correct'], row['attempt_type'], row['created_at']))
            else:
                logging.warning('Отсутствует user_id ')
        return tbl        
    except Exception as err:
        logging.error(f'Ошибка чтения API: {err}')


def get_spreadsheet(key, spreadsheet):
    scope = ['https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive']
    gc = gspread.service_account(os.path.join(os.getcwd(),key), scope)
    return gc.open(spreadsheet).sheet1


def send_email(smtp_server, smtp_port, sender, password, recipient, subject, message):
    msg = EmailMessage()
    msg.set_content(message)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient

    try:
        with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
            server.login(sender, password)
            server.send_message(msg=msg)
            logging.info("Email отправлен")
    except Exception as err:
        logging.error(f"Ошибка отправки email: {err}")





# ====================================
# Предположим что скрипт запускаем в первый час нового дня по UTC без параметров и выгружаем данные за полный предыдущий день
# Если задана одна дата, то формируем диапазон за это день, если две, то диапазон -  начало дня первой и конец дня второй 

# считываем аргументы командной строки в словарь
args =  vars(parse_args())

s_dt = args.get('start', '')
e_dt = args.get('end', '')

# формируем диапазон времени для выгрузки данных
if len(sys.argv) == 1:
    dt = datetime.now()
    dt_end = dt.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    dt_start = dt_end.replace(hour=0, minute=0, second=0, microsecond=0)

else:
    if len(s_dt)>=10:
        try:
            dt_start = datetime.fromisoformat(s_dt)
            tstart = dt_start.isoformat(sep=' ')
        except:
            logging.error('Неправильный формат даты')
            logging.info('Завершение работы скрипта')
            sys.exit()
    if len(e_dt)>=10:
        try:
            dt_end = datetime.fromisoformat(e_dt) 
            if dt_start >= dt_end:
                logging.error('Дата начала равна или больше даты окончания')
                logging.info('Завершение работы скрипта')                
                sys.exit()
            else:
                tstop = dt_end.isoformat(sep=' ') 
                if len(e_dt) == 10:
                    dt_end = dt_end + timedelta(days=1) - timedelta(microseconds=1)
        except:
            logging.error('Неправильный формат даты')
            logging.info('Завершение работы скрипта') 
            sys.exit() 
    else:
        if dt_start:
            dt_end = dt_start.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1) - timedelta(microseconds=1)    
                               
tstart = dt_start.isoformat(sep=' ')
tstop = dt_end.isoformat(sep=' ')

mail_message = 'Итоги работы скрипта выгрузки:\n'

# чтение параметров подключения к базе
db_config = load_config()

# чтение параметров подключения к API
api_url = load_config(section = 'API')
api_params = load_config(section = 'API parameters')
api_params['start'] = tstart
api_params['end'] = tstop

# Сброс и настройка логирования
drop_logging()
start_logging()  

# подключение к API
logging.info('Подключение к API')
ua = get_user_activity(api_url['api_url'], **api_params)
logging.info('Завершение работы с API')

if ua:
    # подключение к базе
    try:
        db = DataBase(**db_config)
        logging.info('Соединение c БД установлено')
    except Exception as err:
        logging.error(f'Ошибка работы с БД: {err}')

if db:
    # очистить данные за период
    qr = f""" delete from grade.lms_grage_statistics where date(created_at) between date('{tstart}') and date('{tstop}') ; """
    try:
        db.insert(qr)
        logging.info(f'Очистка данных в БД, удалено записей {db.cursor.rowcount}')
    except (Exception, psycopg2.Error) as err:
        logging.error(f'Ошибка {err}')
    # запись новых данных
    by_line = False
    data_str = ','.join(db.cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s)", x).decode('utf-8') for x in ua)
    qr = """ insert into grade.lms_grage_statistics (user_id, oauth_consumer_key, lis_result_sourcedid, lis_outcome_service_url,
             is_correct, attempt_type ,created_at) values """ + data_str
    qr_s = u""" insert into grade.lms_grage_statistics values(%s, %s, %s, %s, %s, %s, %s) """ 
    try:
        db.insert(qr)
    except (Exception, psycopg2.Error) as err:
        logging.error(f'Ошибка массовой загрузки {err}')
        db.connection.rollback()
        by_line = True
    else: 
        logging.info('Массовая загрузка данных в БД завершена')   
    # если при массовой загрузке происходит ошибка пытаемся загружать данные построчно
    if by_line:
        for line in ua:
            try:
                db.insert(qr_s, line)
            except Exception as err:
                logging.error(f'Ошибка загруки строки: {err}')
        logging.info('Последовательная загрузка данных в БД завершена')

    # подготовка итогов для загрузки в google sheets
    qr = f"""select 
                min(date(created_at))::VARCHAR as "Дата",
                count(distinct user_id) as "Пользователей",
                count(case attempt_type when 'submit' then 1 end ) as "Попыток решения всего", 
                sum(case attempt_type when 'submit' then is_correct end ) as "Успешных решений", 
                0 as "% успешных решений",
                count(attempt_type) as "Всего запусков кода"
            from grade.lms_grage_statistics 
            where date(created_at) = date('{tstart}')"""
    logging.info("Подготовка итогов дня к выгрузке")
    try:
        data_to_gs = db.select(qr)
        gs_columns = [desc[0] for desc in db.cursor.description]
        logging.info("Итоги дня подготовлены")
    except (Exception, psycopg2.Error) as err:
        logging.error(f'Ошибка {err}')

    qr = f"""select count(*) from grade.lms_grage_statistics where date(created_at) = date('{tstart}')"""
    try:
        srs = db.select(qr)
    except (Exception, psycopg2.Error) as err:
        logging.error(f'Ошибка {err}')

    db.close_connection()
    logging.info('Соединение с БД закрыто')

    # заполнение итогов в google sheets
    logging.info('Подключение к google sheets')
    api_gs = load_config(section = 'google sheets api')
    wks = get_spreadsheet(os.path.join(os.getcwd(),api_gs['credentials']), api_gs['file']) 
    #Format Header
    wks.format("A1:F1", {
        "backgroundColor": {"red": 0.7, "green": 0.8, "blue": 0.9},
        "horizontalAlignment": "CENTER",
        "textFormat": {"foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.2}, "fontSize": 12, "bold": True }
    })
    # Запись наименований колонок
    for i, c in enumerate(gs_columns, 1):
        wks.update_cell(1, i, c)
    # подготовка строки
    row = data_to_gs[0]
    index = 2
    # вставка строки
    wks.insert_row(row , index)
    wks.format('A2:F2',{"horizontalAlignment": "CENTER",'borders': {'top': {'style': 'SOLID'}, 'left': {'style': 'SOLID'}, 'right': {'style': 'SOLID'}, 'bottom': {'style': 'SOLID'} }})
    wks.update_acell('E2','=iferror(D2/C2, 0)')
    logging.info('Итоги записаны в таблицу google sheets')

    # отправка email 
    dt = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M')
    smtp_config = load_config(section = 'SMTP')
    smtp_config['subject'] = f'Выгрузка grade от {dt}'
    smtp_config['message'] = mail_message + f'Выгружено {str(srs[0][0])} записей на дату {datetime.strftime(dt_end, '%Y-%m-%d')}.'

    send_email(**smtp_config)

    logging.info('Процесс выгрузки завершен')
    logging.shutdown()  

