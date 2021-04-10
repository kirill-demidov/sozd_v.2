import threading
from threading import Lock
import datetime
from requests.auth import HTTPBasicAuth
import users
import issues
import issues_worklog
import groups_users
import issues_filed
import projects_roles_actors
import postgres_etl
import psycopg2
import time
import Scripts_DB_Oblects
import commonthread

lock = Lock()
is_live = True
connection = None
# host_db = 'bi-postgres'
host_db = '127.0.0.1'
port_db = 5432
name_db = 'postgres'
user_name = 'postgres'
password = 'password'
# password = 'postgres'
#  дискретность (для суток 86400, один час - 3600)
discret = 86400
# начальная точка отсчета дискретностей (7200 - 2 часа ночи)
first_time = 7200

t_0 = None


def txt_result(error):
    if error:
        return "ERROR"
    else:
        return 'INFO'


def write_log(level: str, src: str, msg: str, with_out_lf = False):
    st = "lvl=" + level + ' ' + 'src="' + str(src).replace('"', "'") + '" msg="' + str(msg).replace('"', "'") + '"'
    # lock.acquire()
    if with_out_lf:
        print("\r" + st + '                               ', end="\r")
    else:
        print(st)
    # lock.release()


def get_value_time(t):
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond // 1000 / 1000


def print_dest(t_st):
    t_end = datetime.datetime.now()
    t = get_value_time(t_end) - get_value_time(t_st)
    t0 = get_value_time(t_end) - get_value_time(commonthread.t_0)
    return '; T pipeline = ' + "%.3f" % t + ' sec; Passed = ' + "%.3f" % t0 + ' sec'


def utc_local(utc: datetime) -> datetime:
    epoch = time.mktime(utc.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return utc + offset


def isNull(val, default=0):
    if not val:
        return default
    else:
        return val


class TImport(threading.Thread):
    needStop = False
    connection = None

    def __init__(self, conn):
        threading.Thread.__init__(self)
        self.daemon = True
        self.connection = conn

    def make_work(self):
        write_log('INFO', 'START', time.ctime())
        auth = HTTPBasicAuth("k.demidov@alterosmart.com", "L99Ib8xsuFJKtvTn8SpM8F3C")
        commonthread.t_0 = datetime.datetime.now()
        t_st = datetime.datetime.now()
        error, result = users.jira_users(auth, connection)
        write_log(txt_result(error), 'users', result + print_dest(t_st))
        # send(messages=[time.ctime()," users: " + result + print_dest(t_st)])
        if not error:
            t_st = datetime.datetime.now()
            error, result = groups_users.group_users(auth, connection)
            write_log(txt_result(error), 'groups_users', result + print_dest(t_st))
            # send(messages=[time.ctime()," groups_users: " + result + print_dest(t_st)])
            if not error:
                t_st = datetime.datetime.now()
                error, result = projects_roles_actors.project_roles_actors(auth, connection)
                write_log(txt_result(error), 'projects_roles_actor', result + print_dest(t_st))
                #  send(messages=[time.ctime(), " projects_roles_actors: " + result + print_dest(t_st)])
                if not error:
                    t_st = datetime.datetime.now()
                    error, result = issues_filed.issues_fields(auth, connection)
                    write_log(txt_result(error), 'issues_filed', result + print_dest(t_st))
                    #  send(messages=[time.ctime(), " issues_filed: " + result + print_dest(t_st)])
                    if not error:
                        t_st = datetime.datetime.now()
                        error, result = issues.issues(auth, connection)
                        write_log(txt_result(error), 'issues', result + print_dest(t_st))
                        #  send(messages=[time.ctime(), " issues: " + result + print_dest(t_st)])
                        if not error:
                            t_st = datetime.datetime.now()
                            error, result = issues_worklog.worklog(auth, connection)
                            write_log(txt_result(error), 'worklog', result + print_dest(t_st))
                            #  send(messages=[time.ctime(), " worklog: " + result + print_dest(t_st)])
                            if not error:
                                t_st = datetime.datetime.now()
                                error, result = postgres_etl.postgres_etl(connection)
                                write_log(txt_result(error), 'postgres_etl', result + print_dest(t_st))
                                # send(messages=[time.ctime(), " postgres_etl: " + result + print_dest(t_st)])
        t_st = datetime.datetime.now()
        t0 = get_value_time(t_st) - get_value_time(commonthread.t_0)
        write_log('INFO', 'FINISH', time.ctime() + '; Passed = ' + "%.3f" % t0 + ' sec')
        # send(messages=[time.ctime(), " main time: " + result + print_dest(t_st)])

    def run(self):  # после запуска потока выполняется эта процедура
        try:
            while not self.needStop:
                self.make_work()  # в этой функции основная работа
                # определим момент следующего запуска
                tek_time = datetime.datetime.now()
                dt = tek_time.hour * 3600 + tek_time.minute * 60 + tek_time.second  # текущая секунда в сутках
                tek_time = tek_time + datetime.timedelta(0, -dt)  # начало суток
                if dt > first_time:  # до следующих суток ( после начального времени)
                    next_time = tek_time + datetime.timedelta(1, first_time)
                else:  # до начального времени
                    next_time = tek_time + datetime.timedelta(0, first_time)
                tek_time = datetime.datetime.now()
                # найдем ближайшее время следующего запуска
                dt = next_time - datetime.timedelta(0, discret)
                while dt > tek_time:
                    next_time = dt
                    dt = next_time - datetime.timedelta(0, discret)
                write_log('INFO', 'thread', 'Next start at ' + next_time.ctime())
                while datetime.datetime.now() < next_time:  # будем спать до указанного времени
                    time.sleep(1)
        except Exception as err:
            write_log('ERROR', 'thread', 'Exception: ' + f"{err}" + ' ' + time.ctime())
        # завершение (по какой-то причине) работы потока
        commonthread.lock.acquire()
        commonthread.is_live = False
        commonthread.lock.release()


def connect_and_update_db():
    try:
        conn = psycopg2.connect(
            user=user_name,
            password=password,
            host=host_db,
            port=port_db,
            database=name_db)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(Scripts_DB_Oblects.txt_scripts)  # check existing db objects and createing missing objects
        write_log('INFO', 'main.py', time.ctime() + ": all tables are updated")
        return conn, True
    except Exception as err:
        write_log('ERROR', 'main.py', time.ctime() + ": error " + f"{err}")
        return None, False
