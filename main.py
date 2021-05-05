import time
import commonthread
import os


def start_thread():
    at = commonthread.TImport(commonthread.connection)
    at.start()
    commonthread.is_live = True


# прочитаем параметры из окружения, если нет кого-то, то используются по умолчанию
commonthread.host_db = commonthread.isNull(os.environ.get("Jira_host_db"), commonthread.host_db)
commonthread.port_db = int(commonthread.isNull(os.environ.get("Jira_port_db"), commonthread.port_db))
commonthread.name_db = commonthread.isNull(os.environ.get("Jira_name_db"), commonthread.name_db)
commonthread.user_name = commonthread.isNull(os.environ.get("Jira_user_name"), commonthread.user_name)
commonthread.password = commonthread.isNull(os.environ.get("Jira_pasword"), commonthread.password)
commonthread.discret = int(commonthread.isNull(os.environ.get("Jira_discret"), commonthread.discret))
commonthread.first_time = int(commonthread.isNull(os.environ.get("Jira_first_time"), commonthread.first_time))
#  создать коннекцию к БД и апдейтить ее
# commonthread.connection, result = commonthread.connect_and_update_db()

# if not result:  # оршибки БД
#     # send(messages=[time.ctime()+": error "+f"{err}"])
#     exit(-5)
# else:  # создаем поток и запускаем его на работу
start_thread()

# уходим в бесконечный цикл
while True:
    time.sleep(60)
    # проверка и если надо перезапуск потока
    commonthread.lock.acquire()
    up = commonthread.is_live
    commonthread.lock.release()
    if not up:  # поток завершился - снова запустить
        commonthread.write_log('WARN', 'main', 'Cancel thread')
        start_thread()
