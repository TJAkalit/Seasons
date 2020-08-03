import os
import hashlib
import time

class Queue:

    def __init__(self):

        self.queue = []
        print('I\'m Queue')

    def pop_task(self):

        try:
            
            return self.queue.pop(0)

        except:

            return False

    def push_task(self, task):

        self.queue.append(task)
        return True

class Task:

    def __init__(self):
        
        self.operation = '' # Выполняемая операция
        self.info = {} # Информация для выполнения операции
        self.time = int() # Время инициализации задачи
        self.source = 'Main programm' # Источник задачи
        print('I\'m Task')

class Worker:

    def __init__(self):

        self.queue = None
        print('I\'m Worker!')

    def run(self):

        map = {
            'walk': walking,
            # Файлы
            'file_exist': file_exist,
            'if_file_equal': if_file_equal,
            'file_rewrite': file_rewrite,
            # Папки
            'dir_exist': dir_exist,
            'make_dir': make_dir
        }

        while True:

            task = self.queue.pop_task()

            if not task:
                print('Нет ёбаных задач для меня, иди нахуй')
                time.sleep(2)
                continue

            # print(task.operation)
            map[task.operation](task, queue = self.queue)

def walking(task, queue = None): # Получение файлов и папок в дирректории

    target = task.info['original']
    path, dirs, files = next(os.walk(target))
    print(path, dirs, files)

    for dir in dirs: # TODO

        temp_task = Task()
        temp_task.operation = 'dir_exist'
        temp_task.info = {
            'original': task.info['original'] + '\\' + dir,
            'target': task.info['target'] + '\\' + dir
        }
        queue.push_task(temp_task)

    for file in files: # Для каждого файла создаётся задача
                        # Выяснения наличия файла с таким же именем
        temp_task = Task() # В целевой директории
        temp_task.operation = 'file_exist'
        temp_task.info = {
            'original': task.info['original'] + '\\' + file,
            'target': task.info['target'] + '\\' + file
        }
        queue.push_task(temp_task) # Помещение задачи в очередь

# Файлы

def file_exist(task, queue = None): # Выяснение, существует ли файл в целевой
            # Директории
    if os.path.isfile(task.info['target']): 
        # Если файл существует, создание задачи на сравнение файлов целевой и исходной дирректорий
        temp_task = Task()
        temp_task.operation = 'if_file_equal'
        temp_task.info = {
            'original': task.info['original'],
            'target': task.info['target']
        }
    
    else:
        # Если файла по данному пути нет, создаётся задача на запись файла
        temp_task = Task()
        temp_task.operation = 'file_rewrite'
        temp_task.info = {
            'original': task.info['original'],
            'target': task.info['target']
        }
    
    queue.push_task(temp_task) # Результрующая задача помещается в очередь

def if_file_equal(task, queue = None): # Сравнение файлов

    with open(task.info['original'], 'rb') as orig: # Каждый файл открывается по очереди
        # Чтобы достать хэш каждого фала
        temp = hashlib.md5()
        # Ведётся по "чанкам", чтобы не выгружать весь файл в оперативную память
        for chunk in iter(lambda: orig.read(4096), b''):

            temp.update(chunk)
        
        h_1 = temp.hexdigest()

    with open(task.info['target'], 'rb') as target:

        temp = hashlib.md5()
        
        for chunk in iter(lambda: target.read(4096), b''):

            temp.update(chunk)
        
        h_2 = temp.hexdigest()
    # Если хеши равны, последующие задачи не требуются
    if h_1 != h_2: # Сравнение хешей файлов

        temp_task = Task() # Создание задачи на копирование
        temp_task.operation = 'file_rewrite'
        temp_task.info = {
            'original': task.info['original'],
            'target': task.info['target']
        }
        queue.push_task(temp_task) # Отправка задания в очередь выполнения

def file_rewrite(task, queue = None): # Запись/перезапись файла

    with open(task.info['original'], 'rb') as orig:

        with open(task.info['target'], 'wb') as target:
            
            for chunk in iter(lambda: orig.read(16), b''): # Считывание через буфер, чтобы не выгружать весь файл в ОЗУ

                target.write(chunk)
                # Продолжения задач не требуется 

# Папки

def dir_exist(task, queue = None):

    if os.path.isdir(task.info['target']):
        
        _, _, files = next(os.walk(task.info['original']))

        for file in files:

            temp_task = Task()
            temp_task.operation = 'file_exist'
            temp_task.info = {
                'original': task.info['original'] + '\\' + file,
                'target': task.info['target'] + '\\' + file
            }
            queue.push_task(temp_task)
        
    
    else:
        
        temp_task = Task()
        temp_task.operation = 'make_dir'
        temp_task.info = {
            'original': task.info['original'],
            'target': task.info['target']
        }
        queue.push_task(temp_task)
        print("Хуйня какая-то")

def make_dir(task, queue = None):

    os.mkdir(task.info['target'])
    _, _, files = next(os.walk(task.info['original']))

    for file in files:

        temp_task = Task()
        temp_task.operation = 'file_rewrite'
        temp_task.info = {
            'original': task.info['original'] + '\\' + file,
            'target': task.info['target'] + '\\' + file
        }
        queue.push_task(temp_task)


task2 = Task()
task2.operation = 'walk'
task2.info = {
    'original': 'C:\\Users\\user\\Documents\\LQ_work',
    'target': 'C:\\VMs',
    'queue': None
}

worker = Worker()
worker.queue = Queue()
worker.queue.push_task(task2)
worker.run()