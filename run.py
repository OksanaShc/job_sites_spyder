import redis
import json
import datetime

from tasks import tasks


class Scheduler:
    def __init__(self):
        self.last_executed = {}
        self.r = redis.Redis(host='127.0.0.1')

    def get_task_config(self, task):
        method = task['fn']
        task_config = json.loads(self.r.hget('scheduler_cv', method)) if self.r.hget('scheduler_cv', method) else []
        if task_config:
            task_config = {'time': task['time'], 'name': task['name']}
            for key in task_config.keys():
                self.r.hset('scheduler', key, task_config[key])
        task_config = {'time': task['time'], 'name': task['name']}
        return task_config

    def run(self):
        while True:
            for task in tasks:
                method = task['fn']
                name = task['name']
                task_config = self.get_task_config(task)
                now = datetime.datetime.now()
                if now.strftime('%H-%M') != task_config['time'] or \
                                    self.last_executed.get(name) == now.strftime('%Y-%m-%d'):
                    continue
                self.last_executed[task_config['name']] = now.strftime('%Y-%m-%d')
                print('Start execution: %s at %s' % (name, now))
                for i in range(1, 4):
                    try:
                        method()
                        print('Finished execution: %s at %s' % (name, (datetime.datetime.now() - now).total_seconds()))
                        break
                    except Exception as e:
                        print('Exception in task: %s, attempt: %s : \n %s' % (name, i, e))

if __name__ == "__main__":
    s = Scheduler()
    s.run()