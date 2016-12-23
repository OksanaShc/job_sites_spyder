import time
import datetime

from tasks import tasks


class Scheduler:
    def __init__(self):
        self.last_executed = {}

    def run(self):
        while True:
            time.sleep(15)
            for task in tasks:
                method = task['fn']
                name = task['name']
                task_config = {'time': task['time'], 'name': task['name']}
                now = datetime.datetime.now()
                today = now.strftime('%Y-%m-%d')
                if now.strftime('%H-%M') != task_config['time'] or self.last_executed.get(name) == today:
                    continue
                self.last_executed[name] = today
                print('Start execution: "%s" at %s' % (name, now))
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
