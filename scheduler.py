import time
import datetime

from tasks import tasks


class Scheduler:
    def __init__(self):
        self.last_executed = {}

    def run(self):
        while True:
            time.sleep(2)
            for task in tasks:
                method = task['fn']
                name = task['name']
                task_config = {'time': task['time'], 'name': task['name']}
                now = datetime.datetime.now()
                today = now.date().isoformat()
                time_tuple = tuple(int(x) for x in task_config['time'].split('-'))
                print(time_tuple, task_config['time'])
                if (now.hour, now.minute) != time_tuple or self.last_executed.get(name) == today:
                    print('Name: %s, time tuple now %s, time tuple config %s' % (
                    name, (now.hour, now.minute), time_tuple), flush=True)
                    continue
                self.last_executed[name] = today
                print('Start execution: "%s" at %s' % (name, now), flush=True)
                for i in range(1, 4):
                    try:
                        method()
                        print('Finished execution: %s at %s' % (name, (datetime.datetime.now() - now).total_seconds()), flush=True)
                        break
                    except Exception as e:
                        print('Exception in task: %s, attempt: %s : \n %s' % (name, i, e), flush=True)


if __name__ == "__main__":
    s = Scheduler()
    s.run()
