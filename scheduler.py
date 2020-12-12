#!usr/bin/env/python38
# coding: utf-8
# Author: Maxincer
# CreateDateTime: 20201104T220000

"""
定时任务
"""

from datetime import datetime

from apscheduler.schedulers.background import BlockingScheduler

from globals import Globals
from posttrdmng import PostTrdMng
from pretrdmng import PreTrdMng


class Scheduler:
    @staticmethod
    def task_at_0840():
        str_today = datetime.today().strftime('%Y%m%d')
        task1 = Globals(str_today, download_winddata_mark=1)
        task2 = PostTrdMng(str_today, download_winddata_mark=0)
        task2.run()
        task3 = PreTrdMng(str_today, download_winddata_mark=0)
        task3.run()

    def schedule(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(self.task_at_0840, 'date', run_date=datetime(2020, 12, 11, 8, 35, 0))
        scheduler.start()

    def run(self):
        self.schedule()


if __name__ == '__main__':
    task = Scheduler()
    task.run()
    print('Done')




