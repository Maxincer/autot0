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
from secloan_mng import SecLoanMng


class Scheduler:
    # def __init__(self):
    #     self.gl = Globals()

    @staticmethod
    def secloan_mng():
        secloanmng = SecLoanMng()
        secloanmng.run()

    def schedule(self):
        scheduler = BlockingScheduler()
        scheduler.add_job(self.secloan_mng, 'date', run_date=datetime(2020, 11, 6, 8, 40, 0))
        scheduler.start()

    def run(self):
        self.schedule()


if __name__ == '__main__':
    task = Scheduler()
    task.run()
    print('Done')




