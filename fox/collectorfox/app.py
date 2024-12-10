import api
from collector import collectorfox
from dataset_def import *

if __name__ == '__main__':
    collectorfox.register_dataset(LeRobotUniversal())
    collectorfox.run()