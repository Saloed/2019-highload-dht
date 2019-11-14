import argparse

import durations
import enum


class LoadTestingMode(enum.Enum):
    create_unique = 'create-unique'
    create_overwrite = 'create-overwrite'
    get = 'get'
    get_new = 'get-new'
    mixed = 'mixed'

    def __str__(self):
        return self.value

    @staticmethod
    def help_message():
        return """Type of generated ammo:
{create_unique}     Put with unique entities    
{create_overwrite}  Put with duplicated entities (10%% probability)
{get}               Get entities uniformly. Also produce ammo to fulfill storage   
{get_new}           Get mostly newest entities. Also produce ammo to fulfill storage
{mixed}             Mixed get and create entities. Also produce ammo to fulfill storage   
        """.format(
            create_unique=LoadTestingMode.create_unique.value,
            create_overwrite=LoadTestingMode.create_overwrite.value,
            get=LoadTestingMode.get.value,
            get_new=LoadTestingMode.get_new.value,
            mixed=LoadTestingMode.mixed.value
        )


parser = argparse.ArgumentParser(
    description='Generate ammo with specified parameters',
    formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('--rps', type=int, required=True,
                    help='Expected maximal RPS')
parser.add_argument('--duration', type=durations.Duration, required=True,
                    help='Expected maximal duration')
parser.add_argument('--mode', type=LoadTestingMode,
                    choices=list(LoadTestingMode), required=True,
                    help=LoadTestingMode.help_message())
args = parser.parse_args()
