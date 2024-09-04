# Main file for launching ETLs

from events_engine import EventsEngine
from sensors_engine import SensorsEngine
from operators_engine import OperatorsEngine


if __name__ == "__main__":

    print('--------- Running events engine ---------')
    evt = EventsEngine()
    evt.run()
    print('Ended events engine')

    print('--------- Running Operators engine ---------')
    oe = OperatorsEngine()
    oe.run()
    print('Ended operators engine')

    print('--------- Running sensors engine ---------')
    ssr = SensorsEngine()
    ssr.save_csv()
    ssr.save_hdf5()
    ssr.save_pickle()
    ssr.setSensorsInfoDB()
    print('Ended sensors engine')


