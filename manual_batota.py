from optibook.synchronous_client import Exchange
from importlib import reload

from utils_3 import *

import logging
import threading
import time

exitFlag = 0
logger = logging.getLogger('client')
logger.setLevel('ERROR')

print("Setup was successful.")


e = Exchange()
a = e.connect()

print("Connection OK.")

data=data_loader()
companies = list(data.columns)[1:]

#df_pvals, df_ratios = tabulate_cointeg_pvals_ratios(data, only_significant=0)
#print(df_pvals)
#print(df_ratios)
#data=data_loader()

cointeg_comp_pairs = cointeg_significant_pairs(data, alpha=0.01)
print(cointeg_comp_pairs)

volume_storage = []
current_positions = []
new_positions = []
pair_list = []
levels_deep_list = []

limits = [0, 0.001, 0.0016, 0.0028, 0.0047]
max_levels=len(limits)-1
base_volume=500/max_levels -1


stock_prices_max = {'AIRBUS': 22,
 'SIEMENS': 31,
 'TOTAL': 218,
 'UNILEVER': 358,
 'SAP': 100,
 'ASML': 30,
 'LVMH': 218,
 'ALLIANZ': 523}
stock_prices_min = {'AIRBUS': 21,
 'SIEMENS': 30,
 'TOTAL': 99,
 'UNILEVER': 163,
 'SAP': 40,
 'ASML': 15,
 'LVMH': 81,
 'ALLIANZ': 161}



companies = list(data.columns[1:])



#BATOTA
cheat_code(companies, e)


hack_out_of_positions(e) # Resets all our our actual holdings to 0
current_positions = get_nr_positions_custom(e) # Resets our tracker of current positions
volume_storage = creates_volume_storage(cointeg_comp_pairs, max_levels) # Resets the storage for traded volumes at various levels

#
pair_list=creates_pairs_storage(cointeg_comp_pairs)
levels_deep_list=creates_levels_deep_storage(cointeg_comp_pairs)
gamma_list=create_gamma_list(cointeg_comp_pairs)
c_list=create_c_list(cointeg_comp_pairs)

res = 0

print("Lists created")

res = 0


        
class myThread (threading.Thread):
    def __init__(self, threadID, name, aux):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.aux=aux
        #self.counter = counter
    def run(self):
        print ("Starting " + self.name)
        exec_trade(self.name, self.aux)
        print ("Exiting " + self.name)

def exec_trade(threadName, aux):
    counter=1
    aux=aux
    #while counter:
    if exitFlag:
        threadName.exit()
    #time.sleep(0.5)
    automated_trading(current_positions,levels_deep_list, volume_storage, pair_list, max_levels, limits, base_volume, aux, e, c_list, gamma_list, stock_prices_max, stock_prices_min)
        #counter -= 1
        
print("myThread Class created")



threads = []
thread1 = myThread(1, "Thread-1", 0)
threads.append(thread1)
thread2 = myThread(2, "Thread-2", 1)
threads.append(thread2)
thread3 = myThread(3, "Thread-3", 2)
threads.append(thread2)

# Start new Threads
thread1.start()
thread2.start()
thread3.start()


for t in threads:
    t.join()

print ("Exiting Main Thread")
