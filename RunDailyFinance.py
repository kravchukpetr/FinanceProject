import FinanceLib as fl
import sys
import os

# print('Parameters:')
# print(sys.argv[0] + ' ' + sys.argv[1] + ' ' + sys.argv[2] + ' ' + sys.argv[3] + ' ' + sys.argv[4])

fname = os.path.dirname(os.path.realpath(__file__)).replace("\\","/") + '/' + 'DB.config'
log_dir = os.path.dirname(os.path.realpath(__file__)).replace("\\","/")
fl.daily_update_quote(log_dir)
