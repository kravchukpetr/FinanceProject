import FinanceLib as fl
import MaintenanceLib as mntl
import warnings
warnings.filterwarnings('ignore')

fl.daily_update_quote()
fl.get_all_stock_recomendation()
mntl.rm_old_logs()
