import FinanceLib as fl
import MaintenanceLib as mntl
import warnings
warnings.filterwarnings('ignore')
from dotenv import load_dotenv
load_dotenv()

fl.daily_update_quote()
fl.get_all_stock_recomendation()
mntl.rm_old_logs()
