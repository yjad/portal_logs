import sys
from plogs import log_2_db, resolve_log_files
from reports import login_IP_stats
import plogs as logs


if __name__ == "__main__":
    #resolve_log_files("23-06-2020")
    log_2_db(None, 'ERROR')
    #log_2_db("23-06-2020")
    #log_2_db("26-07-2020")
    #get_timestamp("2020_07_26_00_00_30_394AM", "confirmLandReservation")
    #session_duration("28502021403556")
    #high_session_customers("26-07-2020")
    #high_session_customers("23-06-2020")
    #high_session_customers("16-06-2020")
    #login_IP_stats("26-07-2020")
    # login_IP_stats()

    if len (sys.argv) != 2:
        print ("Usage: python menu.py <log file or direcotry for log files>")
    else:
        logs.summerize_portal_logs(sys.argv[1])