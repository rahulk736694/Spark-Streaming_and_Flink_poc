from prettytable import PrettyTable
from datetime import datetime, timedelta
# Define transaction types and time windows (in days)
TRANSACTION_TYPES = [
    'atm_withd', 'atm_dep', 'wire_withd', 'wire_dep',
    'ach_withd', 'ach_dep', 'check_withd', 'check_dep'
]

TIME_WINDOWS = [7, 15, 90, 180, 365]

# In-memory transaction log per user
user_transaction_log = {}

def process_transaction(transaction: dict):
    user_id = transaction['user_id']
    txn_type = transaction['transaction_type']
    txn_amount = transaction['transaction_amount']
    txn_time_str = transaction['timestamp']

    # Parse timestamp
    txn_time = datetime.strptime(txn_time_str, '%Y-%m-%d %H:%M:%S')

    # Initialize log structure
    if user_id not in user_transaction_log:
        user_transaction_log[user_id] = {t: [] for t in TRANSACTION_TYPES}

    # Append this transaction to the log
    user_transaction_log[user_id][txn_type].append((txn_amount, txn_time))

    # Print aggregated results
    print_user_info_table()


def print_user_info_table():
    table = PrettyTable()

    # Header: user_id + txn_type_window for each type and time window
    headers = ['user_id']
    for t in TRANSACTION_TYPES:
        for w in TIME_WINDOWS:
            headers.append(f"{t}_{w}d")
    table.field_names = headers

    now = datetime.now()

    for user_id, txn_data in sorted(user_transaction_log.items()):
        row = [user_id]
        for t in TRANSACTION_TYPES:
            txn_list = txn_data[t]
            for w in TIME_WINDOWS:
                # Compute sum of amounts in the time window
                cutoff = now.replace(microsecond=0) - timedelta(days=w)
                total = sum(amount for amount, t_time in txn_list if t_time >= cutoff)
                row.append(round(total, 2))
        table.add_row(row)

    print("\n[FLINK TIME-BUCKETED TABLE STATE]")
    print(table)
    print()
