#!/usr/bin/env python

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
import argparse
import time
from datetime import datetime

start_time = time.time()

main_table = 'transactions'
database_file = 'data/database.sqlite'

# highest priority, match the model
suspicious_transactions_file = 'data/suspicious_transactions.csv'
# lowest priority, validation error
suspicious_entities_file = 'data/suspicious_entity.csv'
transactions_file = 'data/transactions.csv'
sample_file = 'data/singleline.sample'

# entries per insert
chunksize = 20000
# percentage, 10 means from 90to100%
bridge_coefficient = 10
# in days, default is 1
day_coefficient = 1

engine = create_engine('sqlite:///' + database_file, echo=False)
Session = sessionmaker(bind=engine)
session = Session()


size_of_db = os.path.getsize(transactions_file)
appr_line_size = os.path.getsize(sample_file)

parser = argparse.ArgumentParser(description='Suspicious transactions scanner')
parser.add_argument('action', type=str, help='Possible options: export, import')
args = parser.parse_args()

if args.action == 'import':
    i = 0
    for df in pd.read_csv('data/transactions.csv', chunksize=chunksize, iterator=True, encoding='utf-8', sep='|'):
        df = df.rename(columns={c: c.lower() for c in df.columns})
        for index, row in df.iterrows():
            if row.timestamp == '2006-02-29' or row.sender == 'ID00000000000000' or row.sender == 'ID00000000000000':
                df_error = pd.DataFrame(data=[[row.transaction, row.timestamp, row.amount, row.sender, row.receiver]])
                df_error.to_csv(suspicious_entities_file, mode='a', header=False, sep='|')
                df = df.drop(index)
                continue

        df['timestamp'] = pd.to_datetime(df['timestamp'], format="%Y-%m-%d", errors='coerce')
        # normalize balances to integers
        df['amount'] = df['amount'].apply(lambda x: int(x * 100))
        df.to_sql('transactions', engine, if_exists='replace')

        # output
        i = i + 1
        progress = appr_line_size * chunksize * i
        percentage = progress / (size_of_db/100)
        print(percentage)
    print("Completed %d iterations " % i)
    print('Suspicious entities saved to ', suspicious_entities_file)


if args.action == 'export':
    query = "select first.`transaction`, second.`transaction` as secondTransaction, first.timestamp, second.`timestamp` as secondTimestamp, first.sender as A, first.receiver as C, second.receiver as B, first.amount as amountBefore, second.amount as amountAfter " \
            "from transactions first inner join transactions second on first.receiver = second.sender " \
            "where second.sender != second.receiver " \
            "group by first.timestamp " \
            "order by A, C, B, first.timestamp"

    df = pd.read_sql_query(query, engine)
    length = len(df)
    coefficient = float((100 - bridge_coefficient) / 100)
    time_pattern = "%Y-%m-%d 00:00:00.000000"
    # in seconds
    timestamp_offset = 86400 * 1

    result = pd.DataFrame(columns=['transaction', 'timestamp', 'amount', 'sender', 'receiver'])
    counter = 0

    for index, row in df.iterrows():
        amountBefore = 0
        amountAfter = 0
        time_from = datetime.strptime(row['timestamp'], time_pattern)
        needle = False

        j = 0
        for j in range(day_coefficient):
            if index + j + 1 > len(df):
                break

            runner = df[index+j:index+j+1]
            if row['A'] != runner.get('A').values[0]:
                continue

            if row['B'] != runner.get('B').values[0]:
                continue

            if row['C'] != runner.get('C').values[0]:
                continue


            time_to = datetime.strptime(runner.get('timestamp').values[0], time_pattern)
            day_difference = time_to - time_from

            if day_difference.total_seconds()+1 > timestamp_offset:
                continue

            amountBefore += runner.get('amountBefore').values[0]
            amountAfter += runner.get('amountAfter').values[0]

        if amountAfter >= amountBefore:
            continue

        if amountAfter * (bridge_coefficient/100 + 1) > amountBefore:
            result = result.append({
                'transaction': row['transaction'],
                'timestamp': row['timestamp'],
                'amount': row['amountBefore'],
                'sender': row['A'],
                'receiver': row['C']
            }, ignore_index=True)

            result = result.append({
                'transaction': row['secondTransaction'],
                'timestamp': row['secondTimestamp'],
                'amount': row['amountAfter'],
                'sender': row['C'],
                'receiver': row['B']
            }, ignore_index=True)

    result.to_csv(suspicious_transactions_file)

# debug
if args.action == 'clean':
    try:
        os.remove(database_file)
        os.remove(suspicious_entities_file)
        os.remove(suspicious_transactions_file)
    except:
        pass


end_time = time.time()
print('Total time ', end_time - start_time)