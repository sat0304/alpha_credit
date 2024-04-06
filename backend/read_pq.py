from pathlib import Path
import time

from tqdm import tqdm
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

PQ_DIR = Path("/home/sergei/MIiF_alph_cs/train_data")


def read_dfs(src_dir=PQ_DIR):
    column_names = [
        'id',
        'rn',
        'pre_since_opened',
        'pre_since_confirmed',
        'pre_pterm',
        'pre_fterm',
        'pre_till_pclose',
        'pre_till_fclose',
        'pre_loans_credit_limit',
        'pre_loans_next_pay_summ',
        'pre_loans_outstanding',
        'pre_loans_total_overdue',
        'pre_loans_max_overdue_sum',
        'pre_loans_credit_cost_rate',
        'pre_loans5',
        'pre_loans530',
        'pre_loans3060',
        'pre_loans6090',
        'pre_loans90',
        'is_zero_loans5',
        'is_zero_loans530',
        'is_zero_loans3060',
        'is_zero_loans6090',
        'is_zero_loans90',
        'pre_util',
        'pre_over2limit',
        'pre_maxover2limit',
        'is_zero_util',
        'is_zero_over2limit',
        'is_zero_maxover2limit',
        'enc_paym_0',
        'enc_paym_1',
        'enc_paym_2',
        'enc_paym_3',
        'enc_paym_4',
        'enc_paym_5',
        'enc_paym_6',
        'enc_paym_7',
        'enc_paym_8',
        'enc_paym_9',
        'enc_paym_10',
        'enc_paym_11',
        'enc_paym_12',
        'enc_paym_13',
        'enc_paym_14',
        'enc_paym_15',
        'enc_paym_16',
        'enc_paym_17',
        'enc_paym_18',
        'enc_paym_19',
        'enc_paym_20',
        'enc_paym_21',
        'enc_paym_22',
        'enc_paym_23',
        'enc_paym_24',
        'enc_loans_account_holder_type',
        'enc_loans_credit_status',
        'enc_loans_account_cur',
        'enc_loans_credit_type',
        'pclose_flag',
        'fclose_flag'
    ]
    df = [0] * 12
    for i in range(12):
        time.sleep(2)
        df[i] = pd.read_parquet(f'{src_dir}/train_data_{i}.pq',
                         use_threads=True,
                         engine='auto',
                         columns=column_names
                         )
        print(df[i], file=open('df.csv', 'a'))
    return


def main():
    read_dfs()


if __name__ == "__main__":
    main()