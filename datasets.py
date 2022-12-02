import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm


# For progress_apply
tqdm.pandas()

last_date = datetime(2016, 10, 22)

class Datasets:

    def __init__(self):
        self.transactions = pd.read_pickle('data/transactions.pkl')
        self.customers    = pd.read_pickle('data/customers.pkl')
        self.query_users  = pd.read_pickle('data/query_users.pkl')
        self.customersX   = pd.read_pickle('data/customersX.pkl')

    def processTransactions(self):
        ''' Process the transactions data to clean it up and make it more usable '''

        self.transactions = pd.read_csv('data/bank_transactions.csv')
        self.transactions.columns = ['tid', 'cid', 'cdob', 'cgender', 'clocation', 'cbalance', 'tdate', 'ttime', 'tamount']
        self.transactions.dropna(inplace=True)

        print('Processing customer dob')
        self.transactions = self.transactions[self.transactions['cdob'] != '1/1/1800']
        self.transactions['cdob'] = self.transactions['cdob'].progress_apply(lambda x: datetime.strptime(x, '%d/%m/%y'))
        self.transactions['cdob'] = self.transactions['cdob'].progress_apply(lambda x: x.replace(year=x.year - 100) if x.year > 2000 else x)
        print('Processing transaction times')
        self.transactions['ttime'] = self.transactions['ttime'].progress_apply(lambda x: str(x).zfill(6))
        self.transactions['tdate'] = self.transactions.progress_apply(lambda x: datetime.strptime(x['tdate'] + x['ttime'], '%d/%m/%y%H%M%S'), axis=1)
        self.transactions.drop(columns=['ttime'], inplace=True)

        print('Processing customer IDs')
        cid = {}; j = 0
        for i, row in tqdm(self.transactions.iterrows(), total=self.transactions.shape[0]):
            if (row['cdob'], row['clocation'], row['cbalance']) not in cid:
                cid[row['cdob'], row['clocation'], row['cbalance']] = j
                j += 1

        self.transactions['cid'] = self.transactions.progress_apply(lambda x: cid[x['cdob'], x['clocation'], x['cbalance']], axis=1)
        self.transactions['cid'] = self.transactions['cid'].astype(int)

        pd.to_pickle(self.transactions, 'data/transactions.pkl')

    def getTransactions(self):
        return self.transactions

    def processCustomers(self):
        ''' Process the transactions data to get the customer data '''

        print('Grouping transactions by customer')
        self.customers = self.transactions.groupby('cid') \
            .agg({'cdob': 'first', 'cgender': 'first', 'clocation': 'first', 'cbalance': 'first'})
        self.customers.columns = ['dob', 'gender', 'location', 'balance']
        pd.to_pickle(self.customers, 'data/customers.pkl')

    def getCustomers(self):
        return self.customers

    def processQuery(self):
        self.query_users = pd.read_csv('data/query_users.csv')
        self.query_users.columns = ['dob', 'gender', 'location', 'balance', 'tdate', 'ttime', 'tamount']
        # drop if '1/1/1800' in dob
        self.query_users = self.query_users[self.query_users.dob != '1/1/1800']
        self.query_users['dob'] = self.query_users['dob'].apply(lambda x: datetime.strptime(x, '%d/%m/%y'))
        self.query_users.drop(columns=['gender', 'location','tdate', 'ttime'], inplace=True)
        self.query_users['age'] = self.query_users['dob'].apply(lambda x: (last_date - x).days // 365).astype(int)
        self.query_users['age'] = self.query_users['age'].apply(lambda x: x + 100 if x < 15 else x)

        pd.to_pickle(self.query_users, 'data/query_users.pkl')

    def getQueryUsers(self):
        return self.query_users

    def processCustomersX(self):
        self.customersX = self.customers.copy()
        self.customersX['age'] = self.customersX['dob'].apply(lambda x: (last_date - x).days // 365).astype(int)
        self.customersX['age'] = self.customersX['age'].apply(lambda x: x + 100 if x < 15 else x)
        
        # Number of transactions
        self.customersX['num_t'] = self.transactions.groupby('cid').size()
        self.customersX['num_t_g100'] = self.transactions[self.transactions['tamount'] > 100].groupby('cid').size()

        # Number of transactions greater than 100
        self.customersX['num_t_g100'] = self.customersX['num_t_g100'].fillna(0).astype(int)
        self.customersX['avg_tamount'] = self.transactions.groupby('cid')['tamount'].mean()

        # Average transaction amount
        self.customersX['utilisation'] = self.customersX['balance'] - self.customersX['avg_tamount']

        # Utilisation: Balance - average transaction amount
        self.customersX['utilisation'] = self.customersX['utilisation'].apply(lambda x: x if x > 0 else 0)

        # Var 2
        self.customersX['tamount_balance_ratio'] = self.customersX['avg_tamount'] / self.customersX['balance']

        # Var 3
        self.customersX['log_balance'] = np.log(self.customersX['balance'] + 1)

        # Var 4
        self.customersX['log_tamount'] = np.log(self.customersX['avg_tamount'] + 1)

        # Var 5
        self.customersX['log_utilisation'] = np.log(self.customersX['utilisation'] + 1)

        # Var 6
        self.customersX['log_num_t'] = np.log(self.customersX['num_t'] + 1)

        # Var 7
        self.customersX['log_num_t_g100'] = np.log(self.customersX['num_t_g100'] + 1)

        # Var 8
        self.customersX['log_tamount_balance_ratio'] = np.log(self.customersX['tamount_balance_ratio'] + 1)

        # Var 9
        self.customersX['t_max'] = self.transactions.groupby('cid')['tamount'].max()

        # Var 10
        self.customersX['t_min'] = self.transactions.groupby('cid')['tamount'].min()

        # Var 11
        self.customersX['t_std'] = self.transactions.groupby('cid')['tamount'].std()

        # Var 12
        self.customersX['t_skew'] = self.transactions.groupby('cid')['tamount'].skew()

        # Var 13
        self.customersX['t_kurt'] = self.transactions.groupby('cid')['tamount'].apply(pd.DataFrame.kurt)

        # Var 14
        self.customersX['first_transaction'] = self.transactions.groupby('cid')['tdate'].min()
        self.customersX['last_transaction'] = self.transactions.groupby('cid')['tdate'].max()
        self.customersX['seconds_between_transactions'] = (self.customersX['last_transaction'] - self.customersX['first_transaction']).dt.seconds
        self.customersX['transaction_freq'] = 24 * 60 * self.customersX['num_t'] / self.customersX['seconds_between_transactions']

        self.customersX.drop(columns=['first_transaction', 'last_transaction', 'seconds_between_transactions'], inplace=True)

        # Var 15
        self.customersX['vol_weighted_transaction_freq'] = self.customersX['transaction_freq'] * self.customersX['avg_tamount']

        # Var 16
        self.customersX['tamount_age_ratio'] = self.customersX['avg_tamount'] / self.customersX['age']

        # Var 17
        self.customersX['boomer'] = self.customersX['age'] > 60

        # Var 18
        self.customersX['zoomer'] = self.customersX['age'] < 20

        # Var 19
        self.customersX['days_till_bd'] = (last_date - self.customersX['dob'].apply(lambda x: x.replace(year=2016))).dt.days.apply(lambda x: abs(x))

        # Var 20
        self.customersX['zodiac'] = self.customersX['dob'].apply(lambda x: self.zodiac(x))

        pd.to_pickle(self.customersX, 'customersX.pkl')

    def zodiac_sign(self, dob):
        if (dob.month == 3 and dob.day >= 21) or (dob.month == 4 and dob.day <= 20):
            return 'Aries'
        elif (dob.month == 4 and dob.day >= 21) or (dob.month == 5 and dob.day <= 20):
            return 'Taurus'
        elif (dob.month == 5 and dob.day >= 21) or (dob.month == 6 and dob.day <= 20):
            return 'Gemini'
        elif (dob.month == 6 and dob.day >= 21) or (dob.month == 7 and dob.day <= 22):
            return 'Cancer'
        elif (dob.month == 7 and dob.day >= 23) or (dob.month == 8 and dob.day <= 22):
            return 'Leo'
        elif (dob.month == 8 and dob.day >= 23) or (dob.month == 9 and dob.day <= 22):
            return 'Virgo'
        elif (dob.month == 9 and dob.day >= 23) or (dob.month == 10 and dob.day <= 22):
            return 'Libra'
        elif (dob.month == 10 and dob.day >= 23) or (dob.month == 11 and dob.day <= 21):
            return 'Scorpio'
        elif (dob.month == 11 and dob.day >= 22) or (dob.month == 12 and dob.day <= 21):
            return 'Sagittarius'
        elif (dob.month == 12 and dob.day >= 22) or (dob.month == 1 and dob.day <= 19):
            return 'Capricorn'
        elif (dob.month == 1 and dob.day >= 20) or (dob.month == 2 and dob.day <= 18):
            return 'Aquarius'
        elif (dob.month == 2 and dob.day >= 19) or (dob.month == 3 and dob.day <= 20):
            return 'Pisces'
        else:
            return 'Unknown'

    def getCustomersX(self):
        return self.customersX

if __name__ == '__main__':
    d = Datasets()
    print(d.transactions.head())
