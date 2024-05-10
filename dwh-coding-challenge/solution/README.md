# 1.Visualize the complete historical table view of each tables in tabular format in stdout 
print('Accounts:')
print(df_accounts)
print('Cards:')
print(df_cards)
print('Saving Accounts:')
print(df_savingacc)

# 2. Visualize the complete historical table view of the denormalized joined table in stdout by joining these three tables
print('Historical Accounts & Cards')
query = '''with source1 as (SELECT accounts.timestamp at,cards.timestamp ct,accounts.*,cards.*
        FROM accounts
        left join cards on cards.timestamp=accounts.timestamp order by accounts.timestamp asc),
        source2 as (SELECT accounts.timestamp at,cards.timestamp ct,accounts.*,cards.*
        FROM cards
        left join accounts on cards.timestamp=accounts.timestamp order by accounts.timestamp asc),
        source as (select * from source1 union all select * from source2)
        select distinct *
        from source
'''
result_df = pd.read_sql_query(query, conn)
print(result_df)
print('Historical Accounts & Saving Accounts')
query2 = '''with source1 as (SELECT accounts.timestamp at,saving_accounts.timestamp ct,accounts.*,saving_accounts.*
        FROM accounts
        left join saving_accounts on saving_accounts.timestamp=accounts.timestamp order by accounts.timestamp asc),
        source2 as (SELECT accounts.timestamp at,saving_accounts.timestamp ct,accounts.*,saving_accounts.*
        FROM saving_accounts
        left join accounts on saving_accounts.timestamp=accounts.timestamp order by accounts.timestamp asc),
        source as (select * from source1 union all select * from source2)
        select distinct *
        from source
'''
result_df2 = pd.read_sql_query(query2, conn)
print(result_df2)

# 3.From result from point no 2, discuss how many transactions has been made, when did each of them occur, and how much the value of each transaction?  
selected_rows_df = result_df.loc[result_df['credit_used'] > 0, ['at', 'ct', 'operation:1', 'credit_used']]
print("total credit used:")
print(selected_rows_df)

Answer: there are 3 transactions in cards
total credit used:
    at            ct operation:1  credit_used
11 NaN  1.579361e+12           u      37000.0
12 NaN  1.578314e+12           u      12000.0
13 NaN  1.578420e+12           u      19000.0

selected_rows_df2 = result_df2.loc[result_df2['balance'] > 0, ['at', 'ct', 'operation:1', 'balance']]
print("\n total balance change:")
print(selected_rows_df2)

Answer: there are 4 transactions in saving accounts
total balance change:
    at            ct operation:1  balance
9  NaN  1.578649e+12           u  40000.0
10 NaN  1.578654e+12           u  21000.0
11 NaN  1.579505e+12           u  33000.0
12 NaN  1.577956e+12           u  15000.0