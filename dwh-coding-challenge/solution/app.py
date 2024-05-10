import json
import pandas as pd
import os


def json_to_dataframe(json_directory):
    data_table = []

    for file_name in os.listdir(json_directory):
        if file_name.endswith('.json'):
            file_path = os.path.join(json_directory, file_name)
            
            with open(file_path, 'r') as file:
                value = json.load(file)
            
 
            if 'data' in value:
                row_data = {
                    'id': value['id'],
                    'operation': value['op'],
                    'timestamp': value['ts'],
                    **value['data']
                }
            elif 'set' in value:
                row_data = {
                    'id': value['id'],
                    'operation': value['op'],
                    'timestamp': value['ts'],
                }
                row_data.update(value['set']) 
            
            data_table.append(row_data)


    data_frame = pd.DataFrame(data_table)
    
    return data_frame

if __name__ == "__main__":

    df_accounts = json_to_dataframe('/Users/pinjammodal3/Downloads/dwh-coding-challenge/data/accounts')
    df_cards = json_to_dataframe('/Users/pinjammodal3/Downloads/dwh-coding-challenge/data/cards')
    df_savingacc = json_to_dataframe('/Users/pinjammodal3/Downloads/dwh-coding-challenge/data/savings_accounts')
    # 1.Visualize the complete historical table view of each tables in tabular format in stdout 
    print('Accounts:')
    print(df_accounts)
    print('Cards:')
    print(df_cards)
    print('Saving Accounts:')
    print(df_savingacc)
    import sqlite3
    conn = sqlite3.connect(':memory:')  # Creates an in-memory SQLite database
    df_accounts.to_sql('accounts', conn, index=False)
    df_cards.to_sql('cards', conn, index=False)
    df_savingacc.to_sql('saving_accounts', conn, index=False)
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
    # 3.From result from point no 2, discuss how many transactions has been made, when did each of them occur, and how much the value of each transaction?  
    selected_rows_df2 = result_df2.loc[result_df2['balance'] > 0, ['at', 'ct', 'operation:1', 'balance']]
    print("\n total balance change:")
    print(selected_rows_df2)
