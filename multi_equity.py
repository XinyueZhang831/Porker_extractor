import pandas as pd
import re
import os
import holdem_calc
import concurrent.futures
import glob
import parallel_holdem_calc
from multiprocessing import Process


def give_all_file_name():
    all_file = glob.glob('*.csv')
    for i in all_file:
        read_in_file(i)


def read_in_file(filename):
    print(filename)
    total_df = pd.read_csv('/Users/xinyue/Documents/data/output/'+filename)
    game_list = []
    total_result = pd.DataFrame()
    for index, row in total_df.iterrows():
        stage_number = row['Stage']
        if (stage_number in game_list) == False:
            game_list.append(stage_number)
    for i in game_list:
        sub_df = give_seperate(i, total_df)
        total_result = total_result.append(sub_df)
    total_result = reorder_df(total_result)
    write_to_file(total_result, filename[:-4])


def reorder_df(df):
    cols = ['Stage', 'Game', 'Date', 'Table', 'User_ID', 'Starting Chips', \
            'Action', 'Current_Card', 'idk','Equity', 'Current_Round', 'amount', 'Show', 'Won', 'Lost']
    df = df[cols]
    return df


def give_seperate(game_number, total_df):
    sub_df = total_df.loc[total_df['Stage'] == game_number]
    if check_if_need(sub_df):
        return give_to_prossess(sub_df)
    else:
        sub_df['Equity'] = ''
        return sub_df

def give_to_prossess(sub_df):
    output_map = {}
    sub_df = modify_card(sub_df)
    list_show = []
    player_counter = 1
    for index, row in sub_df.iterrows():
        if str(row['idk']) != 'nan':
            output_map[row['User_ID']] = player_counter
            player_counter = player_counter+1
            print(row['Current_Card'])
            for i in row['Current_Card']:
                list_show.append(i)
    print(list_show)
    print(output_map)
    Pokert_card = Pokert_card_map(output_map,list_show)
    for index, row in sub_df.iterrows():
        if (row['User_ID'] in Pokert_card) and (str(row['Current_Card']) == 'Empty') and (str(row['Current_Round']) == 'POCKET CARDS'):
             sub_df.at[index, 'Equity'] = Pokert_card[row['User_ID']]
    for index, row in sub_df.iterrows():
        if (row['User_ID'] in output_map) and (str(row['Current_Card']) != 'Empty') and (str(row['Action']) != 'Show' and str(row['Action']) != 'Does not show'):
            player_order = output_map[row['User_ID']]
            print(player_order)
            print(str(row['Current_Card']) + str(list_show))
            print(index)
            sub_df.at[index, 'Equity'] = holdem_calc.calculate(row['Current_Card'], True, 1, None, list_show, False)[player_order]
    print(sub_df)
    return sub_df


def Pokert_card_map(output_map, list_show):
    new_map = output_map.copy()
    none_output = holdem_calc.calculate(None, True, 1, None, list_show, False)
    for user in new_map:
        new_map[user] = none_output[new_map[user]]
    print(output_map)
    return new_map




def modify_card(sub_df):
    for index, row in sub_df.iterrows():
        if '[' in str(row['Current_Card']):
            list_content = row['Current_Card'].split()
            list_append =[]
            for i in list_content:
                if len(i)>1:
                    if ',' in i:
                        list_append.append(i[:-1])
                    else:
                        list_append.append(i)
            sub_df.at[index, 'Current_Card'] = list_append
    card_list=[]
    current_card = []
    for index, row in sub_df.iterrows():
        if (str(row['Action']) != 'Show' and str(row['Action']) != 'Does not show') \
                and (str(row['Current_Card']) != 'Empty') and (str(row['Current_Card']) != 'nan'):
            if row['Current_Card'] in card_list:
                sub_df.at[index, 'Current_Card'] = current_card[-1]
            else:
                card_list.append(row['Current_Card'])
                if len(current_card)<1:
                    current_card.append(row['Current_Card'])
                else:
                    current_card.append(current_card[-1]+row['Current_Card'])
                sub_df.at[index, 'Current_Card'] = current_card[-1]
    return sub_df


def check_if_need(df):
    number2 = len(df['idk'].index)
    number = df['idk'].isnull().sum()
    decision = number2 - number
    if decision >1:
        return True
    else:
        return False


def write_to_file(df,filename):
    df.to_csv(filename+'_new'+'.csv')


read_in_file('test_output.csv')
