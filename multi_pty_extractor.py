import pandas as pd
import re
import concurrent.futures
import glob


def read_in_file(pty_filename):
    f = open(pty_filename, 'r')
    list_content = pty_filename.split('/')

    filename = list_content[-1][:-4]
    path = ('/').join(list_content[:-1])+'/'

    file = []
    for x in f:
        file.append(x)
    total_stage = extract_all_stage(file)
    separate_the_file(file, total_stage, filename, path)
    file.clear()


def extract_all_stage(file):
    total_stage = []
    for x in file:
        if 'Game #' in x:
            splitString = x.split()
            number = splitString[1][1:]
            total_stage.append(number)
    return total_stage


def separate_the_file(file, total_stage, filename,path):
    sub_array = []
    total_result = pd.DataFrame()
    i = 0
    while i < len(total_stage)-1:
        for x in file:
            if ('Game #' in x) and (total_stage[i] in x) == False:
                for j in sub_array:
                    if ('Connection Lost due to some reason' in j) or ('The high card is the' in j):
                        i += 1
                        print('yes,it is here')
                        sub_array.clear()
                #print(len(sub_array))
                if len(sub_array)>0:
                    i += 1
                    #print('great')
                    sub_dtf = give_subarray_clean(sub_array)
                    del_index = []
                    for index, row in sub_dtf.iterrows():
                        if (row['Action'] == 'Collects') or (str(row['Won']) == 'nan'):
                            del_index.append(index)
                    sub_dtf = sub_dtf.drop(sub_dtf.index[del_index])
                    sub_dtf = sub_dtf.reset_index(drop=True)
                    sub_dtf = check_this(sub_dtf)
                    sub_dtf['Stage'] = extract_stage_number(sub_array)
                    sub_dtf = reorder_columns_all(sub_dtf)
                    total_result = total_result.append(sub_dtf)
                    sub_array.clear()
                    sub_array.append(x)
            else:
                sub_array.append(x)
    write_to_csv(total_result, filename, path)


def check_this(sub_dtf):
    adding_df = find_insert_player(sub_dtf)
    j = 0
    while j < 5:
        user_id_list = sub_dtf.User_ID.tolist()
        if ('' in user_id_list) ==False:
            break
        else:
            for i, r in sub_dtf.iterrows():
                if r['User_ID'] == '':
                    new_df = adding_df.replace({'Current_Card':''}, r['Current_Card'])
                    new_df = new_df.replace({'Current_Round': ''}, r['Current_Round'])
                    sub_dtf = sub_dtf.drop(i, axis=0).reset_index(drop=True)
                    new_df = check_how_many_row(sub_dtf,new_df,r['Current_Round'])
                    sub_dtf = pd.concat([sub_dtf.iloc[:i], new_df, sub_dtf.iloc[i:]]).reset_index(drop=True)
                    break

        j = j+1
    #sub_dtf.drop_duplicates(subset=['User_ID', 'Action', 'Current_Card', 'Current_Card'], \
    #                        keep='last', inplace=True)
    return sub_dtf


def check_how_many_row(sub_dtf,new_df, current_round):
    ckecking_sub = sub_dtf.loc[(sub_dtf['Current_Round'] == current_round) & (sub_dtf['Show'] == '')]
    checking_list = ckecking_sub.User_ID.tolist()
    new_df_list = new_df.User_ID.tolist()
    same_user = set(checking_list) & set(new_df_list)
    if len(same_user) >0:
        index_list = []
        for i, r in new_df.iterrows():
            if r['User_ID'] in same_user:
                index_list.append(i)
        new_df1 = new_df.drop(index_list)
        new_df = new_df1.reset_index(drop=True)
    return new_df


def find_insert_player(sub_dtf):
    find_show = sub_dtf[sub_dtf['Show'] !='']
    col = sub_dtf.columns
    find_show = find_show.drop(col[5:], axis=1)
    find_show['Action'] = ''
    find_show['Current_Card'] = ''
    find_show['idk'] = ''
    find_show['Current_Round'] = ''
    find_show['amount'] = ''
    find_show['Show'] = ''
    find_show['Won'] = 0
    find_show['Lost'] = 0
    return find_show


def give_subarray_clean(sub_array):
    delet_row = []
    table = extract_table(sub_array)
    game = extract_game(sub_array)
    date = extract_date(sub_array)
    chip_map = extract_chip_map(sub_array)
    round = extract_round(sub_array, chip_map)#this is a data frame
    round['Game'] = game
    round['Table'] = table
    round['Date'] = date
    reorder_df = reorder_columns(round)
    result = modify_result(reorder_df, sub_array,chip_map, round)
    #print(result.idk)
    result = modify_pty(result)
    for index, row in result.iterrows():
        if ((row.Show == 2) and (row.Lost == 0)):
            delet_row.append(index)
    result1 = result.drop(result.index[delet_row])
    result = result1.reset_index(drop=True)
    #print(result)
    return result


def modify_pty(reorder_df):
    check_Collection = reorder_df.Action.str.contains('Collects',  case=True, regex=True).sum()
    check_Show = reorder_df.Action.str.contains('Show', case=True, regex=True).sum()
    map_win = {}
    index_list = []
    if check_Collection >= 1 and check_Show >= 1:
        for index, row in reorder_df.iterrows():
            if row['Won'] != 0:
                number = row['Won']
                index_list.append(index)
                map_win[row.User_ID] = number
        for index, row in reorder_df.iterrows():
            if (row['Show'] == 1) and row['Lost'] == 0:
                reorder_df.at[index, 'Won'] = map_win.get(row.User_ID)
        result1 = reorder_df.drop(reorder_df.index[index_list])
        result = result1.reset_index(drop=True)

    else:
        result = reorder_df

    return result


def extract_round(sub_array, extract_chip_map):
    current_round = ['Begin']
    current_card = ['Empty']
    most_data_in_frame = pd.DataFrame()
    for x in sub_array:
        #print(x)
        if "Dealing down cards" in x:
            #print('yes')
            current_round.append("POCKET CARDS")
        elif "Dealing Flop" in x:
            #print('yes')
            current_round.append("FLOP")
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card.append(new_sting[len(new_sting) - 1])
        elif "Dealing Turn " in x:
            current_round.append("TURN")
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card.append(new_sting[len(new_sting) - 1])
        elif "Dealing River" in x:
            current_round.append("RIVER")
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card.append(new_sting[len(new_sting) - 1])
        elif "Summary:" in x:
            current_round.append("SHOW DOWN")
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card.append(new_sting[len(new_sting) - 1])
        new_rest = extract_rest(current_round, current_card, x, sub_array,extract_chip_map)
        if new_rest != 'empty':
            most_data_in_frame = most_data_in_frame.append(new_rest, ignore_index=True)
        #write_to_csv(most_data_in_frame, 'see_this_result.txt')
    return most_data_in_frame


def modify_result(reorder_df, sub_array,chip_map,round):
    reorder_df = reorder_columns(reorder_df)
    sub_df = reorder_df.loc[(reorder_df['amount'] !=0) | (reorder_df['Won'] !=0)| (reorder_df['Show'] != '')]
    cols = ['User_ID', 'amount', 'Show', 'Won', 'Lost']
    sub_df = sub_df[cols]
    sub_df.amount = pd.to_numeric(sub_df.amount, errors='coerce')
    sub_df.Won = pd.to_numeric(sub_df.Won, errors='coerce')
    sub_df.Lost = pd.to_numeric(sub_df.Lost, errors='coerce')

    sub_df.Show = pd.to_numeric(sub_df.Show, errors='coerce')
    sub_df1 = sub_df.groupby('User_ID')[['Show','amount', 'Won', 'Lost']].sum()
    sub_df1 = sub_df1.reset_index()
    for index, row in sub_df1.iterrows():
        if (row['amount'] != 0) and (row['Won'] == 0):
            sub_df1.at[index, 'Lost'] = 2
    new_result = generate_new_result(reorder_df, sub_df1, sub_array,chip_map,round)
    return new_result


def generate_new_result(reorder_df, sub_df1,sub_array,seat,round):
    current_round = round.Current_Round[round.tail(1).index].values
    sub_df1['Game'] = reorder_df['Game']
    sub_df1['Date'] = reorder_df['Date']
    sub_df1['Table'] = reorder_df['Table']
    sub_df1['amount'] = ''
    sub_df1['Current_Round'] = current_round[0]
    sub_df1['Starting Chips'] = ''
    sub_df1['Current_Card'] = ''
    sub_df1['Action'] = ''
    drop_list = []
    for i, r in sub_df1.iterrows():
        if r['User_ID'] == '':
            drop_list.append(i)
    sub_df1 = sub_df1.drop(reorder_df.index[drop_list])
    sub_df1 = sub_df1.reset_index(drop=True)

    idk_map = {}
    current_card_map = {}
    for i, r in reorder_df.iterrows():
        if r['Show'] == 1.0:
            idk_map[r['User_ID']] = r['idk']
            current_card_map[r['User_ID']] = r['Current_Card']
        elif r['Show'] == 2.0:
            idk_map[r['User_ID']] = r['idk']
            current_card_map[r['User_ID']] = r['Current_Card']
    for i, r in sub_df1.iterrows():
        if r['Show'] == 1.0:
            sub_df1.at[i,'idk'] = idk_map[r['User_ID']]
            sub_df1.at[i, 'Current_Card'] = current_card_map[r['User_ID']]
            sub_df1.at[i, 'Starting Chips'] = seat.Chips[seat.User_ID == r.User_ID].values[0]
            sub_df1.at[i, 'Action'] = 'Show'
        elif r['Show'] == 2.0:
            sub_df1.at[i,'idk'] = idk_map[r['User_ID']]
            sub_df1.at[i,'Current_Card'] = current_card_map[r['User_ID']]
            sub_df1.at[i, 'Starting Chips'] = seat.Chips[seat.User_ID == r.User_ID].values[0]
            sub_df1.at[i, 'Action'] = 'Does not show'
        else:
            sub_df1.at[i,'idk'] = ''
            sub_df1.at[i,'Current_Card'] = ''
            sub_df1.at[i, 'Starting Chips'] = seat.Chips[seat.User_ID == r.User_ID].values[0]
            sub_df1.at[i, 'Action'] = ''
    new_merged_dataframe = reorder_columns(sub_df1)
    reorder_df = reorder_df.append(new_merged_dataframe, ignore_index=True)
    return reorder_df


def extract_stage_number(sub_array):
    for x in sub_array:
        #print(sub_array)
        if '#Game No' in x:
            splitString = x.split()
            number = splitString[3]
            break
            #print(number)
        else:
            number = 'empty'
    return number


def extract_table(sub_array):
    for x in sub_array:
        if 'Table Table' in x:
            splitString = x.split()
            number = splitString[2]
        if 'Table Deep Stack' in x:
            splitString = x.split()
            number = splitString[3][1:]
        if 'Table Jackpot' in x:
            splitString = x.split()
            number = splitString[2][1:]
        if 'Table Heads Up' in x:
            splitString = x.split()
            number = splitString[3][1:]
        if 'Table Speed' in x:
            splitString = x.split()
            number = splitString[2][1:]
    return number


def extract_date(sub_array):
    for x in sub_array:
        if (('The time at which hand ended:' in x) == False):

            if ((' EDT' in x) or (' CEST' in x)):
                splitString = x.split()
                day = splitString[len(splitString)-6][:-1]
    return day


def extract_game(sub_array):
    for x in sub_array:
        if (('The time at which hand ended:' in x) == False):
            if (' EDT' in x) or (' CEST' in x) and ('ended' in x) == False:
                new_sting = x.split()
                part1 = new_sting[0]
    return part1


def extract_chip_map(sub_array):
    if type(sub_array) is list:
        extract_seat_position = pd.DataFrame()
        game = extract_game(sub_array)
        game = game.replace('$','')
        game = game.replace(',','')
        game = int(game)/100
        for x in sub_array:
            splitString = x.split()
            if ( "Dealing down cards" in x ) == False:
                if ('Seat' in x) and(':' in x):
                    chips = extract_chips(x).replace('$', '')
                    chips = chips.replace(',', '')
                    chips = float(chips)
                    chips = chips/ game
                    data1 = {'User_ID': splitString[2], 'Chips': chips}
                    extract_seat_position = extract_seat_position.append(data1, ignore_index=True)
                elif ('joined' in x):
                    chips = 'NaN'
                    data1 = {'User_ID': splitString[0],'Chips': chips}
                    extract_seat_position = extract_seat_position.append(data1, ignore_index=True)
            else:
                break
    return extract_seat_position


def extract_chips(x):
    splitString = x.split()
    number = splitString[4]
    return number


def extract_rest(current_round, current_card, x, sub_array, seat):
    splitString = x.split()
    if (':' in x) == False:
        if "Dealing Flop" in x:
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card = new_sting[len(new_sting) - 1]
            data1 = {'User_ID': '', 'Action': '', 'amount': '', \
                     'Show': '', 'Won': 0, 'Lost': 0,'Current_Round': current_round[len(current_round)-1], \
                     'Current_Card': current_card, 'Starting Chips': '', 'idk': ''}
            return data1
        elif "Dealing Turn " in x:
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card = new_sting[len(new_sting) - 1]
            data1 = {'User_ID': '', 'Action': '', 'amount': '', \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1],\
                     'Current_Card': current_card, 'Starting Chips': '', 'idk': ''}
            return data1
        elif "Dealing River" in x:
            new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
            current_card=new_sting[len(new_sting) - 1]
            data1 = {'User_ID': '', 'Action': '', 'amount': '', \
                     'Show': '', 'Won': 0, 'Lost': 0,'Current_Round': current_round[len(current_round)-1], \
                     'Current_Card': current_card, 'Starting Chips': '', 'idk': ''}
            return data1
        elif "Summary:" in x:
             new_sting = re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)
             current_card=new_sting[len(new_sting) - 1]
             data1 = {'User_ID': '', 'Action': '', 'amount': '', \
                     'Show': '', 'Won': 0, 'Lost': 0, '': '', \
                     'Current_Card': current_card, 'Starting Chips': '', 'idk':''}
             return data1
        elif "sitting out" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Sitting Out', 'amount': '', \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
        elif "posts" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            amount = splitString[len(splitString)-2][2:]
            data1 = {'User_ID': splitString[0], 'Action': 'Post', 'amount': amount, \
                 'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                 'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "folds" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Fold', 'amount': 0, \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1],'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "raises" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            amount = splitString[len(splitString)-2][2:]
            data1 = {'User_ID': splitString[0], 'Action': 'Raises', 'amount': amount, \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "checks" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Checks', 'amount': 0, \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "shows" in x:
            result = extract_card(x)
            card = result[2]
            idk_what_is_this = (' ').join(result[3:])
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Show', 'amount': 0, \
                     'Show': 1, 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': card, 'Starting Chips': chips[0], 'idk': idk_what_is_this}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "bets" in x:
            amount = splitString[len(splitString) - 2][2:]
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Bets', 'amount': amount, \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                     'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #   ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif ("calls" in x) and ('[' in x ):
            amount = splitString[2][2:]
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Call', 'amount': amount, \
                     'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                     'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "1returned" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[len(splitString)-1]]].values
            data1 = {'User_ID': splitString[len(splitString)-1], 'Action': 'Return', 'amount': '-'+splitString[3][1: len(splitString[3])], \
                    'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "mucks" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Mucks', 'amount': 0, \
                    'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "wins" in x:
            if len(splitString)<6:
                chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
                data1 = {'User_ID': splitString[0], 'Action': 'Collects', 'amount': 0, \
                        'Show': '', 'Won': 1 , 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                        'Current_Card': '', 'Starting Chips': chips[0], 'idk': ''}
                return data1
            elif ('from the main pot' in x) or ('from the side pot' in x):
                chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
                data1 = {'User_ID': splitString[0], 'Action': 'Collects', 'amount': 0, \
                        'Show': '', 'Won': 1, 'Lost': 0,
                        'Current_Round': current_round[len(current_round) - 1], \
                        'Current_Card': '', 'Starting Chips': chips[0],
                        'idk': ''}
                #print(data1)
                return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "net: -" in x:
            if len(splitString)>6:
                result = extract_card(x)
                card = result[len(result) - 1]
                lost_amount1 = splitString[5].replace('$', '')
                lost_amount = lost_amount1[:len(lost_amount1)-1]
                show = 1
            else:
                card = ''
                lost_amount = splitString[5].replace('$', '')
                show = ''
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[2]]].values
            data1 = {'User_ID': splitString[2], 'Action': 'Lost', 'amount': 0, \
                    'Show': show, 'Won': 0, 'Lost': lost_amount, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': card, 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
            #ac_re_array = ac_re_array.append(data1, ignore_index=True)
        elif "all-In" in x:
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'All_In', 'amount': splitString[3][2:], \
                    'Show': '', 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': current_card[len(current_card)-1], 'Starting Chips': chips[0], 'idk': ''}
                #print(data1)
            return data1
        elif "doesn't show" in x:
            result = extract_card(x)
            card = result[3]
            idk_what_is_this = (' ').join(result[4:])
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[0]]].values
            data1 = {'User_ID': splitString[0], 'Action': 'Does not show', 'amount': 0, \
                    'Show': 2, 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                    'Current_Card': card, 'Starting Chips': chips[0], 'idk': idk_what_is_this}
            #print(data1)
            return data1
        elif ("net: +" in x) and len(splitString) > 6:
            result = extract_card(x)
            card = result[len(result)-1]
            chips = seat.Chips[seat.index[seat['User_ID'] == splitString[2]]].values
            data1 = {'User_ID': splitString[2], 'Action': 'Show', 'amount': 0, \
                 'Show': 1, 'Won': 0, 'Lost': 0, 'Current_Round': current_round[len(current_round)-1], \
                 'Current_Card': card, 'Starting Chips': chips[0], 'idk': ''}
            #print(data1)
            return data1
    else:
        return 'empty'


def extract_card(x):
    return re.findall('\[[^\]]*\]|\([^\)]*\)|\"[^\"]*\"|\S+', x)

def reorder_columns(rest):
    cols = ['Game', 'Date', 'Table','User_ID', 'Starting Chips', \
            'Action', 'Current_Card','idk', 'Current_Round','amount', 'Show', 'Won', 'Lost']
    df = rest[cols]
    #print(df)
    return df


def reorder_columns_all(rest):
    cols = ['Stage','Game', 'Date', 'Table', 'User_ID', 'Starting Chips', \
            'Action', 'Current_Card', 'idk', 'Current_Round', 'amount', 'Show', 'Won', 'Lost']
    df = rest[cols]
    # print(df)
    return df


def write_to_csv(df, filename,path):
    print(filename)
    df.to_csv('/'+path+filename + '.csv')
    #I don't think Windows need the first /, you can delete it.
    #If according to this instruction it does not work, text me.

    print(path+filename + '.csv')


with concurrent.futures.ProcessPoolExecutor() as executor:
    #these three varaibels are list of files in the directory.
    #each of them is one directory
    #I modified this to save the file in the same folder as where it from.
    #then, see the first function
    allfile_folder1 = glob.glob('/Users/xinyue/Documents/data/25/*.txt')
    allfile_folder2 = glob.glob('/Users/xinyue/Documents/data/4/*.txt')
    allfile_folder3 = glob.glob('/Users/xinyue/Documents/data/6/*.txt')
    allfile_folder4 = glob.glob('/Users/xinyue/Documents/data/10/*.txt')
    allfile = allfile_folder1 + allfile_folder2 + allfile_folder3 + allfile_folder4
    print(allfile)
    executor.map(read_in_file, allfile)