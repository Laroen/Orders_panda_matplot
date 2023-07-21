import pandas
import numpy


def Multiply(x,y):
    return x*y


def Store_Change(store,df_order):
    store.set_index('piece_id',inplace=True)
    store['need'] = pandas.Series()
    #df_order['cost'] = pandas.Series()
    df_order['makeable'] = pandas.Series()
    df_order['cost'] = df_order.apply(lambda x: Multiply(x['price'],x['pieces']),axis=1)
    for i in df_order.iterrows():
        #df_order['cost'].loc[i[0]] = i[1]['price'] * i[1]['pieces']
        if store.loc[i[1]['piece_id']]['ammount'] >= i[1]['pieces']:
            store['ammount'].loc[i[1]['piece_id']] -= i[1]['pieces']
            df_order['makeable'].loc[i[0]] = 'Succes'
        else:
            #store['need'] = store['ammount'].apply(lambda x: x - i[1]['pieces'])
            store['need'].loc[i[1]['piece_id']] = store['ammount'].loc[i[1]['piece_id']] - i[1]['pieces']
            df_order['makeable'].loc[i[0]] = 'False'
    return store,df_order


def Check_order(df_order):
    letter = pandas.DataFrame()
    letter['email'] = pandas.Series()
    letter['text'] = pandas.Series()
    df_order['order_id'] = df_order['order_id'].astype(numpy.int64)
    for i in range(1,len(df_order['order_id'])):
        if i in df_order['order_id'].values:
            each_order = df_order[df_order['order_id'] == i]
            order_possible = each_order.groupby('makeable', as_index=False)['cost'].sum()
            order_possible['piece_id'] = pandas.Series()
            must_0 = each_order.groupby('makeable')['piece_id']
            index = 0
            for i in must_0:
                order_possible['piece_id'][index] = list(i[1].values)
                index += 1
            order_possible['email'] = each_order.iloc[0]['email']
            # print(order_possible)
            order_possible = pandas.DataFrame(order_possible)
            index = 0
            for i in order_possible.iterrows():
                # print(i[1]['makeable'], i[1]['email'])
                # print(len(order_possible['makeable']))
                if len(order_possible['makeable']) == 1:
                    if i[1]['makeable'] == 'False':
                        new_row = {'email': i[1]['email'],
                                   'text': f' A következő rendelései: {";".join(i[1]["piece_id"])} függő állapotba került(ek).' \
                                           f' Hamarosan értesítjük a szállítás időpontjáról.'
                                   }
                        # letter._append(new_row, ignore_index=True)
                        letter.loc[len(letter)] = new_row
                    elif i[1]['makeable'] == 'Succes':
                        new_row = {'email': i[1]['email'],
                                   'text': f' A következő rendeléseit: {";".join(i[1]["piece_id"])} két napon belül szállítjuk.' \
                                           f' A rendelés értéke: {i[1]["cost"]}] Ft'
                                   }
                        letter.loc[len(letter)] = new_row
                elif len(order_possible['makeable']) == 2:
                    if index == 0:
                        add_email = i[1]['email']
                        add_text = [f'A következő rendelései: {";".join(i[1]["piece_id"])} függő állapotba került(ek).' \
                                    f' Hamarosan értesítjük a szállítás időpontjáról.']
                    elif index == 1:
                        add_text.append(
                            f' A következő rendeléseit: {";".join(i[1]["piece_id"])} két napon belül szállítjuk.' \
                            f' A rendelés értéke: {i[1]["cost"]}] Ft')
                        new_row = {'email': add_email, 'text': ' '.join(add_text)}
                        letter.loc[len(letter)] = new_row
                    index += 1
                # print(';'.join(i[1]['piece_id']))
        else:
            continue
    return letter

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 1000)
pandas.set_option('mode.chained_assignment', None)

store_data = pandas.read_csv('raktar.csv', header=None, encoding='iso8859-1', sep=';', engine='pyarrow')
df_order = pandas.read_csv('rendeles.csv',sep=';',header=None, engine='pyarrow')
M = pandas.DataFrame(df_order[df_order[0] == 'M'])
T = pandas.DataFrame(df_order[df_order[0] == 'T'])

M.columns = ['type','date','order_id','email']
M.drop(['type'], inplace=True, axis=1)

T.columns = ['type','order_id','piece_id','pieces']
T.drop(['type'], inplace=True, axis=1)

store_data.columns = ['piece_id','product','price','ammount']
m_t = M.merge(T, on="order_id", validate='one_to_many', how='right')
df_order = store_data.merge(m_t, on="piece_id", validate='one_to_many', how='right')
df_order.drop(['ammount','product'], axis=1, inplace=True)


'''
store_data.loc['P001'] = store_data.loc['P001'].values[2] - 4
print(store_data.loc['P001'].values[2])
'''
df_order['pieces'] = df_order['pieces'].astype(numpy.int64)

store_data,df_order = Store_Change(store_data,df_order)
print(df_order.head(3))
print(store_data.head(3))

order = store_data['ammount'] > store_data['need']
store_data['order'] = abs(store_data['need'].loc[order])
order = store_data['order'].dropna().reset_index()
print(order.head(3))

letter = Check_order(df_order)

letter.to_csv('levelek.csv',index=None,header=None,sep=';')
order.to_csv('beszerzes.csv',index=None,header=None,sep=';')
