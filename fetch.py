# (c) Mika Mäkelä - 2021
# Stock broker classifier

import sys
import requests
import csv
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# Nasdaq instrument code mapping
instrumentMap = {'BICO':'SSE128490',
                'KAMUX':'HEX136667',
                'BOAT':'SSE215704',
                'HARVIA':'HEX152347',
                'ORTHEX':'HEX219193',
                'PUUILO':'HEX227473',
                'GOFORE':'HEX145875',
                'ROVIO':'HEX144044',
                'SIILI':'HEX90227',
                'DIGIA':'HEX24367',
                'SEYE':'SSE128635', 
                'FINNAIR':'HEX24266',
                'REVENIO':'HEX24250',
                'TOKMANNI':'HEX121161',
                'SIEVI':'HEX24348',
                'ASPO':'HEX24236',
                'SPINNOVA':'HEX228201',
                'MARIMEKKO':'HEX24304',
                'EFECTE':'HEX146391',
                'INCAP':'HEX24279',
                'VERK':'HEX100175',
                'CIBUS':'SSE152133',
                'PCELL':'SSE105121',
                'EMBRAC':'SSE128651',
                'VINCIT': 'HEX127211',
                'SOLTEQ':'HEX24354',
                'MERUS':'HEX226474',
                'AKTIA':'HEX69423',
                'OMASP':'HEX163392',
                'NIGHT':'HEX218851',
                'REMEDY':'HEX137987',
                'VERK':'HEX100175',
                'DIGIA':'HEX24367',
                'HUMANA':'SSE120363',
                'MODU':'HEX235016',
                "TEKNO":'HEX24373',
                'INDERES':'HEX235795'}

# Fetch the trades from the Nasdaq
def fetchNasdaq(instrumentCode, from_date_str):

    url = 'http://www.nasdaqomxnordic.com/webproxy/DataFeedProxy.aspx'

    requestXML = '''<post>
    <param name="SubSystem" value="Prices"/>
    <param name="Action" value="GetTrades"/>
    <param name="Exchange" value="NMF"/>
    <param name="t__a" value="30,1,2,7,8,18"/>
    <param name="FromDate" value="''' + from_date_str + '''"/>
    <param name="Instrument" value="''' + instrumentCode + '''"/>
    <param name="ext_contenttype" value="application/ms-excel"/>
    <param name="ext_contenttypefilename" value="share_export.csv"/>
    <param name="ext_xslt" value="/nordicV3/trades_csv.xsl"/>
    <param name="ext_xslt_lang" value="fi"/>
    <param name="showall" value="1"/>
    <param name="app" value="/shares/microsite"/>
    </post>'''

    params = { 'xmlquery': requestXML }

    # The header attributes are required to complete the request
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' ,'Accept-Encoding': 'gzip, deflate' ,'Accept-Language': 'en-US,en;q=0.9,fi;q=0.8,da;q=0.7,sv;q=0.6' ,'Cache-Control': 'no-cache' ,'Content-Type': 'application/x-www-form-urlencoded' ,'Host': 'www.nasdaqomxnordic.com' ,'Origin': 'http://www.nasdaqomxnordic.com' ,'User-Agent': '' }

    r = requests.post(url, data=params, headers=headers, timeout=15)
    print('-> HTTP code: ' + str(r.status_code))

    data = r.text
    data = data.replace('Execution Time', 'Execution_Time')

    # Store data to the dataframe
    df = pd.DataFrame([x.split(';') for x in data.split('\n')])
    df = df.drop([0, 0])
    df.drop(df.tail(1).index,inplace=True)

    print('-> trade count: ' + str(len(df.index)))

    df.columns = df.iloc[0]
    df = df[1:]
    df = df[:-1]
    new_dtypes = {'Volume': int, 'Price': float}
    
    df = df.astype(new_dtypes)

    return df

# Calculate and plot the trades 
def calculateData(symbol, profileDecimals, agg, plotVolumeProfile):
    
    instrumentCode = ''

    # Accept the Nasdaq instrumet codes and the mapped keys
    if symbol.startswith('HEX') or symbol.startswith('SSE'):
        instrumentCode = symbol
    else:
        instrumentCode = instrumentMap[symbol]

    today = datetime.date.today()
    days = datetime.timedelta(int(sys.argv[2]))
    from_date = today - days
    from_date_str = from_date.strftime('%Y-%m-%d')

    print('Fetch Trades...')

    df = fetchNasdaq(instrumentCode, from_date_str)

    # The following commented code is the proto of calculating sum over the multiple symbols
    #df1 = fetchNasdaq(instrumentMap['x'], from_date_str)
    #df2 = fetchNasdaq(instrumentMap['x'], from_date_str)
    #df3 = fetchNasdaq(instrumentMap['x'], from_date_str)
    #df = pd.concat([df1, df2, df3], ignore_index=True)

    print('Format timestamps...')

    # Format the timestamp for the aggregation. The aggregation is done simply by using the datetime string.
    # If group by hour, remove minutes and seconds from the datetime string
    # If group by day, remove hours, minutes and seconds from the datetime string
    for index, row in df.iterrows():
        if agg == 'hour':
            df.at[index, 'Execution_Time'] = row['Execution_Time'][:-6]
        else:
            df.at[index, 'Execution_Time'] = row['Execution_Time'][:-9]

    # Create the classification of the broker codes. Brokers are divided into two classes: privates and foreign/institutions
    foreign = ['ABC' ,'FOR','FORL' ,'ATG' ,'AOI' ,'AIR' ,'ARC' ,'BRC' ,'BPP' ,'MLEX' ,'CAR' ,'CDG', 'CSV' ,'CSGI' ,'CITI' ,'SAB' ,'DREU' ,'FLW' ,'GSAG' ,'GSI' ,'HRTU' ,'TMB' ,'INT' ,'IEGG' ,'JEF' ,'BBB' ,'JPM' ,'JPAG' ,'JTEU','JTEA' ,'KCM' ,'LAGO' ,'LAI' ,'MGF' ,'MWZD' ,'MLI' ,'MMX' ,'MSE' ,'MSI', 'MST' ,'NYE' ,'OPV' ,'PAS' ,'RDX' ,'RBCG' ,'RBCE' ,'RDBN' ,'SGP', 'SGL' ,'SSWM' ,'SIS' ,'TWR' ,'SRE', 'SREA', 'SREB' ,'UBS' ,'UB' ,'VFI', 'VFG', 'VFB', 'VFA' ,'WEB' ,'XTX' ,'XTXE', 'ENS']
    #private = ['NON', 'AVA', 'EVL', 'POH', 'DDB', 'NDS','NDA','NRD','SHB','SHD','SVB','SWB']
    private = ['NON', 'AVA']


    print('Process foreigns...')
    
    # select sells
    dfSell_foreign =  df[(df['Seller'].isin(foreign))]
    gSell_foreign = dfSell_foreign.groupby(['Execution_Time']).agg({'Volume': 'sum', 'Price': 'first'})

    # select buys
    dfBuy_foreign =  df[(df['Buyer'].isin(foreign))]
    gBuy_foreign = dfBuy_foreign.groupby(['Execution_Time']).agg({'Volume': 'sum', 'Price': 'first'})

    # merge buys and sells
    result_foreign = pd.merge(gSell_foreign, gBuy_foreign, on='Execution_Time', suffixes=('_sells_foreign', '_buys_foreign'))

    # calculate net
    result_foreign['net_foreign'] = result_foreign.Volume_buys_foreign - result_foreign.Volume_sells_foreign


    print('Process privates...')

    # select sells
    dfSell_private =  df[(df['Seller'].isin(private))]
    gSell_private = dfSell_private.groupby(['Execution_Time']).agg({'Volume': 'sum', 'Price': 'first'})

    # select buys
    dfBuy_private =  df[(df['Buyer'].isin(private))]
    gBuy_private = dfBuy_private.groupby(['Execution_Time']).agg({'Volume': 'sum', 'Price': 'first'})

    # merge buys and sells by execution time
    result_private = pd.merge(gSell_private, gBuy_private, on='Execution_Time', suffixes=('_sells_private', '_buys_private'))

    # calculate net
    result_private['net_private'] = result_private.Volume_buys_private - result_private.Volume_sells_private

    # merge datasets
    result_all = pd.merge(result_foreign, result_private, on='Execution_Time').reset_index()


    print('Process same Buyer - Seller...')

    dfSame =  df[(df['Buyer'] == df['Seller'])]
    gSame = dfSame.groupby(['Execution_Time']).agg({'Volume': 'sum', 'Price': 'last'}).reset_index()


    print('Calculate shared fields...')

    result_all['volume_all'] = result_all.Volume_buys_foreign + result_all.Volume_buys_private
    result_all['Volume_sells_foreign_neg'] = 0 - result_all.Volume_sells_foreign
    result_all['Volume_sells_private_neg'] = 0 - result_all.Volume_sells_private


    print('Calculate and plot the volume summary...')

    title = symbol + ' / ' + from_date_str + ' - ' + str(today) + ' / interval: ' + agg

    gSell_broker = df.groupby(['Seller']).agg({'Volume': 'sum'}).reset_index()
    gBuy_broker = df.groupby(['Buyer']).agg({'Volume': 'sum'}).reset_index()

    result_broker = pd.merge(gSell_broker, gBuy_broker, how='outer', left_on='Seller', right_on='Buyer', suffixes=('_sells', '_buys'))
    result_broker['net_broker'] = result_broker.Volume_buys.fillna(0) - result_broker.Volume_sells.fillna(0)
    result_broker = result_broker.sort_values('net_broker', ascending=True)

    for index, row in result_broker.iterrows(): # add a flag to the foreign brokers
        for x in foreign:
            if row['Seller'] == x:
                result_broker.at[index, 'Seller'] = row['Seller'] + ' (F)'
            if row['Buyer'] == x:
                result_broker.at[index, 'Buyer'] = row['Buyer'] + ' (F)'

    result_broker[['Buyer','net_broker']].reset_index().plot(x='Buyer',kind='barh', grid=True, title=title)


    if plotVolumeProfile:
        print('Calculate and plot the volume profile...')

        buy_profile_foreign = dfBuy_foreign.round(profileDecimals).groupby(['Price']).agg({'Volume': 'sum'})
        sell_profile_foreign = dfSell_foreign.round(profileDecimals).groupby(['Price']).agg({'Volume': 'sum'})
        volume_profile_foreign = pd.merge(sell_profile_foreign, buy_profile_foreign, on='Price', suffixes=('_sells', '_buys')).reset_index()

        buy_profile_private = dfBuy_private.round(profileDecimals).groupby(['Price']).agg({'Volume': 'sum'})
        sell_profile_private = dfSell_private.round(profileDecimals).groupby(['Price']).agg({'Volume': 'sum'})
        volume_profile_private = pd.merge(sell_profile_private, buy_profile_private, on='Price', suffixes=('_sells', '_buys')).reset_index()

        volume_profile_foreign[['Price','Volume_sells', 'Volume_buys']].plot(x='Price',kind='barh', grid=True, title='Foreing volume profile', color={"Volume_sells": "red", "Volume_buys": "green"})
        volume_profile_private[['Price','Volume_sells', 'Volume_buys']].plot(x='Price',kind='barh', grid=True, title='Private volume profile', color={"Volume_sells": "red", "Volume_buys": "green"})


    # Plot the main chart
    # Section contains experimental plots
    print('Plot...')

    ax = result_all[['Execution_Time','Price_buys_foreign']].plot(x='Execution_Time', kind='line', lw=3, color='dimgrey', title=title, ylabel='Price')
    result_all[['Execution_Time','net_foreign','net_private']].plot(x='Execution_Time', secondary_y=True, ax=ax, kind='bar')
    #result_all[['Execution_Time','Volume_buys_foreign','Volume_sells_foreign_neg','net_foreign']].plot(x='Execution_Time', secondary_y=True, ax=ax, kind='bar', color={'Volume_buys_foreign': 'limegreen', 'Volume_sells_foreign_neg': 'coral', 'net_foreign':'black'})

    #ax2 = result_all[['Execution_Time','Price_buys_private']].plot(x='Execution_Time', kind='line', lw=3, color='darkgray', title=title, ylabel='Price')
    #result_all[['Execution_Time','Volume_buys_foreign','Volume_buys_private']].plot(x='Execution_Time', secondary_y=True, ax=ax2, kind='bar')
    #result_all[['Execution_Time','Volume_buys_private','Volume_sells_private_neg','net_private']].plot(x='Execution_Time', secondary_y=True, ax=ax2, kind='bar', color={'Volume_buys_private': 'lime', 'Volume_sells_private_neg': 'pink', 'net_private':'black'})

    # Plot same buyer seller
    #ax3 = gSame[['Execution_Time','Price']].plot(x='Execution_Time', kind='line', lw=3, color='darkgray', title=title, ylabel='Price')
    #gSame[['Execution_Time','Volume']].plot(x='Execution_Time', secondary_y=True, ax=ax3, kind='bar')
    
    ax.grid(True)
    ax.tick_params(axis='x', rotation=45)

    #ax2.grid(True)
    #ax2.tick_params(axis='x', rotation=45)


def start():

    plotVolumeProfile = False
    agg = ''

    if len(sys.argv) < 4:
        print('Usage: fetch.py [symbol] [number of days] [aggregation] [with volume profile]')
        print('example: fetch.py GOFORE 15 [-day -hour] [-volume]')
        return

    if sys.argv[3] == '-hourly':
        agg = 'hour'
    elif sys.argv[3] == '-daily':
        agg = 'day'
    else:
        print('Incorrect symbol')
        return


    if len(sys.argv) == 5 and sys.argv[4] == '-volume':
        plotVolumeProfile = True

    calculateData(sys.argv[1].upper(), 1, agg, plotVolumeProfile)

    plt.show()

start()