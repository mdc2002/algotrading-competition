import time
import logging
import sys
import csv
import numpy as np
import pandas as pd
import threading

from datetime import datetime

from optibook.synchronous_client import Exchange
from cointegration.cointegration_analysis import estimate_long_run_short_run_relationships, engle_granger_two_step_cointegration_test

exitFlag = 0

instrument_id = 'TEST'
Airbus = 'AIRBUS'
Allianz = 'ALLIANZ'
ASML = 'ASML'
LVMH = 'LVMH'
SAP = 'SAP'
Siemens = 'SIEMENS'
Total = 'TOTAL'
Unilever = 'UNILEVER'



#Function to change order volume:
def change_order_volume(instrument_id, order_id, new_volume, e):
    result = e.amend_order(instrument_id, order_id=order_id, volume=new_volume)
    print(f"Changed volume for order id {order_id} to {new_volume} lots: {result}")
    
    
# Delete all outstanding orders
def delete_current_orders(instrument_id, e):
    outstanding = e.get_outstanding_orders(instrument_id)
    for o in outstanding.values():
        result = e.delete_order(instrument_id, order_id=o.order_id)
        print(f"Deleted order id {o.order_id}: {result}")
        
def get_outstanding_limit_orders(instrument_id, side, e):
    outstanding = e.get_outstanding_orders(instrument_id)
    order_list=[]
    res=0
    for o in outstanding.values():
        res=[o.order_id, o.instrument_id, o.price, o.volume, o.side]
        if o.side == side:
            order_list.append(res)
        #print(f"Outstanding order: order_id({o.order_id}), instrument_id({o.instrument_id}), price({o.price}), volume({o.volume}), side({o.side})")
    return order_list

        
        
# Hack out of all Positions        
def hack_out_of_positions(e):
    pos1=1000
    pos2=1000
    pos3=1000
    pos4=1000
    pos5=1000
    pos6=1000
    pos7=1000
    pos8=1000

    while pos1!=0 or pos2!=0 or pos3!=0 or pos4!=0 or pos5!=0 or pos6!=0 or pos7!=0 or pos8!=0:
        print(e.get_positions())
        for s, p in e.get_positions().items():
            if p > 0:
                e.insert_order(s, price=1, volume=p, side='ask', order_type='ioc')
            elif p < 0:
                e.insert_order(s, price=100000, volume=-p, side='bid', order_type='ioc')

        x=e.get_positions()
        pos1=x['AIRBUS']
        pos2=x['ALLIANZ']
        pos3=x['ASML']
        pos4=x['LVMH']
        pos5=x['SAP']
        pos6=x['SIEMENS']
        pos7=x['TOTAL']
        pos8=x['UNILEVER']
        print(e.get_positions())        
        time.sleep(0.2)
    return 1



#Function that returns the average bid/ask price of an intrument
#Based on the last 100 recorded transactions
def get_average_trade_value(instrument, side, e):
    trade_history=e.get_trade_tick_history(instrument)
    num_trades=0
    sum_prices=0
    for line in trade_history:
        if line.aggressor_side==side:
            #"print (line.timestamp, line.instrument_id, line.price, line.aggressor_side)
            num_trades+=1
            sum_prices+=line.price
    average_price=sum_prices/num_trades
    #print ("Average ", side ," price for ", instrument, "is:", average_price)
    return average_price


# Function that returns the last bid/ask value an intrument:
# Returns 0 if there is no value
def get_last_trade_value(instrument, side, e):
    trade_history=e.get_trade_tick_history(instrument)
    aux=0
    last_price=0
    for line in trade_history:
        if aux==0:
             if line.aggressor_side==side:
                aux=1
                #print (line.timestamp, line.instrument_id, line.price, line.aggressor_side)
                last_price= line.price
        else:
            #print ("Last ", side ," price for ", instrument, "is:", last_price)
            return last_price
        
        
def get_current_price(instrument_id, e):
    book = e.get_last_price_book(instrument_id)
    #for line in book.asks:
     #   print(line)
    if (len(book.asks)<1 or len(book.bids)<1):
        return 0
    else:
        ask_price=book.asks[0].price
        ask_volume=book.asks[0].volume
        bid_price=book.bids[0].price
        bid_volume=book.bids[0].volume
        #print(instrument_id, "  Ask_P:", ask_price, "  Ask_V:", ask_volume, "  Bid_P:", bid_price, "  Bid_V:", bid_volume)
        return (ask_price, ask_volume, bid_price, bid_volume)
    
    
def get_nr_positions(e):    
    x=e.get_positions()
    n_pos_Airbus=x['AIRBUS']
    n_pos_Allianz=x['ALLIANZ']
    n_pos_ASML=x['ASML']
    n_pos_LVMH=x['LVMH']
    n_pos_SAP=x['SAP']
    n_pos_Siemens=x['SIEMENS']
    n_pos_Total=x['TOTAL']
    n_pos_Unilever=x['UNILEVER']
    
    return {Airbus:n_pos_Airbus, Allianz:n_pos_Allianz, ASML:n_pos_ASML, LVMH:n_pos_LVMH, SAP:n_pos_SAP, Siemens:n_pos_Siemens, Total:n_pos_Total, Unilever: n_pos_Unilever}


def get_nr_positions_custom(e):
    current_positions=[]
    positions = e.get_positions()
    for p in positions:
        to_append=[p, positions[p]]
        current_positions.append(to_append)
    
    #print("Current positions: ", current_positions)
    return(current_positions)


def set_limit_order(instrument_id, side, volume, price, e):
    #change order_type
    result_a = e.insert_order(instrument_id, price=price, volume=volume, side=side, order_type='limit')
    #print(f"Order Id: {result_a}", instrument_id, "V:", volume, "P:", price,"Side:", side)



def exec_trade(stock_1, stock_2, volume, e):
    result_1 = e.insert_order(stock_1, price=445.0, volume=volume, side='bid', order_type='ioc')
    #print(f"Order Id: {result_1}", stock_1, "V:", volume, "Side: Bid")
    
    result_2 = e.insert_order(stock_2, price=1, volume=volume, side='ask', order_type='ioc')
    #print(f"Order Id: {stock_2}", stock_2, "V:", volume, "Side: Ask")
    
    #get_last_trade_value(stock_1, 'bid', e)
    #get_last_trade_value(stock_2, 'ask', e)
    
    return (result_1, result_2)


def exec_trade_2vols(stock_buy, stock_sell, volume_buy, volume_sell, e, stock_prices_max, stock_prices_min):
    # same as the regular exec_trade function, except that you can input 2 different volumes
    
    buy_price = stock_prices_max[stock_buy]
    sell_price = stock_prices_min[stock_sell]
    
    #print ("buy_price", buy_price)
    #print ("sell_price", sell_price)
    
    result_1 = e.insert_order(stock_buy, price=buy_price, volume=volume_buy, side='bid', order_type='ioc')
    print("Order", stock_buy, "V:", volume_buy, "Side: Bid")
    
    result_2 = e.insert_order(stock_sell, price=sell_price, volume=volume_sell, side='ask', order_type='ioc')
    print("Order", stock_sell, "V:", volume_sell, "Side: Ask")
    
    #get_last_trade_value(stock_buy, 'bid', e)
    #get_last_trade_value(stock_sell, 'ask', e)
    
    return (result_1, result_2)


#Probably will not need it, needs to be tested again if we want to use it
def do_limit_orders_check(instrument_id, side, undercut,e, trigger_margin=0.2): # Defined an input for side, so we can run the same command for either side.
    # Saves the current bid and ask prices from the price book.
    pb=get_current_price(instrument_id, e)
    askP=pb[0] # This command returns an error message if the price book is empty
    bidP=pb[2]
    
    new_askP=askP-undercut
    new_askV=5 #TBD
    
    new_bidP=bidP+undercut
    new_bidV=5 #TBD
    
    # we get the list of outstanding orders for that instrument on the given side
    orders= get_outstanding_limit_orders(instrument_id, side, e) 
    #if get_outstanding_limit_orders(instrument_id, side) returns [],
    if orders==[]:
        #then  we have to create a new order
        if side == "ask":
            set_limit_order(instrument_id, side, new_askV, new_askP, e)
        elif side == "bid":
            set_limit_order(instrument_id, side, new_bidV, new_bidP, e)


    # now, for each line, we check if the price is what we expected
    # also, in normal circumstances we will only have one line (one limit order). This might break if we also want to be a market maker.
    for line in orders:
        #print(line)
        if side == "ask" and ((line[2] > askP) or line[2] < askP): 
        # Checks that the order is an ask order
        # Checks whether the order price is above the current lowest ask price
        # Checks whether the order price is lower than the current market price (should have some trigger margin). This trigger does not seem to function though.
            # if current ask order is not priced correctly, we delete it and create a new one
            result = e.delete_order(instrument_id, order_id=line[0])
            print(f"Deleted order id {line[0]}: {result}")
            set_limit_order(instrument_id, side, new_askV, new_askP, e)
        if side == "bid" and (line[2] < bidP or line[2] > bidP):
            # if current bid order is not priced correctly, we delete it and create a new one
            result = e.delete_order(instrument_id, order_id=line[0])
            print(f"Deleted order id {line[0]}: {result}")
            set_limit_order(instrument_id, side, new_bidV, new_bidP, e)
            # If current ask order is not priced currectly, delete it and create a new one
        




"""
def check_arb(safety_margin, stock_1, stock_2, e):
    xt=get_current_price(stock_1)
    yt=get_current_price(stock_2)
    c=estimate_long_run_short_run_relationships(data_logs[stock_1], data_logs[stock_2])[0]
    gamma=estimate_long_run_short_run_relationships(data_logs[stock_1], data_logs[stock_2])[1]
    zt=yt[0]-(c + gamma*xt[2])
    
    trade_1=e.get_trade_tick_history(stock_1)
    price_history = []
    for line in trade_1:
        price_history.append(line.price)
    xt_prev=price_history[-1]

    trade_2=e.get_trade_tick_history(stock_2)
    price_history = []
    for line in trade_2:
        price_history.append(line.price)
    yt_prev=price_history[-1]
    
    Hedge_ratio=gamma*(yt_prev/xt_prev)
    
    res=0
    
    while xt==0 or yt==0:
        print ("Waiting for pricebook")
        time.sleep(1)
        xt=get_current_price(stock_1)
        yt=get_current_price(stock_2)
        
    num_positions=get_nr_positions()
    print (f"Nr. Positions {stock_1}:", num_positions[stock_1], 
           f"Nr. Positions {stock_2}:", num_positions[stock_2])
    
    askP_xt=xt[0]
    askV_xt=xt[1]
    bidP_xt=xt[2]
    bidV_xt=xt[3]
    askP_yt=yt[0]
    askV_yt=yt[1]
    bidP_yt=yt[2]
    bidV_yt=yt[3]
    
    
    if zt>safety_margin:
        # Within the min_volume definition, add another alternative that is the difference between the absolute value of our position and 500.
        #KIV volume of trade would depend on p-value etc, will come back later
        min_volume = min(askV_xt, bidV_yt)
        #print ("Min_Vol1", min_volume)
        if (num_positions[stock_1]+min_volume)<500 and (num_positions[stock_2]-min_volume)>-500:
            res=exec_trade(stock_1, stock_2, round(-(1/(Hedge_ratio))))
            
            

    if zt>safety_margin:
        min_volume = min(bidV_xt, askV_yt)
        #print ("Min_Vol2", min_volume)
        if (num_positions[stock_1]+min_volume)<500 and (num_positions[stock_2]-min_volume)>-500:
            res=exec_trade(stock_1, stock_2, round(-(Hedge_ratio)))
    return(res)
"""




def data_loader():
    data=pd.read_csv('cointegration/data.csv')
    data_logs = data
    for i in range(1,len(data.columns)): # converts the returns into natural log returns
        data_logs.iloc[:,i] = np.log(data.iloc[:,i])
    return (data_logs)




def cointeg(stock_1, stock_2, data):
    # stock_1 = stock name string
    # stock_2 = stock name string
    # data is a data frame in which each column is a stock time series and each row is a returns observation
    
    result = []
    result2 = []
    
    result = estimate_long_run_short_run_relationships(data[stock_1], data[stock_2])
    z_series = result[3]
    
    result_2 = engle_granger_two_step_cointegration_test(data[stock_1], data[stock_2])
    
    # returns 2 tuples
    # tuple 1: c, gamma, alpha, z-series
    # tuple 2: test statistic, p-value
    return(result, result_2)





def trading_ratio(y_prev, x_prev, gamma):
    # y_prev = return on Y in the previous period
    # x_prev = return on X in the previous period
    
    answer = gamma * y_prev / x_prev
    
    # returns how many units of X we should buy per unit of Y
    return(answer)




def tabulate_cointeg_pvals_ratios(data, only_significant=0, alpha=0.05):
    #list of tuples
    
    
    # data = dataframe of log returns. 
        # The first column has time stamps. 
        # Each other column is a stock log return time series
        # Each row is the cross-section of log returns at a particular point in time.
    # only_significant = dummy for whether the return dataframes should only include the entries with significant p-values
    # alpha = significance level. Only applicable when only_significant = 1
        
    companies = list(data.columns[1:]) # Extracts a list of the companies used within the dataframe
    
    df_pvals = pd.DataFrame(
    columns = companies,
    index = companies) # Creates an empty 8x8 dataframe with the company names as column- and row names
    
    df_ratios = pd.DataFrame(
    columns = companies,
    index = companies) # Creates an empty 8x8 dataframe with the company names as column- and row names
    
    
    for comp2 in companies:
        for comp1 in companies:            
            #if 
            #check tuples (comp1, comp2)
            #check tuples (comp2, comp1)
            #pass
            
            result = cointeg(comp1, comp2, data) # runs the cointegration analysis for each stock pair 
            pvalue = result[1][1] # extracts cointegration p-value for each stock pair
            gamma = result[0][1] # extracts the gamma for each stock pair
            ratio = trading_ratio(
                data.loc[:, comp1].iloc[-1], 
                data.loc[:, comp2].iloc[-1], 
                gamma
                ) # Generates trading ratios based on the last return in the input time series
                # Needs to be modified such that the function supports trading ratios based on updated returns data.
            
            if only_significant == 0:
                df_pvals.loc[comp1, comp2] = pvalue
                df_ratios.loc[comp1, comp2] = ratio
            
            elif only_significant == 1 and pvalue < alpha:
                df_pvals.loc[comp1, comp2] = pvalue
                df_ratios.loc[comp1, comp2] = ratio
                
            #add to tuple
        
    # Retruns a dataframe of p-values for each stock pair.
    return(df_pvals, df_ratios)




def cointeg_significant_pairs(data, alpha=0.05):
    companies = list(data.iloc[:,1:].columns)
    cointeg_comp_pairs = [] # Empty list to be populated with lists of unique cointegrated stock pairs
    
    for i in range(0,len(companies)):
        for j in range(i+1,len(companies)): # These for-loops iterate over the lower triangular part (excluding the main diagonal) of all possible iterations of company pairs
            #print(i,j) # This line can be un-commented to test which companies this for-loop iterates over
            
            comp1 = companies[i]
            comp2 = companies[j]
            
            result = cointeg(comp1, comp2, data) # runs the cointegration analysis for each stock pair 
            pvalue = result[1][1] # extracts cointegration p-value for each stock pair
            c = result[0][0]
            gamma = result[0][1] # extracts the gamma for each stock pair
            
            if pvalue < alpha:
                cointeg_comp_pairs.append((comp1, comp2, c, gamma))
    
    # Returns a list of tuples. These contain unique cointegrated stock pairs and their respective gamma values
    return(cointeg_comp_pairs)



def tick_logger_csv(data, e, file_name="trade_ticks.csv", overwrite=0): 
    # Pulls all trade ticks for all companies since the last time this command was run and writes them to a csv file
    # overwrite:
        # 1 = create entirely new dataframe with new data
        # 0 = append new data to the dataframe that was created last time this code was run
    # This command only needs a 'data' input for the purpose of extracting the column names. Can be changed to instead require a list of column names
    
    
    
    ##### Creates a list of companies #####
    companies = list(data.columns)[1:] # creates a list of the companies in the investment universe


    ##### Creates a list of dictionaries containing trade tick data #####
    # Only contains the data since the last time this command was run
    # There are other commands for getting trade tick data from since the exchange was initiated, but those commands return a different trade tick class which lacks some information (such as time stamps)
    rows_list = [] # Creats an empty list to be populated with a dictionary for each trade tick
    for comp in companies: # loops over all companies
        tradeticks = e.poll_new_trade_ticks(comp) # extracts a trade tick object containing all trade ticks for a given company
        for t in tradeticks:
            # puts the relevant trade tick data into a dicitonary
            dict={"time": t.timestamp, "stock": t.instrument_id, "price": t.price, "volume": t.volume, "side": t.aggressor_side, "buyer": t.buyer, "seller": t.seller, "trade_nr": t.trade_nr}
            rows_list.append(dict) # appends the tick data onto the list we created earlier


    ##### Creates dataframe of trade tick data and writes them to a csv file #####
    appended_rows = pd.DataFrame(rows_list, columns = ["time", "stock", "price", "volume", "aggressor_side", "buyer", "seller", "trade_nr"])
    #print(appended_rows)
    if overwrite==0:
        #df_ticks = df_ticks.append(appended_rows, ignore_index=True)
        appended_rows.to_csv(file_name, mode='a', header=False) # Adds new data to a pre-existing csv file
        #print("Successfully appended trade tick history to", file_name)
    elif overwrite==1:
        #df_ticks = pd.DataFrame(rows_list)
        appended_rows.to_csv(file_name, mode='w+') # Overwrites all data in the csv file
        #print("Successfully overwrote trade tick history in", file_name)
    
    return(1)



# Saves current PnL, positions and outstanding prices to a .csv file
def trades_and_pnl_logger(data, e, file_name="trades_and_pnl.csv", overwrite=0):
    ##### Creates a list of companies #####
    companies = list(data.columns)[1:] # creates a list of the companies in the investment universe
    length = len(companies)

    positions=get_nr_positions(e)
    
    ##### Pulls current PnL #####
    pnl = e.get_pnl()
    
    
    ##### pre-creates data to be appended to .csv file
    to_append = {"time": datetime.now(), "pnl": pnl}

    
    ##### pulls current prices and outstanding volumes
    for comp in companies:
        comp_prices = get_current_price(comp, e)
        while comp_prices == 0:
            print("Waiting for price to log")
            time.sleep(0.1)
            comp_prices = get_current_price(comp, e)
        
        to_append[comp+"_pos"] = positions[comp]
        to_append[comp+"_ask"] = comp_prices[0]
        to_append[comp+"_bid"] = comp_prices[2]
    
    
    ##### Prepares to save data to .csv
    columns = ["time", "pnl"]
    for comp in companies:
        columns.append(comp+"_pos")
        columns.append(comp+"_ask")
        columns.append(comp+"_bid")
    
    #print(columns)
    to_be_appended = pd.DataFrame(to_append, columns = columns, index=[0])
    #print(to_be_appended)
    
    
    ##### Writes data to .csv file #####
    if overwrite==0:
        to_be_appended.to_csv(file_name, mode='a', header=False) # Adds new data to a pre-existing csv file
        #print("Successfully appended trade tick history to", file_name)
    elif overwrite==1:
        to_be_appended.to_csv(file_name, mode='w+') # Overwrites all data in the csv file
        #print("Successfully overwrote trade tick history in", file_name)
    
    return(1)


def check_zt(stock_y, stock_x, c, gamma, e):
    yt=get_current_price(stock_y, e)
    xt=get_current_price(stock_x, e)
    
    # retries the function if the pricebook is ever empty
    while xt==0 or yt==0:
        #print ("Waiting for pricebook")
        time.sleep(0.1)
        yt=get_current_price(stock_y, e)
        xt=get_current_price(stock_x, e)

    askP_xt=xt[0]
    bidP_xt=xt[2]
    askP_yt=yt[0]
    bidP_yt=yt[2]

    # Going up when A is too expensive relative to B - hence sell A and buy B
    # Going down when A is too cheap relative to B - hence buy A and sell B
    zt_up = np.log(bidP_yt) - (c + gamma * np.log(askP_xt))
    zt_down = np.log(askP_yt) - (c + gamma * np.log(bidP_xt))
    
    # returns zt as well as the current prices for the given stock pair
    return(zt_up, zt_down, yt, xt)



####################################
##### Volume storage functions #####
####################################

# Creates storage list based on desired nr of levels and the existing nr of pairs
def creates_volume_storage(cointeg_comp_pairs, max_levels): 
    volume_storage = [] # Added this line so if you run the command multiple times, the table doesn't balloon in size (/Emil)
    for pair in cointeg_comp_pairs:
        
        aux=0
        stock_A=pair[0]
        stock_B=pair[1]
        new_pair = [stock_A, stock_B]
        new_pair_storage=[]
        new_pair_storage.append(new_pair)
        empty_volumes=[0,0]
        #print(new_pair_storage)
        while aux<max_levels:
            new_pair_storage.append(empty_volumes)
            aux +=1
        volume_storage.append(new_pair_storage)

    #print("Volume storage: ", volume_storage)
    # Returns an empty list of lists like [[[stock_a, stock_b], [vol_a1, vol_b1], [vol_a2, vol_b2], ...], [[stock_c, stock_d], [col_c1, vol_d1], ...]]
    return(volume_storage)


def store_volumes(stock_A, stock_B, levels_deep, base_volume, volume_hedge, volume_storage):
    # Stores volumes in volume_storage
    success=0
    aux_levels=abs(levels_deep)
    for pair_table in volume_storage: 
        if (pair_table[0] == [stock_A, stock_B]):
            new_entry=[base_volume, volume_hedge] 
            pair_table[aux_levels]=new_entry
            success=1

    #print('Stored Volumes: ', volume_storage)
    if success==0:
        print ('ERROR_1: Volumes not stored')
    #elif success ==1:
        #print('Successfully stored volumes')
    # Returns a list of volumes we've traded at different levels for the given stock pair
    return(volume_storage)

        
def compare_positions(current_positions, e): 
    # Compares the positions stored in current_positions to our actual positions according to the exchange, and returns a list of the differences.
    
    new_positions=[]
    positions = e.get_positions()
    aux=0
    for p in positions:
        pair=current_positions[aux]
        new_number= positions[p]-pair[1]
        to_append=[p, new_number]
        new_positions.append(to_append)
        aux+=1
    #print("New positions: ", new_positions)
    return(new_positions)


def compare_positions_pair(stock_A, stock_B, current_positions, e):
    # Compares the positions of a particular stock pair, stored in current_positions, to our actual positions for that pair, and returns a list of the differences.
    new_positions_pair=[]
    positions = e.get_positions()
    stocks = [stock_A, stock_B]
    
    for stock in stocks:
        for p in current_positions:
            if p[0] == stock:
                to_append = positions[stock] - p[1]
                new_positions_pair.append(to_append)
            
    #print("New pair positions: ", new_positions_pair)
    return(new_positions_pair)



def creates_levels_deep_storage(cointeg_comp_pairs): 
    levels_deep_list=[]
    for pair in cointeg_comp_pairs:
        new_pair_level=0
        levels_deep_list.append(new_pair_level)
    #print (levels_deep_list)
    return (levels_deep_list)


def creates_pairs_storage(cointeg_comp_pairs): 
    pair_list = []
    for pair in cointeg_comp_pairs:
        stock_A=pair[0]
        stock_B=pair[1]
        new_pair = [stock_A, stock_B]
        pair_list.append(new_pair)
    #print (pair_list)
    return (pair_list)  


def create_gamma_list(cointeg_comp_pairs):
    gamma_list=[]
    for pair in cointeg_comp_pairs:
        gamma=pair[3]
        gamma_list.append(gamma)
    #print (gamma_list)
    return (gamma_list) 


def create_c_list(cointeg_comp_pairs):
    c_list=[]
    for pair in cointeg_comp_pairs:
        c=pair[2]
        c_list.append(c)
    #print (c_list)
    return (c_list)  


# Takes a list of 2 values describing the volumes we are currently missing, and executes trades to adjust our position
def fix_positions(stock_A, stock_B, volume_difs, e, stock_prices_max, stock_prices_min):
    
    buy_price_A = stock_prices_max[stock_A]
    sell_price_A = stock_prices_min[stock_A]
    buy_price_B = stock_prices_max[stock_B]
    sell_price_B = stock_prices_min[stock_B]


    volume_A=volume_difs[0]
    volume_B=volume_difs[1]
    res_A=0
    res_B=0

    
    if volume_A<0:
        res_A=e.insert_order(stock_A, price=sell_price_A, volume=abs(volume_A), side='ask', order_type='ioc')
    elif volume_A>0:
        res_A=e.insert_order(stock_A, price=buy_price_A, volume=abs(volume_A), side='bid', order_type='ioc')
    
    if volume_B<0:
        res_B=e.insert_order(stock_B, price=sell_price_B, volume=abs(volume_B), side='ask', order_type='ioc')
    elif volume_B>0:
        res_B=e.insert_order(stock_B, price=buy_price_B, volume=abs(volume_B), side='bid', order_type='ioc')
        
    return (res_A, res_B)


# working, returns overall (sum of all levels) volumes for each stock 
# returns the volumes WE ARE MISSING
def check_volumes(stock_A, stock_B, max_levels,volume_storage, current_positions ):
    #PART1: Calculates sum of volumes for each stock (in sum_stock_A and sum_stock_B)
    aux=1
    sum_stock_A=0
    sum_stock_B=0
    dif_stock_A=0
    dif_stock_B=0
    pair=[stock_A, stock_B]
    for pair_table in volume_storage:
        if pair_table[0]==pair:
            while aux <= max_levels:
                sum_stock_A+=pair_table[aux][0]
                sum_stock_B+=pair_table[aux][1]
                aux+=1
                
    #PART2: Compares to current positions
    for position in current_positions:
        if position[0]==stock_A and position[1]!=sum_stock_A:
            dif_stock_A=sum_stock_A-position[1]
        if position[0]==stock_B and position[1]!=sum_stock_B:
            dif_stock_B=sum_stock_B-position[1]
            
            
    if dif_stock_A!=0 or dif_stock_B!=0:
        return (dif_stock_A, dif_stock_B)
    
    else:
        return (0)


####################################


##### zt_trader Tests #####
levels_deep = 0
# minimum-working prototype

# the volumes should actually be set such that neither base_volume nor (base_volume * hedging_ratio) exceed 250, perhaps with some safety margin. 
# This way, we can quickly take a large position to exploit the arbitrage opportunity

# Needs to have a global counter of how many "levels" into the trade we are, such that we can dynamically exit out as the spread gets smaller

# When we add more levels, we'll need to make the levels_deep variable change by adding or subtracting 1 to it, rather than by setting it to a specific value.
# However, when I tried this, I ran into a bug with the function returning levels_deep = (stupidly large number). Might need investigating

def zt_trader_first_iteration(stock_y, stock_x, c, gamma, levels_deep, e, limits=[0.002, 0.002, 0.003], base_volume=20):
    limit_1 = limits[0]
    limit_2 = limits[1]
    limit_3 = limits[2]
    
    zt, yt, xt = check_zt(stock_y, stock_x, c, gamma, e) # pulls the value of zt, as well as the stock prices used to calculate it
    # perhaps it's unnecessary to create these variables until we're inside one of the if-statements, since we're using unnecessary computing power
    askP_xt=xt[0]
    bidP_xt=xt[2]
    askP_yt=yt[0]
    bidP_yt=yt[2]
    
    
    
    
    res=0
    if levels_deep == 0:
        
        if zt > limit_1:
            #zt > limit_1 implies that y is too expensive relative to x -> buy x and sell y
            # execute trade so levels_deep=1
            
            hedge_ratio = trading_ratio(bidP_yt, askP_xt, gamma) # doesn't work properly at the moment
            volume_hedge = round(base_volume*hedge_ratio)
            
            res = exec_trade_2vols(stock_x, stock_y, volume_hedge, base_volume, e)

            levels_deep = 1
            
        if zt < -limit_1:
            #zt < limit_1 implies that y is too cheap relative to x -> buy y and sell x
            # execute trade so levels_deep=-1
            
            hedge_ratio = trading_ratio(askP_yt, bidP_xt, gamma)
            volume_hedge = round(base_volume*hedge_ratio)
            
            res = exec_trade_2vols(stock_y, stock_x, base_volume, volume_hedge, e)
            
            levels_deep = -1
    
    elif levels_deep == 1:
        positions=get_nr_positions(e)
        if zt < 0:
            # execute trade so levels_deep=0
            res = exec_trade_2vols(stock_y, stock_x, abs(positions[stock_y]), abs(positions[stock_x]), e)
            
            levels_deep = 0
    
    elif levels_deep == -1:
        positions=get_nr_positions(e)
        if zt > 0:
            # execute trade so levels_deep=0
            res = exec_trade_2vols(stock_x, stock_y, abs(positions[stock_x]), abs(positions[stock_y]), e)
            
            levels_deep = 0
    
    # prints for debugging purposes
    #print(f"Now {levels_deep} levels deep")
    #print(f"zt: {zt}")

    return(res, levels_deep)

####################################
##### zt_trader main iteration #####
####################################





# This function is used for executing trades when z_t is moving away from zero
def trade_deeper(going_up, stock_A, stock_B, levels_deep, at, bt, gamma, base_volume, e, stock_prices_max, stock_prices_min):
    
    res=0
    hedge_ratio = 0
    volume_hedge = 0
    
    
    if going_up==1:
        # Going up when A is too expensive relative to B - hence sell A and buy B
        price_A=at[2]
        price_B=bt[0]
        new_base_volume = 0
        
        hedge_ratio = trading_ratio(price_A, price_B, gamma)
        if hedge_ratio > 1: 
            inv_base_volume = round(base_volume/hedge_ratio)
            volume_hedge = base_volume
            res = exec_trade_2vols(stock_B, stock_A, volume_hedge, inv_base_volume, e, stock_prices_max, stock_prices_min)
            new_base_volume= inv_base_volume*-1
            
            #print ("HEDGE_RATIO: ", hedge_ratio)
            return(res, volume_hedge, new_base_volume)
        else:
            volume_hedge = round(base_volume*hedge_ratio)
            res = exec_trade_2vols(stock_B, stock_A, volume_hedge, base_volume, e, stock_prices_max, stock_prices_min)
            new_base_volume= base_volume*-1
            
            #print ("HEDGE_RATIO: ", hedge_ratio)
            return(res, volume_hedge, new_base_volume)
        
    
        
        
        
    else: 
        # Going down when A is too cheap relative to B - hence buy A and sell B
        price_A=at[0]
        price_B=bt[2]
    
        hedge_ratio = trading_ratio(price_A, price_B, gamma)
        if hedge_ratio > 1: 
            inv_base_volume = round(base_volume/hedge_ratio)
            volume_hedge = base_volume
            res = exec_trade_2vols(stock_A, stock_B, inv_base_volume,volume_hedge, e, stock_prices_max, stock_prices_min)
            new_volume_hedge= volume_hedge*-1
            
            #print ("HEDGE_RATIO: ", hedge_ratio)
            return(res,new_volume_hedge, inv_base_volume)
            
        else:
            volume_hedge = round(base_volume*hedge_ratio)
            res = exec_trade_2vols(stock_A, stock_B, base_volume, volume_hedge, e, stock_prices_max, stock_prices_min)
            new_volume_hedge= volume_hedge*-1
    
            #print ("HEDGE_RATIO: ", hedge_ratio)
            return(res, new_volume_hedge, base_volume)
    

    
    return (0)
    


# This function is used for executing trades when z_t is moving towards zero
def trade_shallower(stock_A, stock_B, levels_deep, volume_storage, e, stock_prices_max, stock_prices_min):
    vol_A = 0
    vol_B = 0
    res = 0
    
    # Extracts the volumes we should trade
    for pair_table in volume_storage: 
        if (pair_table[0] == [stock_A, stock_B]):
            vol_A = abs(pair_table[abs(levels_deep)][0])
            vol_B = abs(pair_table[abs(levels_deep)][1])
            #print(vol_A, vol_B)
    
    if levels_deep > 0: # checks that we've sold A
        # Sell A and buy B
        res = exec_trade_2vols(stock_A, stock_B, vol_A, vol_B, e, stock_prices_max, stock_prices_min)
        
    elif levels_deep < 0: # checks that we've bought A
        # Sell B and buy A
        #print(vol_B, vol_A)
        res = exec_trade_2vols(stock_B, stock_A, vol_B, vol_A, e, stock_prices_max, stock_prices_min)
    
    return(res)




def automated_trading(current_positions,levels_deep_list, volume_storage, pair_list, max_levels, limits, base_volume, aux, e, c_list, gamma_list, stock_prices_max, stock_prices_min):
    time_var=0
    aux=aux
    while time_var==0:
        stock_A=pair_list[aux][0]
        stock_B=pair_list[aux][1]    
        

        res = 0
        res, levels_deep_list[aux], current_positions, volume_storage = zt_trader(stock_A, stock_B, c_list[aux], gamma_list[aux], levels_deep_list[aux], current_positions, volume_storage, e, limits, base_volume, stock_prices_max, stock_prices_min)


        # Check goal volumes vs actual positions
        volume_difs_1= check_volumes(stock_A, stock_B, max_levels, volume_storage, current_positions)
        while volume_difs_1 !=0:
            #print ("VOLUME DIFF HERE")
            print(volume_difs_1)
            fix_res=fix_positions(stock_A, stock_B, volume_difs_1, e, stock_prices_max, stock_prices_min)
            current_positions = get_nr_positions_custom(e) 
            #print ("FIXED volume diff. Res: ", fix_res)
            volume_difs_1= check_volumes(stock_A, stock_B, max_levels, volume_storage, current_positions)
            time.sleep(0.1)

        #print("--------------------------------")
        #time.sleep(0.1)


    
# Checks zt for a given stock pair and compares it to levels_deep for that pair, to determine whether we should execute a trade
# The function then executes that trade.

def zt_trader(stock_y, stock_x, c, gamma, levels_deep, current_positions, volume_storage, e, limits, base_volume, stock_prices_max, stock_prices_min):
    """
    limit_1 = limits[0]
    limit_2 = limits[1]
    limit_3 = limits[2]
    """
    base_volume = round(base_volume)
    
    res = 0
    
    #used_limit = abs(levels_deep)
    
    zt_up, zt_down, yt, xt = check_zt(stock_y, stock_x, c, gamma, e) # pulls the value of zt, as well as the stock prices used to calculate it

    shallower_critical_limit = limits[abs(levels_deep)-1]
    
    # Checks whether zt is below (current limit - 1), using different values for zt depending on whether an evaluation of TRUE would imply trading up (levels_deep increases) or trading down (levels_deep decreases).
    if (zt_down < shallower_critical_limit and levels_deep > 0) or (abs(zt_up) < shallower_critical_limit and levels_deep < 0) or (zt_down < 0 and levels_deep == 1) or (zt_up > 0 and levels_deep == -1):
        
        # trade shallower
        res = trade_shallower(stock_y, stock_x, levels_deep, volume_storage, e, stock_prices_max, stock_prices_min)
        #print("stored volume using levels_deep =", levels_deep) # prints for debugging purposes
        
        volume_storage = store_volumes(stock_y, stock_x, abs(levels_deep), 0,0, volume_storage) # Resets the stored volume for this level
        current_positions = get_nr_positions_custom(e) # Stores our new actual positions
        # Needs to check whether we're actually at zero. Otherwise we need to do another trade.
        
        
        # Decrease abs(levels_deep) by 1
        if levels_deep > 0:
            levels_deep -= 1
        elif levels_deep < 0:
            levels_deep += 1
    
    
    elif abs(levels_deep) != len(limits)-1: # Checks that we are not at the deepest level
        if zt_up > 0 and abs(zt_up) > limits[abs(levels_deep)+1]: # Checks whether zt has exceeded a limit (except for the last one)
            
            # Checks whether we've already bought in at this limit
            for pair_table in volume_storage:
                if (pair_table[0] == [stock_y, stock_x]):
                    #print(pair_table)
                    if pair_table[abs(levels_deep)+1] == [0, 0]:
                        
                        # Trade deeper up
                        res = trade_deeper(1, stock_y, stock_x, levels_deep, yt, xt, gamma, base_volume, e, stock_prices_max, stock_prices_min)
                        #print(res)
                        levels_deep += 1
        
                        hedge_volume_to_store=res[1]
                        base_volume_to_store=res[2]
                        volume_storage = store_volumes(stock_y, stock_x, levels_deep, base_volume_to_store, hedge_volume_to_store, volume_storage) # Stores the traded volume
                        current_positions = get_nr_positions_custom(e) # Stores our new actual positions
        
        
        if zt_down < 0 and abs(zt_down) > limits[abs(levels_deep)+1]: # Checks whether zt has exceeded a limit (except for the last one)

            # Checks whether we've already bought in at this limit
            for pair_table in volume_storage:
                if (pair_table[0] == [stock_y, stock_x]):
                    #print(pair_table)
                    if pair_table[abs(levels_deep)+1] == [0, 0]:

                        # Trade deeper down
                        res = trade_deeper(0, stock_y, stock_x, levels_deep, yt, xt, gamma, base_volume, e, stock_prices_max, stock_prices_min)
                        #print(res)
                        levels_deep -= 1


                        hedge_volume_to_store=res[1]
                        base_volume_to_store=res[2]
                        volume_storage = store_volumes(stock_y, stock_x, levels_deep, base_volume_to_store, hedge_volume_to_store, volume_storage) # Stores the traded volume
                        current_positions = get_nr_positions_custom(e) # Stores our new actual positions
                        

    # prints for debugging purposes
    # print(f"Now {levels_deep} levels deep on ", stock_y, stock_x)
    # print(f"zt_up: {zt_up}, zt_down: {zt_down}")

    return(res, levels_deep, current_positions, volume_storage)




# Saves current PnL, positions and outstanding prices to a .csv file
def trades_and_pnl_logger(data, e, file_name="trades_and_pnl.csv", overwrite=0):
    ##### Creates a list of companies #####
    companies = list(data.columns)[1:] # creates a list of the companies in the investment universe
    length = len(companies)

    positions=get_nr_positions(e)
    
    ##### Pulls current PnL #####
    pnl = e.get_pnl()
    
    
    ##### pre-creates data to be appended to .csv file
    to_append = {"time": datetime.now(), "pnl": pnl}

    
    ##### pulls current prices and outstanding volumes
    for comp in companies:
        comp_prices = get_current_price(comp, e)
        while comp_prices == 0:
            print("Waiting for price to log")
            time.sleep(0.1)
            comp_prices = get_current_price(comp, e)
        
        to_append[comp+"_pos"] = positions[comp]
        to_append[comp+"_ask"] = comp_prices[0]
        to_append[comp+"_bid"] = comp_prices[2]
    
    
    ##### Prepares to save data to .csv
    columns = ["time", "pnl"]
    for comp in companies:
        columns.append(comp+"_pos")
        columns.append(comp+"_ask")
        columns.append(comp+"_bid")
    
    #print(columns)
    to_be_appended = pd.DataFrame(to_append, columns = columns, index=[0])
    #print(to_be_appended)
    
    
    ##### Writes data to .csv file #####
    if overwrite==0:
        to_be_appended.to_csv(file_name, mode='a', header=False) # Adds new data to a pre-existing csv file
        #print("Successfully appended trade tick history to", file_name)
    elif overwrite==1:
        to_be_appended.to_csv(file_name, mode='w+') # Overwrites all data in the csv file
        #print("Successfully overwrote trade tick history in", file_name)
    
    return(1)
        


#Tests

def trading_test(stock_a, stock_b, data):
    res_cointegrate=[]
    res_cointegrate=cointeg(stock_a, stock_b, data)
    print (res_cointegrate[0], res_cointegrate[1])
    

def exchange_test(e):
    print ("second test", e)
    
    
    
# FOR OUR EYES ONLY

def cheat_code(companies, e):
    print ("BATOTA")
    
    comps = ["TOTAL", "UNILEVER", "SAP", "ASML", "LVMH", "ALLIANZ"] # This list is just for the competition. If it's not the competition, remove this.
    
    #for comp in companies:
    for comp in comps:
        outstanding = e.get_outstanding_orders(comp)
        for o in outstanding.values():
            result = e.delete_order(comp, order_id=o.order_id)

        e.insert_order(comp, price=10.0, volume=80, side='bid', order_type='limit')
        time.sleep(1)
        e.insert_order(comp, price=490.0, volume=80, side='ask', order_type='limit')
        time.sleep(1)



    
