from datetime import datetime, timezone, timedelta

import logging
import requests
from yahoo_fin import stock_info as si

from util import database_connector
from decimal import *

logging.basicConfig(filename='stock_bot_log.log', level=logging.INFO)
getcontext().prec = 30
getcontext().rounding = ROUND_DOWN


def get_crypto_price_data(code):
    crypto_price_url = 'https://www.binance.com/gateway-api/v2/public/asset-service/product/get-products?includeEtf=false'

    response = requests.get(crypto_price_url)
    data = response.json()['data']
    asset_info = next((a for a in data if (a['b'].lower() == code.lower() and 'usd' in a['q'].lower())), None)

    crypto_name = asset_info['an']
    current_price = float(asset_info['c'])
    previous_close_24_hr = float(asset_info['o'])
    daily_change_amt = current_price - previous_close_24_hr
    daily_change_percent = round((daily_change_amt / previous_close_24_hr) * 100, 2)

    return {'name': crypto_name,
            'current_price': str(current_price),
            'daily_change_amt': str(round(daily_change_amt, 2)),
            'daily_change_percent': str(daily_change_percent)}


def get_stock_price_data(code):
    date = datetime.now(timezone.utc)
    day_of_week = date.weekday()
    daily_data = si.get_quote_data(code)
    try:
        stock_name = daily_data['longName']
    except KeyError:
        stock_name = daily_data['shortName']

    try:
        current_price = daily_data['postMarketPrice']
    except KeyError:
        current_price = daily_data['regularMarketPrice']

    if day_of_week in [5, 6]:
        daily_change_amt = 0.00
        daily_change_percent = 0.00
    else:
        previous_close = daily_data['regularMarketPreviousClose']
        daily_change_amt = round(current_price - previous_close, 2)
        daily_change_percent = round((daily_change_amt / previous_close) * 100, 2)

    return {'name': stock_name,
            'current_price': str(current_price),
            'daily_change_amt': str(daily_change_amt),
            'daily_change_percent': str(daily_change_percent)}


def get_price_of_asset(code, is_crypto):
    return float(get_crypto_price_data(code)['current_price']) if is_crypto == 1 \
        else float(get_stock_price_data(code)['current_price'])


def get_wsb_hits(code):
    date = datetime.now(timezone.utc)
    today = str(int(date.timestamp()))
    one_day_ago = str(int((date - timedelta(days=1)).timestamp()))
    url = 'https://elastic.pushshift.io/rc/comments/_search?source=' \
          '{"query":{"bool":{"must":[{"simple_query_string":{"query":"%(code)s","fields":["body"],' \
          '"default_operator":"and"}}],"filter":[{"range":{"created_utc":{"gte":%(one_day_ago)s,"lte":%(today)s}}},' \
          '{"terms":{"subreddit":["wallstreetbets"]}}],"should":[],"must_not":[]}},' \
          '"size":100,"sort":{"created_utc":"desc"}}'

    try:
        formatted_url = (url % {'code': code, 'today': today, 'one_day_ago': one_day_ago})
        response_data = requests.get(formatted_url, headers={'referer': 'https://redditsearch.io/',
                                                             'origin': 'https://redditsearch.io'}).json()
        hits = response_data['hits']['total']
    except Exception:
        hits = 0
    if hits > 0:
        return ' {hits} hits on r/wallstreetbets in the last 24 hours'.format(hits=hits)
    else:
        return ''


def transact_asset(discord_id, discord_name, asset, amount, price, is_sale, is_crypto):
    logging.info('PRICE IN: ' + str(price))
    price = Decimal(price)
    info = database_connector.get_asset_units(discord_id, asset)
    avg_price = Decimal(info[1])
    if amount == 'max':
        if is_sale == 1:
            volume = Decimal(info[0])
        else:
            volume = Decimal(database_connector.get_asset_units(discord_id, 'USDOLLAR')[0]) / price
            logging.info('ESTIMATED PURCHASABLE MAX VOL: ' + str(volume))
    elif '$' in amount:
        if float(amount.replace('$', '')) <= 0:
            return "You can only trade positive amounts of an asset."
        volume = Decimal(amount.replace('$', '')) / price
    else:
        if float(amount) <= 0:
            return "You can only trade positive amounts of an asset."
        volume = Decimal(amount)
    total = volume * price
    logging.info('Calculated total: ' + str(total))
    net_p_l = total - (volume * avg_price)

    transaction_result = database_connector.make_transaction(discord_id, asset, volume, price, is_sale, is_crypto)
    if transaction_result.get('is_successful'):
        if is_sale:
            profit_loss = '+' if net_p_l >= 0.0 else '-'
            return '{discord_name} sold {volume} {asset} at ${cost_per_unit}ea. for ${total} ' \
                   '({profit_loss_str}${profit_loss_amt}). USD Balance = ${new_bal}.'.format(
                discord_name=discord_name,
                volume=round(volume, 4),
                asset=asset.upper(),
                cost_per_unit=round(price, 2),
                total=round(total, 2),
                profit_loss_str=profit_loss,
                profit_loss_amt='{:,.2f}'.format(abs(net_p_l)),
                new_bal=round(transaction_result.get('available_funds')))
        else:
            return '{discord_name} bought {volume} {asset} at ${cost_per_unit}ea. for ${total}.' \
                   ' USD Balance = ${new_bal}'.format(
                discord_name=discord_name,
                volume=round(volume, 4),
                asset=asset.upper(),
                cost_per_unit=round(price, 2),
                total=round(total, 2),
                new_bal=round(transaction_result.get('available_funds')))
    else:
        if transaction_result.get('message') == 'Insufficient Funds':
            return 'Sorry {discord_name}, you\'re too poor. Available Balance: ${available_bal} ' \
                   'Transaction Cost: ${cost}'.format(
                discord_name=discord_name,
                available_bal=round(transaction_result.get('available_funds'), 3),
                cost=round(transaction_result.get('transaction_cost'), 3))
        elif transaction_result.get('message') == 'Insufficient Shares':
            return 'Sorry {discord_name}, you don\'t own enough {asset}. Available {asset}: {available_bal} ' \
                   'Amount Requested: {cost}'.format(
                discord_name=discord_name,
                asset=asset.upper(),
                available_bal=transaction_result.get('available_funds'),
                cost=amount)


def check_balance(discord_id):
    assets = database_connector.get_all_assets(discord_id)
    for asset in assets:
        if asset['name'] == 'USDOLLAR':
            asset['current_value'] = asset['shares']
            asset['current_unit_price'] = asset['avg_price']
        else:
            asset['current_unit_price'] = get_price_of_asset(asset['name'], asset['is_crypto'])
            asset['current_value'] = asset['current_unit_price'] * asset['shares']
    total = sum(asset['current_value'] for asset in assets)

    return assets, total


def get_pcnt_change(val1, val2):
    return (val1 - val2) / val2 * 100


def format_portfolio(assets_info):
    pages = []
    p_string = 'Total Value: $' + '{:,.2f}'.format(assets_info[1]) + "\n\n"
    assets = assets_info[0]
    p_string += '|Asset'.ljust(11) + '|Volume'.ljust(15) + '|Value'.ljust(15) + '|Average Cost'.ljust(16) + \
                '|Current Cost'.ljust(14) + '|% Change|'
    p_string += '\n|----------|--------------|--------------|---------------|-------------|--------|'

    for asset in assets:
        decimals = 3 if asset['avg_price'] < 10 and asset['name'].upper() != 'USDOLLAR' else 2
        if asset['name'].upper() == 'USDOLLAR' and asset['shares'] < .0000001:
            continue
        p_string += '\n|{asset}|{volume}|${value}|${avg_price}|${current_price}|{pcnt_chg}|'.format(
            asset=asset['name'].upper().ljust(10),
            volume=str(round(asset['shares'], 4)).ljust(14),
            value='{:,.2f}'.format(asset['current_value']).ljust(13),
            avg_price='{:,.{decimals}f}'.format(asset['avg_price'], decimals=decimals).ljust(14),
            current_price='{:,.{decimals}f}'.format(asset['current_unit_price'], decimals=decimals).ljust(12),
            pcnt_chg=str(str(round(get_pcnt_change(asset['current_unit_price'], asset['avg_price']), 2)) + '%').ljust(
                8))
        if len(p_string) > 1750:
            pages.append(p_string)
            p_string = '|Asset'.ljust(11) + '|Volume'.ljust(15) + '|Value'.ljust(15) + '|Average Cost'.ljust(16) + \
                       '|Current Cost'.ljust(14) + '|% Change|'
            p_string += '\n|----------|--------------|--------------|---------------|-------------|--------|'
    pages.append(p_string)
    return pages


def format_leaderboard(server_members):
    users = database_connector.get_all_users()
    user_totals = []
    for index, user in enumerate(users[0]):
        if int(user) in server_members.keys():
            name = server_members[int(user)] if users[1][index] is None else users[1][index]
            user_totals.append({'name': name, 'total': check_balance(user)[1]})

    lb_string = ''
    for index, user in enumerate(sorted(user_totals, key=lambda i: i['total'], reverse=True)):
        lb_string += '{place}. {name}: ${total}\n'.format(place=index + 1,
                                                          name=user['name'],
                                                          total='{:,.2f}'.format(user['total']))
    return lb_string


def format_transaction_history(discord_id):
    transactions = database_connector.get_transaction_history(discord_id)
    transactions_string = 'Recent Transactions:'
    for t in transactions:
        action = 'Sold' if t.is_sale == 1 else 'Bought'
        total = t.volume * t.price_per_unit
        transactions_string += '\n[{date}] {action} {volume} {asset} at {cost_per_unit}/{asset} for ${total}.'.format(
            date=t.transaction_date,
            action=action,
            volume=round(t.volume, 4),
            asset=t.asset_code.upper(),
            cost_per_unit=round(t.price_per_unit, 3),
            total=round(total, 3))
    return transactions_string


def format_alerts(channel_id):
    alerts = database_connector.get_all_alerts()
    alerts_string = 'Active Alerts:'
    if len(alerts) == 0:
        return 'No Active Alerts for this Channel.'
    for a in alerts:
        if int(a.channel_id) == int(channel_id):
            a_b_str = '<' if a.is_less_than else '>'
            alerts_string += '\n[{id}] {asset} {above_below} {price}'.format(id=a.id,
                                                                             asset=a.asset_code.upper(),
                                                                             above_below=a_b_str,
                                                                             price=round(a.price_per_unit, 2))
    return '```' + alerts_string + '```'


def format_limit_orders(discord_id):
    orders = database_connector.get_limit_orders(discord_id)
    orders_string = 'Active Limit Orders:'
    if len(orders) == 0:
        return 'You have no standing limit orders.'
    for o in orders:
        if int(o.discord_id) == int(discord_id):
            b_s_str = 'Sell' if o.is_sale else 'Purchase'
            a_b_str = '<' if o.is_less_than else '>'

            orders_string += '\n[{id}] {buy_sell} {volume} {asset} when the price is {above_below} ${price}.'.format(
                id=o.id,
                asset=o.asset_code.upper(),
                above_below=a_b_str,
                price=round(o.price_per_unit, 2),
                volume=o.volume,
                buy_sell=b_s_str)

    return '```' + orders_string + '```'
