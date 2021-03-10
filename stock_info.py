import os
from datetime import datetime, timezone, timedelta

import discord
import requests
from discord.ext import commands
from dotenv import load_dotenv
from yahoo_fin import stock_info as si

import db_actions

help_command = commands.DefaultHelpCommand(no_category='Commands')
intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix='!', help_command=help_command, intents=intents)
load_dotenv()

TEST_TOKEN = os.getenv('TEST_TOKEN')
REAL_TOKEN = os.getenv('REAL_TOKEN')

message_str = '{code} ({full_name}) : ${current_price} ' \
              'Daily Change: ${daily_change_amt} ({daily_change_percent}%){wsb_info}'

help_message = 'Available Cryptocurrency tags: BTC, ETH, XRP, BCH, ADA, XLM, NEO, LTC, EOS, XEM, IOTA,' \
               ' DASH, XMR, TRX, ICX, ETC, QTUM, BTG, LSK, USDT, OMG, ZEC, SC, ZRX, REP, WAVES, MKR, DCR,' \
               ' BAT, LRC, KNC, BNT, LINK, CVC, STORJ, ANT, SNGLS, MANA, MLN, DNT, NMR, DAI, ATOM, XTZ,' \
               ' NANO, WBTC, BSV, DOGE, USDC, OXT, ALGO, BAND, BTT, FET, KAVA, PAX, PAXG, REN'


@bot.before_invoke
async def wake(x):
    db_actions.wake_up_db()


@bot.command(name='stock', help='Shows the price of a given stock (ex. !stock gme)', aliases=['s'])
async def stock_price_cmd(ctx, code):
    wsb_info = get_wsb_hits(code)
    try:
        price_data = get_stock_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=price_data['name'],
                                          current_price=str(round(float(price_data['current_price']), 2)),
                                          daily_change_amt=price_data['daily_change_amt'],
                                          daily_change_percent=price_data['daily_change_percent'],
                                          wsb_info=wsb_info))
    except (AssertionError, KeyError):
        await ctx.send('Unable to find price information for ' + code.upper())


@bot.command(name='crypto', help='Shows the price of a given cryptocurrency (ex. !crypto btc)', aliases=['c'])
async def crypto_price_cmd(ctx, code):
    try:
        crypto_data = get_crypto_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=crypto_data['name'],
                                          current_price=str(round(float(crypto_data['current_price']), 2)),
                                          daily_change_amt=crypto_data['daily_change_amt'],
                                          daily_change_percent=crypto_data['daily_change_percent'],
                                          wsb_info=''))
    except KeyError:
        await ctx.send('Unable to find price information for ' + code.upper() + '\n' + help_message)


@bot.command(name='buy', help='Buy an asset. !buy [stock/crypto] [ticker] [amount]')
async def buy_cmd(ctx, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    if not is_crypto:
        try:
            purchase_price = get_stock_price_data(code)['current_price']
        except (AssertionError, KeyError):
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = get_crypto_price_data(code)['current_price']
        except KeyError:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    await ctx.send(transact_asset(discord_id, discord_name, code, amount, purchase_price, 0, is_crypto))


@bot.command(name='sell', help='Sell an asset. !sell [stock/crypto] [ticker] [amount]')
async def sell_cmd(ctx, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    if not is_crypto:
        try:
            purchase_price = get_stock_price_data(code)['current_price']
        except (AssertionError, KeyError):
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = get_crypto_price_data(code)['current_price']
        except KeyError:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    await ctx.send(transact_asset(discord_id, discord_name, code, amount, purchase_price, 1, is_crypto))


@bot.command(name='portfolio', help='Shows all of your assets by volume', aliases=['pf'])
async def portfolio_cmd(ctx, *args):
    if len(args) != 0:
        user_id = ''
        for m in ctx.message.guild.members:
            if m.name == args[0]:
                user_id = m.id
        if user_id == '':
            await ctx.send('Can \'t find portfolio for ' + args[0])
            return
        await ctx.send("'''" + args[0] + '\'s Portfolio:\n' +
                       format_portfolio(check_balance(user_id)) + "'''")
    else:
        await ctx.send("```" + ctx.message.author.name + '\'s Portfolio:\n' +
                       format_portfolio(check_balance(ctx.message.author.id)) + "```")


@bot.command(name='history', help='Shows 10 most recent transactions')
async def history_cmd(ctx):
    await ctx.send("```" + get_formatted_transaction_history(ctx.message.author.id) + "```")


@bot.command(name='leaderboard', help='Who da winner?', aliases=['lb'])
async def leaderboard_cmd(ctx):
    mem_dict = {}
    for m in ctx.message.guild.members:
        mem_dict[m.id] = m.name
    await ctx.send(get_formatted_leaderboard(mem_dict))


@bot.command(name='reset', help='Resets your account back to $50,000 USD.')
async def reset(ctx):
    db_actions.reset(ctx.message.author.id)
    await ctx.send(ctx.message.author.name + '\'s Balance Reset to $50000.')


# Coindesk API
# def get_crypto_price_data(code):
#     crypto_price_url = 'https://production.api.coindesk.com/v2/price/values/' \
#                        '{code}?start_date={start_date}&end_date={end_date}&ohlc=false'
#
#     end_date = datetime.now(timezone.utc)
#     start_date = end_date - timedelta(days=1)
#     url = crypto_price_url.format(code=code.upper(),
#                                   start_date=start_date.strftime('%Y-%m-%dT%H:%M'),
#                                   end_date=end_date.strftime('%Y-%m-%dT%H:%M'))
#     response = requests.get(url)
#     data = response.json()
#     crypto_name = data['data']['name']
#     previous_close_24_hr = data["data"]["entries"][0][1]
#     try:
#         current_price = data["data"]["entries"][95][1]
#     except IndexError:
#         current_price = data["data"]["entries"][94][1]
#     daily_change_amt = round(current_price - previous_close_24_hr, 2)
#     daily_change_percent = round((daily_change_amt / previous_close_24_hr) * 100, 2)
#
#     return {'name': crypto_name,
#             'current_price': str(round(current_price, 2)),
#             'daily_change_amt': str(daily_change_amt),
#             'daily_change_percent': str(daily_change_percent)}


# Crypto.com API
# def get_crypto_price_data(code):
#     asset_info = None
#     page = 1
#     total_pages = 999
#     while asset_info is None or page > total_pages:
#         print('Page ' + str(page))
#         crypto_price_url = 'https://crypto.com/price/coin-data/summary/by_market_cap_page_{page}.json'
#         response = requests.get(crypto_price_url.format(page=page))
#         total_pages = response.json()['page_count']
#         data = response.json()['tokens']
#         asset_info = next((a for a in data if a['symbol'].lower() == code.lower()), None)
#         page += 1
#
#     crypto_name = asset_info['name']
#     current_price = asset_info['usd_price']
#     daily_change_percent = round(asset_info['usd_change_24h'] * 100, 2)
#     daily_change_amt = current_price * asset_info['usd_change_24h']
#
#     return {'name': crypto_name,
#             'current_price': str(round(current_price, 2)),
#             'daily_change_amt': str(daily_change_amt),
#             'daily_change_percent': str(daily_change_percent)}


# Binance API
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
    daily_data = si.get_data(code, start_date=date - timedelta(days=5), end_date=date)
    try:
        stock_name = si.get_quote_data(code)['longName']
    except KeyError:
        stock_name = si.get_quote_data(code)['shortName']

    current_price = daily_data['close'].values[len(daily_data) - 1]

    if day_of_week in [5, 6]:
        daily_change_amt = 0.00
        daily_change_percent = 0.00
    else:
        previous_close = daily_data['close'].values[len(daily_data) - 2]
        daily_change_amt = round(current_price - previous_close, 2)
        daily_change_percent = round((daily_change_amt / previous_close) * 100, 2)

    return {'name': stock_name,
            'current_price': str(current_price),
            'daily_change_amt': str(daily_change_amt),
            'daily_change_percent': str(daily_change_percent)}


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
    if amount == 'max':
        if is_sale == 1:
            volume = db_actions.get_asset_units(discord_id, asset)[0]
        else:
            volume = db_actions.get_asset_units(discord_id, 'USDOLLAR')[0] / float(price)
    elif '$' in amount:
        volume = float(amount.replace('$', '')) / float(price)
    else:
        volume = amount
    total = float(volume) * float(price)

    transaction_result = db_actions.make_transaction(discord_id, asset, volume, price, is_sale, is_crypto)
    transact_type = 'Bought' if is_sale == 0 else 'Sold'
    if transaction_result.get('is_successful'):
        return '{discord_name} {transact_type} {volume} {asset} at ${cost_per_unit}ea. for ${total}.' \
               ' USD Balance = ${new_bal}'.format(
            discord_name=discord_name,
            transact_type=transact_type,
            volume=round(float(volume), 4),
            asset=asset.upper(),
            cost_per_unit=round(float(price), 2),
            total=round(total, 2),
            new_bal=str(round(float(transaction_result.get('available_funds')))))
    else:
        if transaction_result.get('message') == 'Insufficient Funds':
            return 'Sorry {discord_name}, you\'re too poor. Available Balance: ${available_bal} ' \
                   'Transaction Cost: ${cost}'.format(
                discord_name=discord_name,
                available_bal=transaction_result.get('available_funds'),
                cost=transaction_result.get('transaction_cost'))
        elif transaction_result.get('message') == 'Insufficient Shares':
            return 'Sorry {discord_name}, you don\'t own enough {asset}. Available {asset}: {available_bal} ' \
                   'Amount Requested: {cost}'.format(
                discord_name=discord_name,
                asset=asset.upper(),
                available_bal=transaction_result.get('available_funds'),
                cost=amount)


def get_price_of_asset(code, is_crypto):
    return get_crypto_price_data(code)['current_price'] if is_crypto == 1 \
        else get_stock_price_data(code)['current_price']


def check_balance(discord_id):
    assets = db_actions.get_all_assets(discord_id)
    for index, asset in enumerate(assets):
        if assets[index]['name'] == 'USDOLLAR':
            assets[index]['current_value'] = assets[index]['shares']
            assets[index]['current_unit_price'] = assets[index]['avg_price']
        else:
            assets[index]['current_unit_price'] = float(get_price_of_asset(assets[index]['name'],
                                                                           assets[index]['is_crypto']))
            assets[index]['current_value'] = assets[index]['current_unit_price'] * float(assets[index]['shares'])
    total = sum(float(assets[index]['current_value']) for index, asset in enumerate(assets))

    return assets, total


def format_portfolio(assets_info):
    p_string = 'Total Value: $' + str(round(assets_info[1], 2)) + "\n"
    assets = assets_info[0]
    for index, asset in enumerate(assets):
        p_string += '\n{asset} Volume: {volume}Value: ${value}Average Paid Price: ${avg_price}' \
                    'Current Price: ${current_price}'.format(
            asset=str(assets[index]['name']).upper().ljust(8),
            volume=str(round(assets[index]['shares'], 2)).ljust(20),
            value=str(round(assets[index]['current_value'], 2)).ljust(20),
            avg_price=str(round(assets[index]['avg_price'], 2)).ljust(20),
            current_price=str(round(assets[index]['current_unit_price'], 2)).ljust(20))
    return p_string


def get_formatted_leaderboard(server_members):
    users = db_actions.get_all_users()
    user_totals = []
    for user in users:
        if int(user) in server_members.keys():
            user_totals.append({'name': server_members[int(user)], 'total': check_balance(user)[1]})

    lb_string = ''
    for index, user in enumerate(sorted(user_totals, key=lambda i: i['total'], reverse=True)):
        lb_string += '{place}. {name}: ${total}\n'.format(place=index + 1,
                                                          name=user['name'],
                                                          total=round(user['total'], 2))

    return lb_string


def get_formatted_transaction_history(discord_id):
    transactions = db_actions.get_transaction_history(discord_id)

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


bot.run(REAL_TOKEN)
