import json
import os
from datetime import datetime, timezone, timedelta

import requests
from discord.ext import commands
from dotenv import load_dotenv
from yahoo_fin import stock_info as si

import db_actions

help_command = commands.DefaultHelpCommand(no_category='Commands')
bot = commands.Bot(command_prefix='!', help_command=help_command)
load_dotenv()

TEST_TOKEN = os.getenv('TEST_TOKEN')
REAL_TOKEN = os.getenv('REAL_TOKEN')

message_str = '{code} ({full_name}) : ${current_price} ' \
              'Daily Change: ${daily_change_amt} ({daily_change_percent}%){wsb_info}'

help_message = 'Available Cryptocurrency tags: BTC, ETH, XRP, BCH, ADA, XLM, NEO, LTC, EOS, XEM, IOTA,' \
               ' DASH, XMR, TRX, ICX, ETC, QTUM, BTG, LSK, USDT, OMG, ZEC, SC, ZRX, REP, WAVES, MKR, DCR,' \
               ' BAT, LRC, KNC, BNT, LINK, CVC, STORJ, ANT, SNGLS, MANA, MLN, DNT, NMR, DAI, ATOM, XTZ,' \
               ' NANO, WBTC, BSV, DOGE, USDC, OXT, ALGO, BAND, BTT, FET, KAVA, PAX, PAXG, REN'


@bot.command(name='stock', help='Shows the price of a given stock (ex. !stock gme)')
async def stock_price_cmd(ctx, code):
    wsb_info = get_wsb_hits(code)
    try:
        price_data = get_stock_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=price_data['name'],
                                          current_price=price_data['current_price'],
                                          daily_change_amt=price_data['daily_change_amt'],
                                          daily_change_percent=price_data['daily_change_percent'],
                                          wsb_info=wsb_info))
    except (AssertionError, KeyError):
        await ctx.send('Unable to find price information for ' + code.upper())


@bot.command(name='crypto', help='Shows the price of a given cryptocurrency (ex. !crypto btc)')
async def crypto_price_cmd(ctx, code):
    try:
        crypto_data = get_crypto_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=crypto_data['name'],
                                          current_price=crypto_data['current_price'],
                                          daily_change_amt=crypto_data['daily_change_amt'],
                                          daily_change_percent=crypto_data['daily_change_percent'],
                                          wsb_info=''))
    except KeyError:
        await ctx.send('Unable to find price information for ' + code.upper() + '\n' + help_message)


@bot.command(name='trade',
             help='Trade an asset. !trade [buy/sell] [stock/crypto] [ticker] [amount]')
async def trade_cmd(ctx, buy_sell, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    if stock_crypto.lower() == 'stock':
        try:
            purchase_price = get_stock_price_data(code)['current_price']
        except:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = get_crypto_price_data(code)['current_price']
        except:
            await ctx.send('Unable to find price information for ' + code.upper() + '\n' + help_message)
            return

    is_sale = 0 if buy_sell.lower() == 'buy' else 1
    await ctx.send(transact_asset(discord_id, discord_name, code, amount, purchase_price, is_sale))


@bot.command(name='portfolio', help='Shows all of your assets by volume')
async def portfolio_cmd(ctx):
    discord_id = ctx.message.author.id
    await ctx.send(ctx.message.author.name + '\'s Portfolio:\n' + json.dumps(check_balance(discord_id), indent=1))


@bot.command(name='reset', help='Resets your account back to $50,000 USD.')
async def reset(ctx):
    reset_balance(ctx.message.author.id)
    await ctx.send(ctx.message.author.name + '\'s Balance Reset to $50000.')


def get_crypto_price_data(code):
    crypto_price_url = 'https://production.api.coindesk.com/v2/price/values/' \
                       '{code}?start_date={start_date}&end_date={end_date}&ohlc=false'

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    url = crypto_price_url.format(code=code.upper(),
                                  start_date=start_date.strftime('%Y-%m-%dT%H:%M'),
                                  end_date=end_date.strftime('%Y-%m-%dT%H:%M'))
    response = requests.get(url)
    data = response.json()
    crypto_name = data['data']['name']
    previous_close_24_hr = data["data"]["entries"][0][1]
    current_price = data["data"]["entries"][95][1]
    daily_change_amt = round(current_price - previous_close_24_hr, 2)
    daily_change_percent = round((daily_change_amt / previous_close_24_hr) * 100, 2)

    return {'name': crypto_name,
            'current_price': str(round(current_price, 2)),
            'daily_change_amt': str(daily_change_amt),
            'daily_change_percent': str(daily_change_percent)}


def get_stock_price_data(code):
    date = datetime.now(timezone.utc)
    day_of_week = date.weekday()
    daily_data = si.get_data(code, start_date=date - timedelta(days=5), end_date=date)
    stock_name = si.get_quote_data(code)['longName']
    current_price = daily_data['close'].values[len(daily_data) - 1]

    if day_of_week in [5, 6]:
        daily_change_amt = 0.00
        daily_change_percent = 0.00
    else:
        previous_close = daily_data['close'].values[len(daily_data) - 2]
        daily_change_amt = round(current_price - previous_close, 2)
        daily_change_percent = round((daily_change_amt / previous_close) * 100, 2)

    return {'name': stock_name,
            'current_price': str(round(current_price, 2)),
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


'''
--- Methods related to the fake user portfolio and transactions
'''


def transact_asset(discord_id, discord_name, asset, amount, price, is_sale):
    if amount == 'max':
        if is_sale == 1:
            volume = db_actions.get_asset_units(discord_id, asset)
        else:
            volume = db_actions.get_asset_units(discord_id, 'USDOLLAR') / float(price)
    elif '$' in amount:
        volume = float(amount.replace('$', '')) / float(price)
    else:
        volume = amount

    total = float(volume) * float(price)

    result = db_actions.make_transaction(discord_id, asset, volume, price, is_sale)
    transact_type = 'Bought' if is_sale == 0 else 'Sold'
    if result.get('is_successful'):
        return '{discord_name} {transact_type} {volume} {asset} for ${total}. USD Balance = ${new_bal}'.format(
            discord_name=discord_name,
            transact_type=transact_type,
            volume=volume, asset=asset,
            total=total,
            new_bal=str(round(float(result.get('available_funds')))))
    else:
        if result.get('message') == 'Insufficient Funds':
            return 'Sorry {discord_name}, you\'re too poor. Available Balance: ${available_bal} ' \
                   'Transaction Cost: ${cost}'.format(
                discord_name=discord_name,
                available_bal=result.get('available_funds'),
                cost=result.get('transaction_cost'))
        elif result.get('message') == 'Insufficient Shares':
            return 'Sorry {discord_name}, you don\'t own enough {asset}. Available {asset}: {available_bal} ' \
                   'Amount Requested: {cost}'.format(
                discord_name=discord_name,
                asset=asset,
                available_bal=result.get('available_funds'),
                cost=amount)


def check_balance(discord_id):
    return db_actions.get_all_assets(discord_id)


def reset_balance(discord_id):
    return db_actions.reset(discord_id)


bot.run(REAL_TOKEN)
