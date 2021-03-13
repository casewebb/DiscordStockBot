import os
from datetime import datetime, timezone, timedelta

import discord
import requests
from discord.ext import commands
from dotenv import load_dotenv
from yahoo_fin import stock_info as si

import db_actions

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix='!', help_command=None, intents=intents)
load_dotenv()

TOKEN = os.getenv('TOKEN')

message_str = '{code} ({full_name}) : ${current_price} ' \
              'Daily Change: ${daily_change_amt} ({daily_change_percent}%){wsb_info}'


@bot.before_invoke
async def wake(x):
    db_actions.wake_up_db()


@bot.command(name='help')
async def help_cmd(ctx):
    message = "```Commands:\n" \
              "  crypto      Shows the price of a given cryptocurrency. (ex. !crypto btc)\n" \
              "  stock       Shows the price of a given stock. (ex. !stock gme)\n" \
              "  buy         Buy an asset. !buy [stock/crypto] [ticker] [amount]\n" \
              "  sell        Sell an asset. !sell [stock/crypto] [ticker] [amount]\n" \
              "  liquidate   Sell all assets at market value.\n" \
              "  portfolio   Shows all of your assets by volume.\n" \
              "  history     Shows 10 most recent transactions.\n" \
              "  leaderboard Shows ordered list of participants by portfolio value.\n" \
              "  reset       Resets your account back to $50,000 USD." \
              "```"
    await ctx.send(message)


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
    except Exception:
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
    except Exception:
        await ctx.send('Unable to find price information for ' + code.upper())


@bot.command(name='buy', help='Buy an asset. !buy [stock/crypto] [ticker] [amount]')
async def buy_cmd(ctx, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    if not is_crypto:
        try:
            purchase_price = get_stock_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = get_crypto_price_data(code)['current_price']
        except Exception:
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
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = get_crypto_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    await ctx.send(transact_asset(discord_id, discord_name, code, amount, purchase_price, 1, is_crypto))


@bot.command(name='liquidate', help='Sell all assets at market value.', aliases=['liq'])
async def liquidate_cmd(ctx):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    assets = db_actions.get_all_assets(ctx.message.author.id)
    for asset in assets:
        if asset['name'].lower() == 'usdollar':
            continue
        if asset['is_crypto']:
            price = get_crypto_price_data(asset['name'])['current_price']
        else:
            price = get_stock_price_data(asset['name'])['current_price']
        await ctx.send(transact_asset(discord_id, discord_name, asset['name'],
                                      'max', price, 1, asset['is_crypto']))
    await ctx.send('All assets sold.')


@bot.command(name='portfolio', help='Shows all of your assets by volume', aliases=['pf'])
async def portfolio_cmd(ctx, *args):
    if len(args) != 0:
        user_id = ''
        for m in ctx.message.guild.members:
            if m.name.lower() == args[0].lower() or m.display_name.lower() == args[0].lower():
                user_id = m.id
        if user_id == '':
            await ctx.send('Can \'t find portfolio for ' + args[0])
            return
        pages = format_portfolio(check_balance(user_id))
        for index, page in enumerate(pages):
            await ctx.send("```" + args[0] + '\'s Portfolio (Page {page}):\n'.format(page=str(index + 1)) +
                           page + "```")
    else:
        pages = format_portfolio(check_balance(ctx.message.author.id))
        for index, page in enumerate(pages):
            await ctx.send(
                "```" + ctx.message.author.name + '\'s Portfolio (Page {page}):\n'.format(page=str(index + 1)) +
                page + "```")


@bot.command(name='history', help='Shows 10 most recent transactions')
async def history_cmd(ctx):
    await ctx.send("```" + format_transaction_history(ctx.message.author.id) + "```")


@bot.command(name='leaderboard', help='Who da winner?', aliases=['lb'])
async def leaderboard_cmd(ctx):
    mem_dict = {}
    for m in ctx.message.guild.members:
        mem_dict[m.id] = m.name
    await ctx.send("```" + format_leaderboard(mem_dict) + "```")


@bot.command(name='reset', help='Resets your account back to $50,000 USD.')
async def reset(ctx):
    db_actions.reset(ctx.message.author.id)
    await ctx.send(ctx.message.author.name + '\'s Balance Reset to $50000.')


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
    price = float(price)
    info = db_actions.get_asset_units(discord_id, asset)
    avg_price = info[1]
    if amount == 'max':
        if is_sale == 1:
            volume = info[0]
        else:
            volume = db_actions.get_asset_units(discord_id, 'USDOLLAR')[0] / price
    elif '$' in amount:
        if float(amount.replace('$', '')) <= 0:
            return "You can only trade positive amounts of an asset."
        volume = float(amount.replace('$', '')) / price
    else:
        if float(amount) <= 0:
            return "You can only trade positive amounts of an asset."
        volume = float(amount)
    total = volume * price
    net_p_l = total - (volume * avg_price)

    transaction_result = db_actions.make_transaction(discord_id, asset, volume, price, is_sale, is_crypto)
    if transaction_result.get('is_successful'):
        if is_sale:
            profit_loss = 'profit' if net_p_l >= 0.0 else 'loss'
            return '{discord_name} sold {volume} {asset} at ${cost_per_unit}ea. for ${total}.' \
                   ' Trade {profit_loss_str} = ${profit_loss_amt}. USD Balance = ${new_bal}'.format(
                discord_name=discord_name,
                volume=round(volume, 4),
                asset=asset.upper(),
                cost_per_unit=round(price, 2),
                total=round(total, 2),
                profit_loss_str=profit_loss,
                profit_loss_amt=round(abs(net_p_l), 2),
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
    return float(get_crypto_price_data(code)['current_price']) if is_crypto == 1 \
        else float(get_stock_price_data(code)['current_price'])


def check_balance(discord_id):
    assets = db_actions.get_all_assets(discord_id)
    for asset in assets:
        if asset['name'] == 'USDOLLAR':
            asset['current_value'] = asset['shares']
            asset['current_unit_price'] = asset['avg_price']
        else:
            asset['current_unit_price'] = get_price_of_asset(asset['name'], asset['is_crypto'])
            asset['current_value'] = asset['current_unit_price'] * asset['shares']
    total = sum(asset['current_value'] for asset in assets)

    return assets, total


def format_portfolio(assets_info):
    pages = []
    p_string = 'Total Value: $' + str(round(assets_info[1], 2)) + "\n"
    assets = assets_info[0]
    for asset in assets:
        decimals = 3 if asset['avg_price'] < 10 else 2
        p_string += '\n{asset} Vol: {volume}Value: ${value}Avg. Paid Price: ${avg_price}' \
                    'Current Price: ${current_price} ({pcnt_chg}%)'.format(
            asset=asset['name'].upper().ljust(10),
            volume=str(round(asset['shares'], 4)).ljust(15),
            value=str(round(asset['current_value'], 2)).ljust(12),
            avg_price=str(round(asset['avg_price'], decimals)).ljust(10),
            current_price=str(round(asset['current_unit_price'], decimals)).ljust(10),
            pcnt_chg=str(round(get_pcnt_change(asset['current_unit_price'], asset['avg_price']), 2)))
        if len(p_string) > 1750:
            pages.append(p_string)
            p_string = ''
    pages.append(p_string)
    return pages


def get_pcnt_change(val1, val2):
    return (val1 - val2) / val2 * 100


def format_leaderboard(server_members):
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


def format_transaction_history(discord_id):
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


bot.run(TOKEN)
