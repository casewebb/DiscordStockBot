import os
from datetime import datetime, timezone, timedelta

import discord
import requests
from discord.ext import commands, tasks
from dotenv import load_dotenv
from yahoo_fin import stock_info as si

import db_actions

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix='!', help_command=None, intents=intents)
load_dotenv()

TOKEN = os.getenv('TOKEN')

message_str = '{code} ({full_name}) : ${current_price} ' \
              'Daily Change: ${daily_change_amt} ({daily_change_percent}%){wsb_info}.'


@bot.before_invoke
async def wake(x):
    db_actions.wake_up_db()


@bot.command(name='help')
async def help_cmd(ctx, *args):
    message = "```Commands:\n" \
              "  alert       Set a price alert for a given asset. !alert [crypto/stock] [ticker] [< or >] [price]\n" \
              "  alerts      Shows all active alerts.\n" \
              "  xalert      Delete an alert. !xalert [id] *id comes from !alerts command.\n" \
              "  crypto      Shows the price of a given cryptocurrency. (ex. !crypto btc)\n" \
              "  stock       Shows the price of a given stock. (ex. !stock gme)\n" \
              "  buy         Buy an asset. !buy [stock/crypto] [ticker] [amount]\n" \
              "  sell        Sell an asset. !sell [stock/crypto] [ticker] [amount]\n" \
              "  limit       !help limit for more info\n" \
              "  liquidate   Sell all assets at market value.\n" \
              "  portfolio   Shows all of your assets by volume.\n" \
              "  history     Shows 10 most recent transactions.\n" \
              "  leaderboard Shows ordered list of participants by portfolio value.\n" \
              "  reset       Resets your account back to $50,000 USD." \
              "```"
    if len(args) > 0 and args[0] == 'limit':
        limit_help_msg = '```Create a limit order. !limit [buy/sell] [crypto/stock] [amount] [ticker] [< or >] [price]\n' \
                         '[amount] works the same as a normal buy/sell, you can use max, $ value, or number of units.\n\n' \
                         'You can have as many orders as you want active, but it will not tell you whether your balance ' \
                         'will allow the transaction to go through until the condition is met. ' \
                         'So be sure you leave USD in ' \
                         'your account for a purchase, or assets in your account for a sale.\n\n' \
                         '!orders to see all of your active limit orders.\n' \
                         '!xorder [id] ID comes from !orders command. This deletes a limit order.\n\n' \
                         'Also I didn\'t test this. Fuck you.```'
        await ctx.send(limit_help_msg)
        return

    await ctx.send(message)


@bot.command(name='setname')
async def set_name_cmd(ctx, name):
    discord_id = ctx.message.author.id
    db_actions.set_display_name(discord_id, name)


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
    users = db_actions.get_all_users()
    if len(args) != 0:
        user_id = ''
        for m in ctx.message.guild.members:
            if m.name.lower() == args[0].lower() or m.display_name.lower() == args[0].lower():
                user_id = m.id
        if user_id == '':
            for index, user in enumerate(users[1]):
                if user.lower() == args[0].lower():
                    user_id = users[0][index]
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


@bot.command(name='alert', help='Set a price alert for a given asset. !alert [crypto/stock] [ticker] [< or >] [price]',
             aliases=['a'])
async def alert_cmd(ctx, stock_crypto, code, direction, price):
    channel_id = ctx.channel.id
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    price = price.replace('$', '').replace(',', '')
    if direction == '<':
        direction = 1
    elif direction == '>':
        direction = 0
    else:
        return

    db_actions.create_alert(channel_id=channel_id,
                            asset=code,
                            is_crypto=is_crypto,
                            is_less_than=direction,
                            price=price)

    await ctx.send(format_alerts(ctx.channel.id))


@bot.command(name='alerts', help='View all active alerts.')
async def alerts_cmd(ctx):
    await ctx.send(format_alerts(ctx.channel.id))


@bot.command(name='xalert', help='Delete an alert !xalert [id]')
async def delete_alert_cmd(ctx, alert_id):
    db_actions.delete_alert(alert_id)
    await ctx.send('Alert Deleted.')


@bot.command(name='limit', help='!limit [buy/sell] [crypto/stock] [amount] [ticker] [< or >] [price]', aliases=['l'])
async def limit_order_cmd(ctx, buy_sell, stock_crypto, amount, code, direction, price):
    channel_id = ctx.channel.id
    discord_id = ctx.message.author.id
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    is_sale = 0 if (buy_sell.lower() == 'buy' or buy_sell.lower() == 'b') else 1
    price = price.replace('$', '').replace(',', '')
    if direction == '<':
        direction = 1
    elif direction == '>':
        direction = 0
    else:
        return

    db_actions.create_limit_order(discord_id=discord_id,
                                  channel_id=channel_id,
                                  asset=code,
                                  volume=amount,
                                  is_crypto=is_crypto,
                                  is_sale=is_sale,
                                  is_less_than=direction,
                                  price=price)

    await ctx.send(format_limit_orders(discord_id))


@bot.command(name='orders', help='View all active limit orders.')
async def orders_cmd(ctx):
    await ctx.send(format_limit_orders(ctx.message.author.id))


@bot.command(name='xorder', help='Delete an order !xorder [id]')
async def delete_order_cmd(ctx, order_id):
    db_actions.delete_limit_order(order_id, ctx.message.author.id)
    await ctx.send('Order Deleted.')


# BACKGROUND TASKS

@tasks.loop(minutes=5)
async def check_alerts():
    alerts = db_actions.get_all_alerts()
    for a in alerts:
        channel = bot.get_channel(int(a.channel_id))
        if channel is None:
            return
        try:
            current_price = get_crypto_price_data(a.asset_code)['current_price'] if a.is_crypto \
                else get_stock_price_data(a.asset_code)['current_price']
        except Exception:
            await channel.send('Issue with alert ' + str(a.id) + ' it will be deleted.')
            await channel.send(format_alerts(a.channel_id))
            db_actions.delete_alert(a.id)
            return
        if a.is_less_than:
            if float(current_price) < float(a.price_per_unit):
                await channel.send('```PRICE ALERT: {asset} below {alert_price}! Current price: {current_price}.```'
                                   .format(asset=a.asset_code.upper(),
                                           alert_price=str(round(a.price_per_unit, 2)),
                                           current_price=str(round(float(current_price), 2))))
                db_actions.delete_alert(a.id)
        else:
            if float(current_price) > float(a.price_per_unit):
                await channel.send('```PRICE ALERT: {asset} above {alert_price}! Current price: {current_price}.```'
                                   .format(asset=a.asset_code.upper(),
                                           alert_price=str(round(a.price_per_unit, 2)),
                                           current_price=str(round(float(current_price), 2))))
                db_actions.delete_alert(a.id)


@tasks.loop(minutes=5)
async def check_limit_orders():
    orders = db_actions.get_limit_orders(None)
    for o in orders:
        display_name = db_actions.get_display_name(o.discord_id)
        channel = bot.get_channel(int(o.channel_id))
        if channel is None:
            return

        try:
            current_price = get_crypto_price_data(o.asset_code)['current_price'] if o.is_crypto \
                else get_stock_price_data(o.asset_code)['current_price']
        except Exception:
            await channel.send('Issue with ' + display_name + '\'s order ' + str(o.id) + ' it will be deleted.')
            await channel.send(format_limit_orders(o.discord_id))
            db_actions.delete_limit_order(o.id, o.discord_id)
            return
        if o.is_less_than:
            if float(current_price) < float(o.price_per_unit):
                await channel.send(transact_asset(o.discord_id, display_name,
                                                  o.asset_code, o.volume, current_price, o.is_sale,
                                                  o.is_crypto))
                db_actions.delete_limit_order(o.id, o.discord_id)
        else:
            if float(current_price) > float(o.price_per_unit):
                await channel.send(transact_asset(o.discord_id, display_name,
                                                  o.asset_code, o.volume, current_price, o.is_sale,
                                                  o.is_crypto))
                db_actions.delete_limit_order(o.id, o.discord_id)


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
    p_string = 'Total Value: $' + '{:,.2f}'.format(assets_info[1]) + "\n\n"
    assets = assets_info[0]
    p_string += '|Asset'.ljust(11) + '|Volume'.ljust(15) + '|Value'.ljust(15) + '|Average Cost'.ljust(16) + \
                '|Current Cost'.ljust(14) + '|% Change|'
    p_string += '\n|----------|--------------|--------------|---------------|-------------|--------|'

    for asset in assets:
        decimals = 3 if asset['avg_price'] < 10 and asset['name'].upper() != 'USDOLLAR' else 2
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


def get_pcnt_change(val1, val2):
    return (val1 - val2) / val2 * 100


def format_leaderboard(server_members):
    users = db_actions.get_all_users()
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


def format_alerts(channel_id):
    alerts = db_actions.get_all_alerts()
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
    orders = db_actions.get_limit_orders(discord_id)
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


check_alerts.start()
check_limit_orders.start()
bot.run(TOKEN)
