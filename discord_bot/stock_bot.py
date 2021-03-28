import os

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from discord_bot import database_connector, helpers

intents = discord.Intents.default()
intents.members = True
bot = commands.Bot(command_prefix='!', help_command=None, intents=intents)
load_dotenv()

TOKEN = os.getenv('TOKEN')

message_str = '{code} ({full_name}) : ${current_price} ' \
              'Daily Change: ${daily_change_amt} ({daily_change_percent}%){wsb_info}.'


@bot.before_invoke
async def wake(x):
    database_connector.wake_up_db()


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
                         '!xorder [id] ID comes from !orders command. This deletes a limit order.```'
        await ctx.send(limit_help_msg)
        return

    await ctx.send(message)


@bot.command(name='setname')
async def set_name_cmd(ctx, name):
    discord_id = ctx.message.author.id
    database_connector.set_display_name(discord_id, name)


@bot.command(name='reset')
async def reset(ctx):
    database_connector.reset(ctx.message.author.id)
    await ctx.send(ctx.message.author.name + '\'s Balance Reset to $50000.')


@bot.command(name='stock', aliases=['s'])
async def stock_price_cmd(ctx, code):
    wsb_info = helpers.get_wsb_hits(code)
    try:
        price_data = helpers.get_stock_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=price_data['name'],
                                          current_price=str(round(float(price_data['current_price']), 2)),
                                          daily_change_amt=price_data['daily_change_amt'],
                                          daily_change_percent=price_data['daily_change_percent'],
                                          wsb_info=wsb_info))
    except Exception:
        await ctx.send('Unable to find price information for ' + code.upper())


@bot.command(name='crypto', aliases=['c'])
async def crypto_price_cmd(ctx, code):
    try:
        crypto_data = helpers.get_crypto_price_data(code)
        await ctx.send(message_str.format(code=code.upper(),
                                          full_name=crypto_data['name'],
                                          current_price=str(round(float(crypto_data['current_price']), 2)),
                                          daily_change_amt=crypto_data['daily_change_amt'],
                                          daily_change_percent=crypto_data['daily_change_percent'],
                                          wsb_info=''))
    except Exception:
        await ctx.send('Unable to find price information for ' + code.upper())


@bot.command(name='buy')
async def buy_cmd(ctx, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    if not is_crypto:
        try:
            purchase_price = helpers.get_stock_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = helpers.get_crypto_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    await ctx.send(helpers.transact_asset(discord_id, discord_name, code, amount, purchase_price, 0, is_crypto))


@bot.command(name='sell')
async def sell_cmd(ctx, stock_crypto, code, amount):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    is_crypto = 0 if (stock_crypto.lower() == 'stock' or stock_crypto.lower() == 's') else 1
    if not is_crypto:
        try:
            purchase_price = helpers.get_stock_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    else:
        try:
            purchase_price = helpers.get_crypto_price_data(code)['current_price']
        except Exception:
            await ctx.send('Unable to find price information for ' + code.upper())
            return
    await ctx.send(helpers.transact_asset(discord_id, discord_name, code, amount, purchase_price, 1, is_crypto))


@bot.command(name='liquidate', aliases=['liq'])
async def liquidate_cmd(ctx):
    discord_id = ctx.message.author.id
    discord_name = ctx.message.author.name
    assets = database_connector.get_all_assets(ctx.message.author.id)
    for asset in assets:
        if asset['name'].lower() == 'usdollar':
            continue
        if asset['is_crypto']:
            price = helpers.get_crypto_price_data(asset['name'])['current_price']
        else:
            price = helpers.get_stock_price_data(asset['name'])['current_price']
        await ctx.send(helpers.transact_asset(discord_id, discord_name, asset['name'],
                                              'max', price, 1, asset['is_crypto']))
    await ctx.send('All assets sold.')


@bot.command(name='portfolio', aliases=['pf'])
async def portfolio_cmd(ctx, *args):
    users = database_connector.get_all_users()
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
        pages = helpers.format_portfolio(helpers.check_balance(user_id))
        for index, page in enumerate(pages):
            await ctx.send("```" + args[0] + '\'s Portfolio (Page {page}):\n'.format(page=str(index + 1)) +
                           page + "```")
    else:
        pages = helpers.format_portfolio(helpers.check_balance(ctx.message.author.id))
        for index, page in enumerate(pages):
            await ctx.send(
                "```" + ctx.message.author.name + '\'s Portfolio (Page {page}):\n'.format(page=str(index + 1)) +
                page + "```")


@bot.command(name='history')
async def history_cmd(ctx):
    await ctx.send("```" + helpers.format_transaction_history(ctx.message.author.id) + "```")


@bot.command(name='leaderboard', aliases=['lb'])
async def leaderboard_cmd(ctx):
    mem_dict = {}
    for m in ctx.message.guild.members:
        mem_dict[m.id] = m.name
    await ctx.send("```" + helpers.format_leaderboard(mem_dict) + "```")


@bot.command(name='alert', aliases=['a'])
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

    database_connector.create_alert(channel_id=channel_id,
                                    asset=code,
                                    is_crypto=is_crypto,
                                    is_less_than=direction,
                                    price=price)

    await ctx.send(helpers.format_alerts(ctx.channel.id))


@bot.command(name='alerts')
async def alerts_cmd(ctx):
    await ctx.send(helpers.format_alerts(ctx.channel.id))


@bot.command(name='xalert')
async def delete_alert_cmd(ctx, alert_id):
    database_connector.delete_alert(alert_id)
    await ctx.send('Alert Deleted.')


@bot.command(name='limit', aliases=['l'])
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

    database_connector.create_limit_order(discord_id=discord_id,
                                          channel_id=channel_id,
                                          asset=code,
                                          volume=amount,
                                          is_crypto=is_crypto,
                                          is_sale=is_sale,
                                          is_less_than=direction,
                                          price=price)

    await ctx.send(helpers.format_limit_orders(discord_id))


@bot.command(name='orders')
async def orders_cmd(ctx):
    await ctx.send(helpers.format_limit_orders(ctx.message.author.id))


@bot.command(name='xorder')
async def delete_order_cmd(ctx, order_id):
    database_connector.delete_limit_order(order_id, ctx.message.author.id)
    await ctx.send('Order Deleted.')


'''BACKGROUND TASKS'''


@tasks.loop(minutes=5)
async def check_alerts():
    alerts = database_connector.get_all_alerts()
    for a in alerts:
        channel = bot.get_channel(int(a.channel_id))
        if channel is None:
            return
        try:
            current_price = helpers.get_crypto_price_data(a.asset_code)['current_price'] if a.is_crypto \
                else helpers.get_stock_price_data(a.asset_code)['current_price']
        except Exception:
            await channel.send('Issue with alert ' + str(a.id) + ' it will be deleted.')
            await channel.send(helpers.format_alerts(a.channel_id))
            database_connector.delete_alert(a.id)
            return
        if a.is_less_than:
            if float(current_price) < float(a.price_per_unit):
                await channel.send('```PRICE ALERT: {asset} below {alert_price}! Current price: {current_price}.```'
                                   .format(asset=a.asset_code.upper(),
                                           alert_price=str(round(a.price_per_unit, 2)),
                                           current_price=str(round(float(current_price), 2))))
                database_connector.delete_alert(a.id)
        else:
            if float(current_price) > float(a.price_per_unit):
                await channel.send('```PRICE ALERT: {asset} above {alert_price}! Current price: {current_price}.```'
                                   .format(asset=a.asset_code.upper(),
                                           alert_price=str(round(a.price_per_unit, 2)),
                                           current_price=str(round(float(current_price), 2))))
                database_connector.delete_alert(a.id)


@tasks.loop(minutes=5)
async def check_limit_orders():
    orders = database_connector.get_limit_orders(None)
    for o in orders:
        display_name = database_connector.get_display_name(o.discord_id)
        channel = bot.get_channel(int(o.channel_id))
        if channel is None:
            return

        try:
            current_price = helpers.get_crypto_price_data(o.asset_code)['current_price'] if o.is_crypto \
                else helpers.get_stock_price_data(o.asset_code)['current_price']
        except Exception:
            await channel.send('Issue with ' + display_name + '\'s order ' + str(o.id) + ' it will be deleted.')
            await channel.send(helpers.format_limit_orders(o.discord_id))
            database_connector.delete_limit_order(o.id, o.discord_id)
            return
        if o.is_less_than:
            if float(current_price) < float(o.price_per_unit):
                await channel.send(helpers.transact_asset(o.discord_id, display_name,
                                                          o.asset_code, o.volume, current_price, o.is_sale,
                                                          o.is_crypto))
                database_connector.delete_limit_order(o.id, o.discord_id)
        else:
            if float(current_price) > float(o.price_per_unit):
                await channel.send(helpers.transact_asset(o.discord_id, display_name,
                                                          o.asset_code, o.volume, current_price, o.is_sale,
                                                          o.is_crypto))
                database_connector.delete_limit_order(o.id, o.discord_id)


'''Execution'''

check_alerts.start()
check_limit_orders.start()
bot.run(TOKEN)
