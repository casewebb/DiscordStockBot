from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, ForeignKey, select, \
    and_, update, distinct
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

meta = MetaData()
engine = create_engine("mysql://root:admin@localhost/discord_stock_bot")
Session = sessionmaker(bind=engine)
session = Session()

user = Table(
    'user', meta,
    Column('discord_id', String(17), primary_key=True),
    Column('start_date', DateTime, server_default=func.now()),
)

transaction = Table(
    'transaction', meta,
    Column('id', Integer, primary_key=True),
    Column('discord_id', String(17), ForeignKey('user.discord_id')),
    Column('asset_code', String(10)),
    Column('volume', Float),
    Column('price_per_unit', Float),
    Column('is_sale', Integer),
    Column('transaction_date', DateTime, server_default=func.now()),
)


def create_database():
    meta.create_all(engine)


def initialize_new_user(discord_id):
    ins = user.insert().values(discord_id=discord_id)
    session.execute(ins)

    init_insert = transaction.insert().values(discord_id=discord_id,
                                              asset_code='USD',
                                              volume=50000,
                                              price_per_unit=1,
                                              is_sale=0)
    session.execute(init_insert)


def make_transaction(discord_id, asset, volume, price_per_unit, is_sale):
    users = session.execute(select([user]).where(
        user.c.discord_id == discord_id
    )).rowcount

    t_ins = transaction.insert().values(discord_id=discord_id,
                                        asset_code=asset,
                                        volume=volume,
                                        price_per_unit=price_per_unit,
                                        is_sale=is_sale)

    if users == 0:
        initialize_new_user(discord_id)

    available_bal = get_available_usd_balance(discord_id)
    if is_sale == 0:
        purchase_req_price = price_per_unit * volume
        new_bal = available_bal - purchase_req_price
        if available_bal >= purchase_req_price:
            bal_upd = (
                update(transaction).where(
                    and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USD')).values(
                    volume=new_bal)
            )
        else:
            print('-----------------------Insuff Funds')
            return {'is_successful': False, 'message': 'Insufficient Funds',
                    'transaction_cost': str(purchase_req_price),
                    'available_funds': str(available_bal)}
    else:
        available_units = get_asset_units(discord_id, asset)
        if available_units >= volume:
            new_bal = available_bal + (price_per_unit * volume)
            bal_upd = (
                update(transaction).where(
                    and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USD')).values(
                    volume=new_bal)
            )
        else:
            print('-----------------------Insuff Shares')
            return {'is_successful': False, 'message': 'Insufficient Shares',
                    'available_funds': str(available_units)}

    try:
        session.execute(t_ins)
        session.execute(bal_upd)
        session.commit()
        session.flush()
        print('-----------------------SUCCESS' + str(new_bal))
        return {'is_successful': True, 'message': 'Successful',
                'available_funds': str(new_bal)}
    except:
        session.rollback()
        print('-----------------------ROLLBACK')
        return {'is_successful': False, 'message': 'Database Error'}


def get_available_usd_balance(discord_id):
    balance = session.execute(select([transaction]).where(
        and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USD')
    )).fetchone()

    return balance.volume


def get_asset_units(discord_id, asset):
    transactions = session.execute(select([transaction]).where(
        and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == asset)
    )).fetchall()

    vol_total = 0
    for t in transactions:
        if t.is_sale == 1:
            vol_total -= t.volume
        else:
            vol_total += t.volume

    return vol_total


def get_all_assets(discord_id):
    assets = session.execute(select([distinct(transaction.asset_code)]).where(
        (transaction.c.discord_id == discord_id)
    )).fetchall()

    for asset in assets:
        transactions = session.execute(select([transaction]).where(
            and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == asset)
        )).fetchall()

        total_vol = 0
        for t in transactions:
            if t.is_sale == 1:
                total_vol -= t.volume
            else:
                total_vol += t.volume


# make_transaction('CASE', 'GME', 10, 2000, 1)
# make_transaction('CASE', 'BTC', 10, 5000, 1)
# make_transaction('34284', 'bb', 50, 15.15, 0)
