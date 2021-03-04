from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, ForeignKey, select, \
    and_, update, distinct, delete, types
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
    Column('volume', types.Float(precision=30)),
    Column('price_per_unit', Float),
    Column('is_sale', Integer),
    Column('is_crypto', Integer),
    Column('transaction_date', DateTime, server_default=func.now()),
)


def create_database():
    meta.create_all(engine)


def initialize_new_user(discord_id):
    ins = user.insert().values(discord_id=discord_id)
    session.execute(ins)

    init_insert = transaction.insert().values(discord_id=discord_id,
                                              asset_code='USDOLLAR',
                                              volume=50000,
                                              price_per_unit=1,
                                              is_sale=0,
                                              is_crypto=0)
    session.execute(init_insert)
    session.commit()
    session.flush()


def make_transaction(discord_id, asset, volume, price_per_unit, is_sale, is_crypto):
    users = session.execute(select([user]).where(
        user.c.discord_id == discord_id
    )).rowcount

    t_ins = transaction.insert().values(discord_id=discord_id,
                                        asset_code=asset,
                                        volume=volume,
                                        price_per_unit=price_per_unit,
                                        is_sale=is_sale,
                                        is_crypto=is_crypto)
    if users == 0:
        initialize_new_user(discord_id)

    available_bal = get_available_usd_balance(discord_id)
    if is_sale == 0:
        purchase_req_price = float(price_per_unit) * float(volume)
        new_bal = available_bal - purchase_req_price
        if available_bal >= purchase_req_price:
            bal_upd = (
                update(transaction).where(
                    and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USDOLLAR')).values(
                    volume=new_bal)
            )
        else:
            return {'is_successful': False,
                    'message': 'Insufficient Funds',
                    'transaction_cost': str(purchase_req_price),
                    'available_funds': str(available_bal)}
    else:
        available_units = get_asset_units(discord_id, asset)
        if float(available_units) >= float(volume):
            new_bal = available_bal + (float(price_per_unit) * float(volume))
            bal_upd = (
                update(transaction).where(
                    and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USDOLLAR')).values(
                    volume=new_bal)
            )
        else:
            return {'is_successful': False,
                    'message': 'Insufficient Shares',
                    'available_funds': str(available_units)}

    try:
        session.execute(t_ins)
        session.execute(bal_upd)
        session.commit()
        session.flush()
        return {'is_successful': True, 'message': 'Successful',
                'available_funds': str(new_bal)}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}


def get_available_usd_balance(discord_id):
    balance = session.execute(select([transaction]).where(
        and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USDOLLAR')
    )).fetchone()

    return balance.volume


def get_asset_units(discord_id, asset):
    transactions = session.execute(select([transaction]).where(
        and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == asset)
    )).fetchall()

    if len(transactions) == 0:
        initialize_new_user(discord_id)
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
    assets = session.execute(select([distinct(transaction.c.asset_code), transaction.c.is_crypto]).where(
        (transaction.c.discord_id == discord_id)
    )).fetchall()

    asset_vol_dict = {}
    rolling_index = 0
    for index, asset in enumerate(assets):
        total = get_asset_units(discord_id, asset.asset_code)
        if total > 0:
            asset_vol_dict[rolling_index] = {'name': asset.asset_code,
                                             'shares': total,
                                             'current_value': 0,
                                             'is_crypto': asset.is_crypto}
            rolling_index += 1
    return asset_vol_dict


def reset(discord_id):
    bal_upd = (
        update(transaction).where(
            and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USDOLLAR')).values(
            volume=50000)
    )

    delete_all_transactions = (
        delete(transaction).where(and_(transaction.c.discord_id == discord_id, transaction.c.asset_code != 'USDOLLAR'))
    )

    try:
        session.execute(bal_upd)
        session.execute(delete_all_transactions)
        session.commit()
        session.flush()
        return {'is_successful': True, 'message': 'Successfully reset balance.',
                'available_funds': '50000'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Error resetting account'}


def get_all_users():
    user_ids = []
    users = session.execute(select([user])).fetchall()
    for u in users:
        user_ids.append(u.discord_id)
    return user_ids


create_database()
