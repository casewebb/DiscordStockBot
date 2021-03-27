from sqlalchemy import create_engine, Table, Column, Integer, Float, String, DateTime, MetaData, ForeignKey, select, \
    and_, update, distinct, delete, types, desc, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

meta = MetaData()
engine = create_engine("mysql://root:admin@localhost/discord_stock_bot", pool_recycle=3600, pool_pre_ping=True)
Session = sessionmaker(bind=engine, autocommit=True)
session = Session()

user = Table(
    'user', meta,
    Column('discord_id', String(17), primary_key=True),
    Column('display_name', String(30)),
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

alert = Table(
    'alert', meta,
    Column('id', Integer, primary_key=True),
    Column('channel_id', String(20)),
    Column('asset_code', String(10)),
    Column('price_per_unit', Float),
    Column('is_crypto', Integer),
    Column('is_less_than', Integer),
)

limit_transaction = Table(
    'limit_transaction', meta,
    Column('id', Integer, primary_key=True),
    Column('discord_id', String(17), ForeignKey('user.discord_id')),
    Column('channel_id', String(20)),
    Column('asset_code', String(10)),
    Column('volume', String(20)),
    Column('price_per_unit', Float),
    Column('is_sale', Integer),
    Column('is_crypto', Integer),
    Column('is_less_than', Integer),
    Column('transaction_date', DateTime, server_default=func.now()),
)


def create_database():
    meta.create_all(engine)


def wake_up_db():
    global session
    session.close()
    session = Session()


def initialize_new_user(discord_id):
    ins = user.insert().values(discord_id=discord_id)
    session.execute(ins)

    init_insert = transaction.insert().values(discord_id=discord_id,
                                              asset_code='USDOLLAR',
                                              volume=50000,
                                              price_per_unit=1,
                                              is_sale=0,
                                              is_crypto=0)

    try:
        session.execute(init_insert)
        session.flush()
    except Exception:
        session.rollback()


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

    available_bal = get_asset_units(discord_id, 'USDOLLAR')[0]
    if is_sale == 0:
        purchase_req_price = price_per_unit * volume
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
                    'transaction_cost': purchase_req_price,
                    'available_funds': available_bal}
    else:
        available_units = get_asset_units(discord_id, asset)[0]
        if available_units >= volume:
            new_bal = available_bal + (price_per_unit * volume)
            bal_upd = (
                update(transaction).where(
                    and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == 'USDOLLAR')).values(
                    volume=new_bal)
            )
        else:
            return {'is_successful': False,
                    'message': 'Insufficient Shares',
                    'available_funds': available_units}

    try:
        session.execute(t_ins)
        session.execute(bal_upd)
        session.flush()
        return {'is_successful': True, 'message': 'Successful',
                'available_funds': new_bal}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}


def get_asset_units(discord_id, asset):
    transactions = session.execute(select([transaction]).where(
        and_(transaction.c.discord_id == discord_id, transaction.c.asset_code == asset)
    ).order_by(asc(transaction.c.transaction_date))).fetchall()

    vol_total = 0
    average_price = 0
    for t in transactions:
        if t.is_sale == 1:
            vol_total -= t.volume
            if float(vol_total) == 0:
                average_price = 0
        else:
            if vol_total + t.volume > 0:
                average_price = (((average_price * vol_total) + (t.price_per_unit * t.volume)) / (vol_total + t.volume))
            vol_total += t.volume
    return vol_total, average_price


def get_all_assets(discord_id):
    assets = session.execute(select([distinct(transaction.c.asset_code), transaction.c.is_crypto]).where(
        (transaction.c.discord_id == discord_id)
    )).fetchall()

    asset_vol_list = []
    for asset in assets:
        info = get_asset_units(discord_id, asset.asset_code)
        total = info[0]
        avg_cost = info[1]
        if total > 0:
            asset_vol_list.append({'name': asset.asset_code,
                                   'shares': total,
                                   'current_value': 0,
                                   'avg_price': avg_cost,
                                   'is_crypto': asset.is_crypto})
    return asset_vol_list


def get_transaction_history(discord_id):
    transactions = session.execute(select([transaction]).where(
        transaction.c.discord_id == discord_id
    ).order_by(desc(transaction.c.transaction_date)).limit(10)).fetchall()

    if len(transactions) == 0:
        initialize_new_user(discord_id)
        transactions = session.execute(select([transaction]).where(
            transaction.c.discord_id == discord_id
        ).order_by(desc(transaction.c.transaction_date)).limit(10)).fetchall()
    return transactions


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
        session.flush()
        return {'is_successful': True, 'message': 'Successfully reset balance.',
                'available_funds': '50000'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Error resetting account'}


def get_all_users():
    user_ids = []
    user_names = []
    users = session.execute(select([user])).fetchall()
    for u in users:
        user_ids.append(u.discord_id)
        user_names.append(u.display_name)
    return user_ids, user_names


def set_display_name(discord_id, name):
    name_upd = (update(user).where(user.c.discord_id == discord_id).values(display_name=name))
    try:
        session.execute(name_upd)
        session.flush()
    except Exception as e:
        print(e)
        session.rollback()


def get_display_name(discord_id):
    x = session.execute(select([user]).where(user.c.discord_id == discord_id)).fetchall()
    return x[0].display_name


# ALERTS

def create_alert(channel_id, asset, is_crypto, is_less_than, price):
    a_ins = alert.insert().values(channel_id=channel_id,
                                  asset_code=asset,
                                  is_crypto=is_crypto,
                                  price_per_unit=price,
                                  is_less_than=is_less_than)

    try:
        session.execute(a_ins)
        session.flush()
        return {'is_successful': True, 'message': 'Successful'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}


def get_all_alerts():
    return session.execute(select([alert])).fetchall()


def delete_alert(alert_id):
    delete_alert_stmt = (
        delete(alert).where(alert.c.id == alert_id)
    )

    try:
        session.execute(delete_alert_stmt)
        session.flush()
        return {'is_successful': True, 'message': 'Success'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}


def create_limit_order(discord_id, channel_id, asset, volume, is_crypto, is_sale, is_less_than, price):
    limit_ins = limit_transaction.insert().values(channel_id=channel_id,
                                                  discord_id=discord_id,
                                                  asset_code=asset,
                                                  is_crypto=is_crypto,
                                                  is_sale=is_sale,
                                                  price_per_unit=price,
                                                  volume=volume,
                                                  is_less_than=is_less_than)

    try:
        session.execute(limit_ins)
        session.flush()
        return {'is_successful': True, 'message': 'Successful'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}


def get_limit_orders(discord_id):
    if discord_id is None:
        return session.execute(select([limit_transaction])).fetchall()
    else:
        return session.execute(select([limit_transaction]).where(
            limit_transaction.c.discord_id == discord_id
        )).fetchall()


def delete_limit_order(order_id, discord_id):
    delete_limit_stmt = (
        delete(limit_transaction).where(and_(limit_transaction.c.id == order_id,
                                             limit_transaction.c.discord_id == discord_id))
    )

    try:
        session.execute(delete_limit_stmt)
        session.flush()
        return {'is_successful': True, 'message': 'Success'}
    except Exception as e:
        print(e)
        session.rollback()
        return {'is_successful': False, 'message': 'Database Error'}
