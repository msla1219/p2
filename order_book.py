from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.sql import text
from sqlalchemy import Table, MetaData
from sqlalchemy.sql import select, and_    

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):

  #1. Insert new order
  order_obj = Order(  sender_pk=order['sender_pk'],
                      receiver_pk=order['receiver_pk'], 
                      buy_currency=order['buy_currency'], 
                      sell_currency=order['sell_currency'], 
                      buy_amount=order['buy_amount'], 
                      sell_amount=order['sell_amount'], 
                      exchange_rate=(order['buy_amount']/order['sell_amount'])
                      )

  session.add(order_obj)
  session.commit()

  # check up if it works well and get the order id
  results = session.execute("select distinct id from orders where " + 
                            " sender_pk = '" + str(order['sender_pk']) + "'" +
                            " and receiver_pk = '" + str(order['receiver_pk']) + "'")

  order_id = results.first()['id']
  # print(" new order: ", order_id, order['buy_currency'], order['sell_currency'], order['buy_amount'], order['sell_amount'])

  #2. Matching order
  results = session.execute("select count(id) " + 
                            " from orders where orders.filled is null " + 
                            " and orders.sell_currency = '" + order['buy_currency'] + "'" +
                            " and orders.buy_currency = '" + order['sell_currency'] + "'" +
                            " and exchange_rate <= " + str(order['sell_amount']/order['buy_amount']))

  if results.first()[0] == 0:
    # print("::::no matching order::::")
    return

  results = session.execute("select distinct id, sender_pk, receiver_pk, buy_currency, sell_currency, buy_amount, sell_amount " + 
                            "from orders where orders.filled is null " + 
                            " and orders.sell_currency = '" + order['buy_currency'] + "'" +
                            " and orders.buy_currency = '" + order['sell_currency'] + "'" +
                            " and exchange_rate <= " + str(order['sell_amount']/order['buy_amount'])) 

  for row in results:
    m_order_id = row['id']
    m_sender_pk = row['sender_pk']
    m_receiver_pk = row['receiver_pk'] 
    m_buy_currency = row['buy_currency'] 
    m_sell_currency = row['sell_currency'] 
    m_buy_amount = row['buy_amount']
    m_sell_amount = row['sell_amount']
    # print(" matched at ID: ", m_order_id)
    break

  # print(" matching order: ", m_order_id, m_buy_currency, m_sell_currency, m_buy_amount, m_sell_amount)
  # print(" order['sell_amount']/order['buy_amount']: ", order['sell_amount']/order['buy_amount'], ">=", "(buy_amount/sell_amount)", (m_buy_amount/m_sell_amount))

  # update both the matching orders 
  stmt = text("UPDATE orders SET counterparty_id=:id, filled=:curr_date WHERE id=:the_id")
  stmt = stmt.bindparams(the_id=order_id, id=m_order_id, curr_date=datetime.now())
  session.execute(stmt)  # where session has already been defined

  stmt = text("UPDATE orders SET counterparty_id=:id, filled=:curr_date WHERE id=:the_id")
  stmt = stmt.bindparams(the_id=m_order_id, id=order_id, curr_date=datetime.now())
  session.execute(stmt)  # where session has already been defined
  
  #3. Create derived order
  if order['buy_amount'] > m_sell_amount:
    order_obj = Order(  sender_pk=order['sender_pk'],
                        receiver_pk=order['receiver_pk'], 
                        buy_currency=order['buy_currency'], 
                        sell_currency=order['sell_currency'], 
                        buy_amount=order['buy_amount'] - m_sell_amount, 
                        sell_amount=order['sell_amount'] - ((order['sell_amount']/order['buy_amount']) * m_sell_amount),
                        exchange_rate = (order['buy_amount'] - m_sell_amount)/(order['sell_amount'] - ((order['sell_amount']/order['buy_amount']) * m_sell_amount)),
                        creator_id=order_id)
    session.add(order_obj)
    session.commit()

  elif order['buy_amount'] < m_sell_amount:
    order_obj = Order(  sender_pk=m_sender_pk,
                        receiver_pk=m_receiver_pk, 
                        buy_currency=m_buy_currency, 
                        sell_currency=m_sell_currency, 
                        buy_amount= m_buy_amount - (m_buy_amount/m_sell_amount) * order['buy_amount'], 
                        sell_amount= m_sell_amount - order['buy_amount'],
                        exchange_rate = (m_buy_amount - (m_buy_amount/m_sell_amount) * order['buy_amount'])/(m_sell_amount - order['buy_amount']),
                        creator_id=m_order_id)
    session.add(order_obj)
    session.commit()
