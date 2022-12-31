#!/usr/bin/env python
"""
- Jasmin Web Panel Submit Logs:
===============================
This script will log all sent sms through Jasmin with user information.

Requirement:
- Activate publish_submit_sm_resp in jasmin.cfg
- Install psycopg2:             # Used for PostgreSQL connection
    +   pip install psycopg2

CREATE TABLE IF NOT EXISTS submit_log  (
    msgid VARCHAR(45) NOT NULL PRIMARY KEY,
    source_connector VARCHAR(15) NULL DEFAULT NULL,
    routed_cid VARCHAR(30) NULL DEFAULT NULL,
    source_addr VARCHAR(40) NULL DEFAULT NULL,
    destination_addr VARCHAR(40) NOT NULL CHECK (destination_addr <> ''),
    rate DECIMAL(12,7) NULL DEFAULT NULL,
    charge DECIMAL(12,7) NULL DEFAULT NULL,
    pdu_count SMALLINT NULL DEFAULT '1',
    short_message BYTEA NULL DEFAULT NULL,
    binary_message BYTEA NULL DEFAULT NULL,
    status VARCHAR(15) NOT NULL CHECK (status <> ''),
    uid VARCHAR(15) NOT NULL CHECK (uid <> ''),
    trials SMALLINT NULL DEFAULT '1',
    created_at TIMESTAMP(0) NOT NULL,
    status_at TIMESTAMP(0) NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX ON submit_log (source_connector);
CREATE INDEX ON submit_log (routed_cid);
CREATE INDEX ON submit_log (source_addr);
CREATE INDEX ON submit_log (destination_addr);
CREATE INDEX ON submit_log (status);
CREATE INDEX ON submit_log (uid);
CREATE INDEX ON submit_log (created_at);
CREATE INDEX ON submit_log (created_at, uid);
CREATE INDEX ON submit_log (created_at, uid, status);
CREATE INDEX ON submit_log (created_at, routed_cid);
CREATE INDEX ON submit_log (created_at, routed_cid, status);
CREATE INDEX ON submit_log (created_at, source_connector);
CREATE INDEX ON submit_log (created_at, source_connector, status);
"""
import os
from time import sleep
from dotenv import load_dotenv

load_dotenv()

import pickle as pickle
import binascii
from datetime import datetime
from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log
from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
import txamqp.spec

from smpp.pdu.pdu_types import DataCoding

from mysql.connector import connect as mysql_connect
from psycopg2 import pool as pg_pool
from psycopg2 import Error as pg_error

q = {}

db_type_mysql = int(os.getenv('DB_TYPE_MYSQL', '1')) == 1

pg_connection_params = {
    'database': os.getenv("DB_DATABASE", default="jasmin_web_db"),
    'user': os.getenv("DB_USER", default="jasmin"),
    'password': os.getenv("DB_PASS", default="jasmin"),
    'port': int(os.getenv("DB_PORT", default="5432")),
    'host': os.getenv("DB_HOST", default="127.0.0.1")
}

my_connection_params = {
    'database': os.getenv("DB_DATABASE", default="jasmin_web_db"),
    'user': os.getenv("DB_USER", default="jasmin"),
    'password': os.getenv("DB_PASS", default="jasmin"),
    'port': int(os.getenv("DB_PORT", default="3306")),
    'host': os.getenv("DB_HOST", default="127.0.0.1")
}

def get_pgsql_conn():
    psql_pool = pg_pool.SimpleConnectionPool(1, 20, **pg_connection_params)
    return psql_pool.getconn()

def get_mysql_conn():
    
    return mysql_connect(
            **my_connection_params,
            pool_name = "mypool",
            pool_size = 20)


@inlineCallbacks
def gotConnection(conn, username, password):
    print("Connected to broker, authenticating: %s" % username, flush=True)
    yield conn.start({"LOGIN": username, "PASSWORD": password})

    print("Authenticated. Ready to receive messages", flush=True)
    chan = yield conn.channel(1)
    yield chan.channel_open()

    yield chan.queue_declare(queue="sms_logger_queue")

    # Bind to submit.sm.* and submit.sm.resp.* routes to track sent messages
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.*')
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='submit.sm.resp.*')
    # Bind to dlr_thrower.* to track DLRs
    yield chan.queue_bind(queue="sms_logger_queue", exchange="messaging", routing_key='dlr_thrower.*')

    yield chan.basic_consume(queue='sms_logger_queue', no_ack=False, consumer_tag="sms_logger")
    queue = yield conn.queue("sms_logger")

    if db_type_mysql:
        db_conn = get_mysql_conn()
        if db_conn:
            print("*** Pooling 20 connections", flush=True)
            print("*** Connected to MySQL", flush=True)    
    else:
        db_conn = get_pgsql_conn()
        if db_conn:
            print("*** Pooling 20 connections", flush=True)
            print("*** Connected to psql", flush=True)
    cursor = db_conn.cursor()

    # Wait for messages
    # This can be done through a callback ...
    while True:
        msg = yield queue.get()
        props = msg.content.properties

        if db_type_mysql:
            db_conn.ping(reconnect=True, attempts=10, delay=1)
        else:
            check_connection = True
            while check_connection:
                try:
                    cursor = db_conn.cursor()
                    cursor.execute('SELECT 1')
                    check_connection = False
                except pg_error:
                    print ('*** PostgreSQL connection exception. Trying to reconnect', flush=True)
                    db_conn = get_pgsql_conn()
                    if db_conn:
                        print ("*** Pooling 20 connections", flush=True)
                        print ("*** Re-connected to psql", flush=True)
                    cursor = db_conn.cursor()

        if msg.routing_key[:10] == 'submit.sm.' and msg.routing_key[:15] != 'submit.sm.resp.':
            pdu = pickle.loads(msg.content.body)
            pdu_count = 1
            short_message = pdu.params['short_message']
            billing = props['headers']
            billing_pickle = billing.get('submit_sm_resp_bill')
            if not billing_pickle:
                billing_pickle = billing.get('submit_sm_bill')
            if billing_pickle is not None:
                submit_sm_bill = pickle.loads(billing_pickle)
            else:
                submit_sm_bill = None
            source_connector = props['headers']['source_connector']
            routed_cid = msg.routing_key[10:]

            # Is it a multipart message ?
            while hasattr(pdu, 'nextPdu'):
                # Remove UDH from first part
                if pdu_count == 1:
                    short_message = short_message[6:]

                pdu = pdu.nextPdu

                # Update values:
                pdu_count += 1
                short_message += pdu.params['short_message'][6:]

            # Save short_message bytes
            binary_message = binascii.hexlify(short_message)

            # If it's a binary message, assume it's utf_16_be encoded
            if pdu.params['data_coding'] is not None:
                dc = pdu.params['data_coding']
                if (isinstance(dc, int) and dc == 8) or (isinstance(dc, DataCoding) and str(dc.schemeData) == 'UCS2'):
                    short_message = short_message.decode('utf_16_be', 'ignore').encode('utf_8')

            q[props['message-id']] = {
                'source_connector': source_connector,
                'routed_cid': routed_cid,
                'rate': 0,
                'charge': 0,
                'uid': 0,
                'destination_addr': pdu.params['destination_addr'],
                'source_addr': pdu.params['source_addr'],
                'pdu_count': pdu_count,
                'short_message': short_message,
                'binary_message': binary_message,
            }
            if submit_sm_bill is not None:
                q[props['message-id']]['rate'] = submit_sm_bill.getTotalAmounts()
                q[props['message-id']]['charge'] = submit_sm_bill.getTotalAmounts() * pdu_count
                q[props['message-id']]['uid'] = submit_sm_bill.user.uid
        elif msg.routing_key[:15] == 'submit.sm.resp.':
            # It's a submit_sm_resp

            pdu = pickle.loads(msg.content.body)
            if props['message-id'] not in q:
                print('*** Got resp of an unknown submit_sm: %s' % props['message-id'], flush=True)
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            qmsg = q[props['message-id']]

            if qmsg['source_addr'] is None:
                qmsg['source_addr'] = ''
            """
            # ON DUPLICATE KEY UPDATE trials = trials + 1;
            # http://stackoverflow.com/a/34639631/4418

            # ON CONFLICT (trials) DO UPDATE SET trials = submit_log.trials + 1;
            """
            if db_type_mysql:
                sql_conflict = "ON DUPLICATE KEY UPDATE trials = trials + 1";
            else:
                sql_conflict = "ON CONFLICT (trials) DO UPDATE SET trials = submit_log.trials + 1"
            pdu_status = str(pdu.status).replace("CommandStatus.", "")
            cursor.execute("""INSERT INTO submit_log (msgid, source_addr, rate, pdu_count, charge,
                                                      destination_addr, short_message,
                                                      status, uid, created_at, binary_message,
                                                      routed_cid, source_connector, status_at, trials)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                    %s;
                    """, (
                props['message-id'],
                qmsg['source_addr'],
                qmsg['rate'],
                qmsg['pdu_count'],
                qmsg['charge'],
                qmsg['destination_addr'],
                qmsg['short_message'],
                pdu_status,
                qmsg['uid'],
                props['headers']['created_at'],
                qmsg['binary_message'],
                qmsg['routed_cid'],
                qmsg['source_connector'],
                props['headers']['created_at'],
                sql_conflict,))
            db_conn.commit()
        elif msg.routing_key[:12] == 'dlr_thrower.':
            if props['headers']['message_status'][:5] == 'ESME_':
                # Ignore dlr from submit_sm_resp
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            # It's a dlr
            if props['message-id'] not in q:
                print('*** Got dlr of an unknown submit_sm: %s' % props['message-id'], flush=True)
                chan.basic_ack(delivery_tag=msg.delivery_tag)
                continue

            # Update message status
            qmsg = q[props['message-id']]
            message_status = str(props['headers']['message_status']).replace("CommandStatus.", "")
            update_log = ("UPDATE submit_log SET status = %s, status_at = %s WHERE msgid = %s;")
            cursor.execute(update_log, (
                message_status,
                datetime.now(),
                props['message-id'],))
            db_conn.commit()
        else:
            print('*** unknown route: %s' % msg.routing_key, flush=True)

        chan.basic_ack(delivery_tag=msg.delivery_tag)

    # A clean way to tear down and stop
    yield chan.basic_cancel("sms_logger")
    yield chan.channel_close()
    chan0 = yield conn.channel(0)
    yield chan0.connection_close()

    reactor.stop()


if __name__ == "__main__":
    sleep(2)
    print(' ', flush=True)
    print(' ', flush=True)
    print('***************** sms_logger *****************', flush=True)
    if db_type_mysql == 1:
        print('*** Staring sms_logger, DB drive: MySQL', flush=True)
    else:
        print('*** Staring sms_logger, DB drive: PostgreSQL', flush=True)
    print('**********************************************', flush=True)

    host = os.getenv("AMQP_BROKER_HOST", default='127.0.0.1')
    port = int(os.getenv("AMQP_BROKER_PORT", default=5672))
    vhost = os.getenv("AMQP_BROKER_VHOST", default='/')
    username = os.getenv("AMQP_BROKER_USERNAME", default='guest')
    password = os.getenv("AMQP_BROKER_PASSWORD", default='guest')
    spec_file = os.getenv("AMQP_BROKER_SPEC_FILE", default='/etc/jasmin/resource/amqp0-9-1.xml')

    spec = txamqp.spec.load(spec_file)

    # Connect and authenticate
    d = ClientCreator(reactor,
        AMQClient,
        delegate=TwistedDelegate(),
        vhost=vhost,
        spec=spec).connectTCP(host, port)
    d.addCallback(gotConnection, username, password)

    def whoops(err):
        if reactor.running:
            log.err(err)
            reactor.stop()

    d.addErrback(whoops)

    reactor.run()

