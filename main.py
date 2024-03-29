# -*- coding: utf-8 -*-
import csv
import math
import os.path
import codecs
from tronapi.tronapi import Tronapi
import time
from tronapi import keys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from kafka3 import KafkaProducer
import binascii
import pandas as pd
import json
import datetime

decode_hex = codecs.getdecoder("hex_codec")

TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

engine = create_engine('mysql+mysqldb://root:fat-chain-root-password@my-sql:3306/TronBlock')
#engine = create_engine('mysql+mysqldb://root:csquan253905@127.0.0.1:3306/TronBlock')
Session = sessionmaker(bind=engine)
session = Session()

monitor_engine = create_engine('mysql+mysqldb://root:fat-chain-root-password@my-sql:3306/Hui_Collect', pool_size=0, max_overflow=-1)
#monitor_engine = create_engine('mysql+mysqldb://root:csquan253905@127.0.0.1:3306/HuiCollect', pool_size=0, max_overflow=-1)
monitor_Session = sessionmaker(bind=monitor_engine)
monitor_session = monitor_Session()

Base = declarative_base()

kafka_server = "172.31.46.139:9092"
tx_topic = "tx-topic"
matched_topic = "match-topic"

class tasks(Base):
    __tablename__ = 'f_task'

    id = Column(Integer, primary_key=True)

    num = Column(Integer, nullable=False, index=True)
    name = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.name)


class Transaction(Base):
    __tablename__ = 'f_tx'

    t_id = Column(Integer, primary_key=True)

    t_hash = Column(String(64), nullable=False, index=True)
    t_block = Column(Integer, nullable=False, index=False)
    t_fromAddr = Column(String(64), nullable=False, index=False)
    t_toAddr = Column(String(64), nullable=False, index=False)
    t_block_at = Column(String(64), nullable=False, index=False)
    t_amount = Column(String(64), nullable=False, index=False)
    t_is_contract = Column(String(64), nullable=False, index=False)
    t_contract_addr = Column(String(64), nullable=False, index=False)
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.t_hash)


class TRC20Transaction(Base):
    __tablename__ = 'f_trc20_tx'

    t_id = Column(Integer, primary_key=True)

    t_hash = Column(String(64), nullable=False, index=True)
    t_block = Column(Integer, nullable=False, index=False)
    t_fromAddr = Column(String(64), nullable=False, index=False)
    t_toAddr = Column(String(64), nullable=False, index=False)
    t_block_at = Column(String(64), nullable=False, index=False)
    t_amount = Column(String(64), nullable=False, index=False)
    t_contract_addr = Column(String(64), nullable=False, index=False)
    t_status = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.t_hash)

class Contract(Base):
    __tablename__ = 'f_contract'

    id = Column(Integer, primary_key=True)

    t_contract_addr = Column(String(64), nullable=False, index=False)
    t_name = Column(String(64), nullable=False, index=False)
    t_symbol = Column(String(64), nullable=False, index=True)
    t_decimal = Column(String(64), nullable=False, index=False)
    t_total_supply = Column(String(64), nullable=False, index=False)
    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.t_contract_addr)

def Init():
    DirectoryArray = ['config', 'data']
    for directory in DirectoryArray:
        path = os.getcwd() + "/" + directory
        if not os.path.exists(path):
            os.makedirs(path)
        pass
    # if not os.path.exists(os.getcwd() + "/config/template_block.csv"):
    #     with open(os.getcwd() + "/config/template_block.csv", "w", newline='') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["id","total","active","created_at","updated_at","started_at","ended_at","block_at","error"])
    if not os.path.exists(os.getcwd() + "/config/wallet.csv"):
        with open(os.getcwd() + "/config/wallet.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["address", "private_key"])
            writer.writerow(["TSRg164MqUKMxDn2eQYvAg9iFNhQYXAFa8", ""])
    if not os.path.exists(os.getcwd() + "/config/contract.csv"):
        with open(os.getcwd() + "/config/contract.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["address", "name", 'decimals'])
            writer.writerow(["TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", "USDT", 6])
            writer.writerow(["TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8", "USDC", 6])
            writer.writerow(["TAFjULxiVgT4qWk6UZwjqwZXTSaGaqnVp4", "BitTorrent", 18])

        pass
    if not os.path.exists(os.getcwd() + "/config/transaction.csv"):
        with open(os.getcwd() + "/config/transaction.csv", "w", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["hash", "block", "from", "to", "block_at", "amount", "contract_address", 'status'])
        pass


def GetWalletArray():
    contract_array = []
    with open(os.getcwd() + "/config/wallet.csv") as f:
        reader = csv.reader(f, delimiter=',', quotechar='|')
        for row in reader:
            if row[0] == 'address': continue
            contract_array.append(row)
    return contract_array


def GetContactArray():
    contract_array = []
    with open(os.getcwd() + "/config/contract.csv") as f:
        reader = csv.reader(f, delimiter=',', quotechar='|')
        for row in reader:
            if row[0] == 'address': continue
            contract_array.append(row)
    return contract_array


def on_send_success(record_metadata=None):
    print(record_metadata.topic)

def on_send_error(excp=None):
    print('I am an errback', exc_info=excp)


# TRC20/TRC发送kafka逻辑（充值）： 状态hash表：monitor_hash 监控表：monitor
# 1 从监控表中取tx20.toAdd地址对应的UID ，如果能取到，则进入下一阶段
# 2 从状态hash表中取出当前的交易hash，如果没找到，则进入下一阶段
# 3 db中取出token的精度（addtoken添加进db）
# 4 组装消息发送
def KafkaTxLogic(tx,contract_obj, block_num, monitor_dict):
    txKafka = {}
    if monitor_dict is None:
        return
    if tx.t_toAddr not in monitor_dict:
        return
    else:  # UID存在
        print("找到UID")
        print(monitor_dict[tx.t_toAddr].f_uid)

        for value in monitor_dict[tx.t_toAddr].f_uid:
            txKafka["uid"] = value

        if tx.t_contract_addr == "":
            txKafka["amount"] = str(tx.t_amount)
            txKafka["token_type"] = 4  #trx 本币
        else:
            txKafka["token_type"] = 5  #trx代币
            txKafka["amount"] = str(tx.t_amount * (10 ** int(contract_obj.t_decimal)))

        query_sql = 'select * from t_monitor_hash where f_hash = "' + tx.t_hash + '"'
        df_hash = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

        if df_hash.empty is True:  # 在状态hash中没找到
            txKafka["from"] = tx.t_fromAddr
            txKafka["to"] = tx.t_toAddr
            txKafka["tx_hash"] = tx.t_hash
            txKafka["chain"] = "trx"
            txKafka["contract_addr"] = tx.t_contract_addr
            txKafka["decimals"] = int(contract_obj.t_decimal)
            txKafka["asset_symbol"] = contract_obj.t_symbol
            txKafka["tx_height"] = block_num
            txKafka["cur_chain_height"] = block_num + 19
            txKafka["log_index"] = 0

            aa_str = json.dumps(txKafka,default=lambda o: o.__dict__,sort_keys=True, indent=4)

            bootstrap_servers = [kafka_server]

            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

            bb = bytes(aa_str, 'utf-8')

            producer.send(
                topic=tx_topic,
                value=bb).add_callback(on_send_success).add_errback(on_send_error)




# TRC20/TRC发送kafka逻辑（充值）： 状态hash表：monitor_hash 监控表：monitor
# 1 从监控表中取tx20.toAdd地址对应的UID ，如果能取到，则进入下一阶段
# 2 从状态hash表中取出当前的交易hash，如果没找到，则进入下一阶段
# 3 db中取出token的精度（addtoken添加进db）
# 4 组装消息发送
def KafkaMatchTxLogic(tx,transaction,block_num,monitor_hash_dict,logData):
    txpush = {}
    if monitor_hash_dict is None:
        return
    if tx.t_hash not in monitor_hash_dict:
        return
    else:
        print("在状态hash表中匹配到" + tx.t_hash)
        txpush["hash"] = tx.t_hash
        txpush["chain"] = "trx"
        txpush["tx_height"] = block_num
        txpush["cur_chain_height"] = block_num + 19

        query_sql = 'select f_order_id from t_monitor_hash where f_hash = "' + tx.t_hash +'"'
        df = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

        if df.empty is True:
           print("在db中没有找到记录")
        else:
           txpush["order_id"] = df.f_order_id[0]

        txpush["contract_addr"] = tx.t_contract_addr

        if transaction["ret"][0]["contractRet"] != "SUCCESS":
            txpush["success"] = False
        else:
            txpush["success"] = True

        txpush["gas_used"] = ParseLog(logData, tx.t_hash)

        print(txpush)

        aa_str = json.dumps(txpush,default=lambda o: o.__dict__,sort_keys=True, indent=4)

        bootstrap_servers = [kafka_server]

        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

        bb = bytes(aa_str, 'utf-8')
        print(bb)
        ret = producer.send(
            topic=matched_topic,
            value=bb).add_callback(on_send_success).add_errback(on_send_error)
        print("match kafka 执行结果")
        print(ret)


def ParseLog(log_data, hash):
    for obj in enumerate(log_data):
        logs = obj[1]
        if hash == logs["id"]:
            if "fee" in logs:
               return logs["fee"]

    return 0




def GetMonitor():
    query_sql = 'select * from t_monitor'
    df= pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

    if df.empty is True:
        return
    else:  # UID存在
        by_addr_dict = dict(tuple(df.groupby('f_addr')))
        return by_addr_dict

def GetMonitorHash():
    query_sql = 'select * from t_monitor_hash'
    df= pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

    if df.empty is True:
        return
    else:  # UID存在
        by_hash_dict = dict(tuple(df.groupby('f_hash')))
        return by_hash_dict
def hexStr_to_str(hex_str):
    hex = hex_str.decode('utf-8')
    str_bin = binascii.unhexlify(hex)
    return str_bin.decode('utf-8')

def parseTxAndStoreTrc(block_num, delay, monitor_dict,monitor_hash_dict):
    time.sleep(delay)
    count20 =0
    count=0
    tron_api = Tronapi()
    try:
        transactionsData = tron_api.getWalletsolidityBlockByNum(block_num)
    except Exception as e:
        print(e)
        print("可能接口请求过于频繁,因此重新请求")
        return parseTxAndStoreTrc(block_num, 5,monitor_dict,monitor_hash_dict)
    try:
        transaction_at = (transactionsData['block_header']['raw_data']['timestamp']) / 1000
        transaction_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(transaction_at))
    except Exception as e:
        print(e)
        return
    if "transactions" in transactionsData:
        ContactArray = GetContactArray()
        # 这里是TRC和TRC10交易，以48896576为例，158笔 和浏览器对应
        tx_list = []
        tx20_list = []
        contract_list = []
        contract_hex_list = []  # 为了查找方便，设置一个额外的数据结构
        logData = tron_api.getTxInfoByNum(block_num)
        for transaction in transactionsData['transactions']:
            if 'contract_address' in transaction['raw_data']['contract'][0]['parameter']['value']: # 合约交易
                count20 = count20 + 1
                try:
                    contract_in_hex = transaction['raw_data']['contract'][0]['parameter']['value']['contract_address']
                    contract = keys.to_base58check_address(contract_in_hex)
                    active_contract = []
                    for temp in ContactArray:
                        if temp[0] == contract:
                            active_contract = temp
                    if len(active_contract) == 0:
                        continue
                except Exception as e:
                    print('错误:', e)
                    continue
                if "data" not in transaction['raw_data']["contract"][0]["parameter"]["value"]:
                    continue
                func = transaction['raw_data']["contract"][0]["parameter"]["value"]["data"][0:8]
                if func != "a9059cbb":
                    continue
                try:
                    transactionAmount = int(
                        transaction['raw_data']["contract"][0]["parameter"]["value"]["data"][72:].lstrip("0"), 16) / (
                                                1 * math.pow(10, int(active_contract[2])))
                except:
                    continue
                from_address = keys.to_base58check_address(
                    transaction['raw_data']['contract'][0]['parameter']['value']['owner_address'])
                to_str = transaction['raw_data']["contract"][0]["parameter"]["value"]["data"][9:72].lstrip("0")
                if len(to_str) == 0:
                    continue
                to_address = keys.to_base58check_address(
                    transaction['raw_data']["contract"][0]["parameter"]["value"]["data"][9:72].lstrip("0"))
                # write to mysql
                tx20 = TRC20Transaction(
                    t_hash=transaction['txID'],
                    t_block=block_num,
                    t_fromAddr=from_address,
                    t_toAddr=to_address,
                    t_block_at=transaction_at,
                    t_amount=transactionAmount,
                    t_contract_addr=contract,
                    t_status="success",
                )
                tx20_list.append(tx20)

                should_call_api=True
                # 1.先在当前的contract_list中查找 2.然后在db中查找 3.如果都找不到，再去远程接口取
                if contract_in_hex in contract_hex_list:
                    index = contract_hex_list.index(contract_in_hex)
                    contract_obj = contract_list[index]
                    should_call_api = False
                else:
                    # 这里应该从db中读取TRC合约信息
                    contract_obj = session.query(Contract).filter(Contract.t_contract_addr == contract).first()
                    if contract_obj is not None:
                        should_call_api = False

                if should_call_api is True:
                    print("未在缓存和db中找到，开始取线上取")
                    res = tron_api.getContractInfo("name()", contract_in_hex)
                    if res['result']['result'] is True:
                        print(res['constant_result'][0])
                        print("len")
                        print(res['constant_result'][0][64:128].lstrip('0'))
                        length_str = res['constant_result'][0][64:128].lstrip('0')
                        print(length_str)
                        length = int(str(length_str), 16)
                        print(length)
                        print("name")
                        print(res['constant_result'][0][128:128 + length * 2])
                        name = bytes.fromhex(res['constant_result'][0][128:128 + length * 2]).decode()
                        print("name" + name)
                    res = tron_api.getContractInfo("symbol()", contract_in_hex)
                    if res['result']['result'] is True:
                        symbol = bytes.fromhex(res['constant_result'][0][128:192].rstrip('0')).decode()
                        print("symbol:" + symbol)
                    res = tron_api.getContractInfo("decimals()", contract_in_hex)
                    if res['result']['result'] is True:
                        decimals = res['constant_result'][0].lstrip('0')
                        print("decimal:" + decimals)
                    res = tron_api.getContractInfo("totalSupply()", contract_in_hex)
                    if res['result']['result'] is True:
                        totalSupply = int(res['constant_result'][0].lstrip('0'), 16)
                        print("totalSupply:" + str(totalSupply))

                    contract_obj = Contract(
                        t_contract_addr=contract,
                        t_name=name,
                        t_symbol=symbol,
                        t_decimal=decimals,
                        t_total_supply=str(totalSupply)
                    )

                contract_hex_list.append(contract_in_hex)
                contract_list.append(contract_obj)

                KafkaMatchTxLogic(tx20, transaction, block_num, monitor_hash_dict, logData)  # 状态hash匹配
                KafkaTxLogic(tx20, contract_obj, block_num, monitor_dict)  # 充值交易
            else:  # 非TRC20交易(包含TRC交易和10交易，应该区分)
                tx_detail = transaction['raw_data']['contract'][0]['parameter']['value']
                count = count + 1
                if "amount" in tx_detail:
                    transactionAmount = tx_detail['amount']

                    toAddr = keys.to_base58check_address(tx_detail["to_address"])
                    fromAddr = keys.to_base58check_address(tx_detail["owner_address"])

                    # write to mysql
                    tx = Transaction(
                        t_hash=transaction['txID'],
                        t_block=block_num,
                        t_fromAddr=fromAddr,
                        t_toAddr=toAddr,
                        t_block_at=transaction_at,
                        t_amount=transactionAmount,
                        t_contract_addr="",
                        t_is_contract="False"
                    )
                    if "asset_name" in tx_detail:
                        print("检测到TRC10交易，舍弃")
                        continue

                    tx_list.append(tx)
                    contract_obj = Contract()  # 本币
                    contract_obj.t_decimal = "6"
                    contract_obj.t_symbol = "trx"
                    contract_obj.t_name = "trx"

                    KafkaMatchTxLogic(tx, transaction, block_num, monitor_hash_dict, logData)  # 状态hash匹配
                    KafkaTxLogic(tx, contract_obj, block_num, monitor_dict)  # 充值交易
                else:  # resource = "energy"
                    continue
        session.add_all(tx_list)
        session.add_all(tx20_list)
        session.add_all(contract_list)

    # 更新task当前高度
    new_height = block_num + 1
    update_sql = 'update f_task set num = "' + str(new_height) + '" where name = "TRC"'
    session.execute(text(update_sql))
    session.commit()


Base.metadata.create_all(engine, checkfirst=True)

Init()

tronapi = Tronapi()

while True:
    monitor_dict=GetMonitor()
    monitor_hash_dict = GetMonitorHash()

    try:  # 目前从rpc获取来的是不可逆区块，区块比最新高度低19个区块
        GetNowBlock = tronapi.getConfirmedCurrentBlock()
    except Exception as e:
        # 过于频繁的请求波场接口可能会强制限制一段时间,此时sleep一下
        time.sleep(5)
        continue
    if GetNowBlock is None:
        continue
    # 这里应该从db中读取TRC20的任务高度
    sql = r'select * from f_task where name="TRC20"'
    # 读取SQL数据库
    df = pd.read_sql_query(sql=text(sql), con=engine.connect())  # 读取SQL数据库，并获得pandas数据帧。
    now_block_num = int(GetNowBlock.get('block_header').get('raw_data').get('number'))
    handle_block_count = 0

    # 这里应该从db中读取TRC任务高度
    sql = r'select * from f_task where name="TRC"'
    # 读取SQL数据库
    df = pd.read_sql_query(sql=text(sql), con=engine.connect())  # 读取SQL数据库，并获得pandas数据帧。
    now_block_num = int(GetNowBlock.get('block_header').get('raw_data').get('number'))
    min_block_num = now_block_num - 1
    handle_block_count = 0

    start_height = 0
    if df.empty is True:
        start_height = 0
    else:
        start_height = df.num
    # 当数据库的高度比当前高度小(delay+1)
    print(now_block_num)
    if start_height[0] <= now_block_num:
        cur_time = str(datetime.datetime.now())  # 获取当前时间
        print("开始处理TRX交易，当前处理的高度为： " + str(start_height[0]) + "当前时间：" + cur_time)
        parseTxAndStoreTrc(int(start_height[0]), 0,monitor_dict,monitor_hash_dict)

    pass
