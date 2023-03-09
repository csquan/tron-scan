# -*- coding: utf-8 -*-
import csv
import math
import os.path
import codecs
from tronapi.tronapi import Tronapi
import time
import base58
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
Session = sessionmaker(bind=engine)
session = Session()

monitor_engine = create_engine('mysql+mysqldb://root:fat-chain-root-password@my-sql:3306/Tron_Collect', pool_size=0, max_overflow=-1)
monitor_Session = sessionmaker(bind=monitor_engine)
monitor_session = monitor_Session()

Base = declarative_base()


class TxKafka:
    def __init__(self):
        self.From = ""
        self.To = ""
        self.Uid = ""
        self.Amount = ""
        self.TokenType = 0
        self.TxHash = ""
        self.Chain = ""
        self.ContractAddr = ""
        self.Decimals = 0
        self.AssetSymbol = ""
        self.TxHeight = 0
        self.CurChainHeight = 0
        self.LogIndex = 0


class TxMatchPush:
    def __init__(self):
        self.Hash = ""
        self.Chain = ""
        self.TxHeight = ""
        self.CurChainHeight = ""
        self.OrderId = 0
        self.Success = ""
        self.GasLimit = ""
        self.GasPrice = ""
        self.GasUsed = ""
        self.Index = ""
        self.ContractAddr = ""


class TxMonitorHash:
    def __init__(self):
        self.Hash = ""
        self.Chain = ""
        self.OrderID = ""
        self.PushState = ""
        self.ReceiptState = 0
        self.GetReceiptTimes = ""
        self.GasLimit = ""
        self.GasPrice = ""
        self.GasUsed = ""
        self.Index = ""
        self.ContractAddr = ""

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
def KafkaTxLogic(tx):
    query_sql = 'select f_uid from t_monitor where f_addr = "' + tx.t_toAddr + '"'
    df_uid = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

    if df_uid.empty is True:
        return
    else:  # UID存在
        print("找到UID")
        print(df_uid.head().f_uid[0])

        query_sql = 'select * from t_monitor_hash where f_hash = "' + tx.t_hash + '"'
        df_hash = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

        if df_hash.empty is True:  # 在状态hash中没找到
            a = TxKafka()
            a.Uid = "test"
            a.To = tx.t_toAddr
            a.From = tx.t_fromAddr
            a.Amount = tx.t_amount
            a.TokenType = 2
            a.TxHash = tx.t_hash
            a.Chain = "trx"
            a.ContractAddr = tx.t_contract_addr
            a.Decimals = "0"
            a.AssetSymbol = ""
            a.TxHeight = 0
            a.CurChainHeight = 0
            a.LogIndex = 0


            aa_str = json.dumps(a,default=lambda o: o.__dict__,sort_keys=True, indent=4)

            bootstrap_servers = ['192.168.31.242:9092']

            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

            bb = bytes(aa_str, 'utf-8')

            producer.send(
                topic="tx-topic",
                value=bb).add_callback(on_send_success).add_errback(on_send_error)




# TRC20/TRC发送kafka逻辑（充值）： 状态hash表：monitor_hash 监控表：monitor
# 1 从监控表中取tx20.toAdd地址对应的UID ，如果能取到，则进入下一阶段
# 2 从状态hash表中取出当前的交易hash，如果没找到，则进入下一阶段
# 3 db中取出token的精度（addtoken添加进db）
# 4 组装消息发送
def KafkaMatchTxLogic(tx):
    query_sql = 'select * from t_monitor_hash where f_hash = "' + tx.t_hash + '"'
    df_match_hash = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())

    if df_match_hash.empty is False:  # 在状态hash中匹配到,df_match_hash取值
        print("在状态hash表中匹配到" + tx.hash)
        a = TxMatchPush()
        a.Hash = tx.hash
        a.Chain = "trx"
        a.TxHeight = ""
        a.CurChainHeight = ""
        a.OrderID = "test"
        a.Success = 1
        a.GasLimit = ""
        a.GasPrice = ""
        a.GasUsed = ""
        a.ContractAddr = tx.contract_addr
        a.Index = 0

        aa_str = json.dumps(a,default=lambda o: o.__dict__,sort_keys=True, indent=4)

        bootstrap_servers = ['192.168.31.242:9092']

        producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

        bb = bytes(aa_str, 'utf-8')

        producer.send(
            topic="match-topic",
            value=bb).add_callback(on_send_success).add_errback(on_send_error)



def ParseLog(log_data, blocksnum, transaction_at):
    list = []

    for obj in enumerate(log_data):
        idx = obj[0]
        logs = obj[1]
        if "result" not in logs["receipt"]:
            continue
        if logs["receipt"]["result"] != "SUCCESS":
            continue
        if "log" not in logs:
            continue
        for log in logs["log"]:
            if "topics" not in log:
                continue
            if len(log["topics"]) != 3:
                continue
            if len(log["topics"][0]) != 64 or len(log["topics"][1]) != 64 or len(log["topics"][2]) != 64:
                continue
            contractaddr = log["address"]
            contractaddr = keys.to_base58check_address(contractaddr)
            if log["topics"][0][0:2] != "0x":
                log["topics"][0] = "0x" + log["topics"][0]
            if log["topics"][0] != TransferTopic:
                continue
            fromaddr = log["topics"][1]
            toaddr = log["topics"][2]
            fromaddr = "41" + fromaddr[24:]
            toaddr = "41" + toaddr[24:]
            fromaddr = base58.b58encode_check(bytes.fromhex(fromaddr))
            toaddr = base58.b58encode_check(bytes.fromhex(toaddr))

            fromaddr = fromaddr.decode()
            toaddr = toaddr.decode()
            val = decode_hex(log["data"][24:])
            amount = int.from_bytes(val[0], byteorder='big')

            t20tx = TRC20Transaction(
                t_hash=logs["id"],
                t_block=blocksnum,
                t_fromAddr=fromaddr,
                t_toAddr=toaddr,
                t_block_at=transaction_at,
                t_amount=str(amount),
                t_contract_addr=contractaddr,
                t_status=1,   # todo: 该字段取实际执行
            )
            list.append(t20tx)

            KafkaTxLogic(t20tx) # 充值交易
    return list


def parseLogStoreTrc20(block_num, delay):
    time.sleep(delay)
    tron_api = Tronapi()
    try:
        transactionsData = tron_api.getWalletsolidityBlockByNum(block_num)
    except Exception as e:
        print("可能接口请求过于频繁,因此休眠5秒后重新请求")
        return parseLogStoreTrc20(block_num, 5)
    try:
        transaction_at = (transactionsData['block_header']['raw_data']['timestamp']) / 1000
        transaction_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(transaction_at))
    except Exception as e:
        print(e)
        return
    if transactionsData.get("transactions") is None:
        return
    # 取log数据并存储db
    logData = tron_api.getTxInfoByNum(block_num)
    try:
        # 解析log为TRC20交易
        log_list = ParseLog(logData, block_num, transaction_at)
        # 更新task当前高度
        new_height = block_num + 1
        update_sql = 'update f_task set num = "' + str(new_height) + '" where name = "TRC20"'
        session.execute(text(update_sql))
        session.add_all(log_list)
        # 这里保证事物一次提交
        session.commit()
    except Exception as e:
        print(e)
        return

def hexStr_to_str(hex_str):
    hex = hex_str.decode('utf-8')
    str_bin = binascii.unhexlify(hex)
    return str_bin.decode('utf-8')

def parseTxAndStoreTrc(block_num, delay=0):
    time.sleep(delay)
    tron_api = Tronapi()
    try:
        transactionsData = tron_api.getWalletsolidityBlockByNum(block_num)
    except Exception as e:
        print(e)
        print("可能接口请求过于频繁,因此重新请求")
        return parseTxAndStoreTrc(block_num, 5)
    try:
        transaction_at = (transactionsData['block_header']['raw_data']['timestamp']) / 1000
        transaction_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(transaction_at))
    except Exception as e:
        print(e)
        return
    if transactionsData.get("transactions") is None:
        return

    ContactArray = GetContactArray()
    # 这里是TRC和TRC10交易，以48896576为例，158笔 和浏览器对应
    tx_list = []
    contract_list = []
    for transaction in transactionsData['transactions']:
        if 'contract_address' in transaction['raw_data']['contract'][0]['parameter']['value']: # 合约交易
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
            tx = Transaction(
                t_hash=transaction['txID'],
                t_block=block_num,
                t_fromAddr=from_address,
                t_toAddr=to_address,
                t_block_at=transaction_at,
                t_amount=transactionAmount,
                t_contract_addr=contract,
                t_is_contract="True"
            )
            tx_list.append(tx)
        else:  # 非合约交易
            tx_detail = transaction['raw_data']['contract'][0]['parameter']['value']
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
                tx_list.append(tx)
            else:  # resource = "energy"
                continue
        KafkaMatchTxLogic(tx)  # 状态hash匹配
        KafkaTxLogic(tx)  # 充值交易

    # 更新task当前高度
    new_height = block_num + 1
    update_sql = 'update f_task set num = "' + str(new_height) + '" where name = "TRC"'
    session.execute(text(update_sql))
    session.add_all(tx_list)
    # 这里可能有部分合约已经存入导致重复错误
    session.add_all(contract_list)
    session.commit()


Base.metadata.create_all(engine, checkfirst=True)

Init()

tronapi = Tronapi()

while True:
    try:
        GetNowBlock = tronapi.getConfirmedCurrentBlock()
    except Exception as e:
        # 过于频繁的请求波场接口可能会强制限制一段时间,此时sleep一下
        time.sleep(5)
        continue
    # 这里应该从db中读取TRC20的任务高度
    sql = r'select * from f_task where name="TRC20"'
    # 读取SQL数据库
    df = pd.read_sql_query(sql=text(sql), con=engine.connect())  # 读取SQL数据库，并获得pandas数据帧。
    now_block_num = int(GetNowBlock.get('block_header').get('raw_data').get('number'))
    handle_block_count = 0
    delay = 0  # trx的不可逆高度 - 目前从rpc获取来的区块比最新高度低19个区块，足够不可逆了

    start_height = 0
    if df.empty is True:
        start_height = 0
    else:
        start_height = df.num
    # 当数据库的高度比当前高度小(delay+1)
    if start_height[0] + delay + 1 <= now_block_num:
        cur_time = str(datetime.datetime.now())  # 获取当前时间
        print("开始处理TRC20交易，当前处理的高度为： " + str(start_height[0]) + "当前时间：" + cur_time)
        parseLogStoreTrc20(int(start_height[0]), 0)

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
    if start_height[0] + delay + 1 <= now_block_num:
        cur_time = str(datetime.datetime.now())  # 获取当前时间
        print("开始处理TRC交易，当前处理的高度为： " + str(start_height[0]) + "当前时间：" + cur_time)

        parseTxAndStoreTrc(int(start_height[0]), 0)

    pass
