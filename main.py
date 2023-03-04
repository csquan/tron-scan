# -*- coding: utf-8 -*-
import csv
import math
import os.path
import codecs
from tronapi.tronapi import Tronapi
from vendor.ThreadPool import ThreadPool, WorkRequest
import time
import base58
from tronapi import keys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
import pandas as pd
from sqlalchemy import text

decode_hex = codecs.getdecoder("hex_codec")

TransferTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

engine = create_engine('mysql+mysqldb://root:csquan253905@localhost:3306/TronBlock')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()

class tasks(Base):
    __tablename__ = 'f_task'

    id = Column(Integer, primary_key=True)

    num = Column(Integer, nullable=False, index=True)
    name = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.name)

class Transaction(Base):
    __tablename__ = 'f_tx'

    id = Column(Integer, primary_key=True)

    hash = Column(String(64), nullable=False, index=True)
    block = Column(Integer, nullable=False, index=False)
    fromAddr = Column(String(64), nullable=False, index=False)
    toAddr = Column(String(64), nullable=False, index=False)
    block_at = Column(String(64), nullable=False, index=False)
    amount = Column(String(64), nullable=False, index=False)
    symbol = Column(String(32), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.hash)


class TRC20Transaction(Base):
    __tablename__ = 'f_trc20_tx'

    id = Column(Integer, primary_key=True)

    hash = Column(String(64), nullable=False, index=True)
    block = Column(Integer, nullable=False, index=False)
    fromAddr = Column(String(64), nullable=False, index=False)
    toAddr = Column(String(64), nullable=False, index=False)
    block_at = Column(String(64), nullable=False, index=False)
    amount = Column(String(64), nullable=False, index=False)
    contract_address = Column(String(64), nullable=False, index=False)
    status = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.hash)


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

def parseTxLog(logdata,blocksnum,transaction_at):
    count =0
    list = []
    for obj in enumerate(logdata):
        idx = obj[0]
        logs = obj[1]
        print(idx)
        if "result" not in logs["receipt"]:
            continue
        if logs["receipt"]["result"] != "SUCCESS":
           continue
        if "log" not in logs:
            continue
        for log in logs["log"]:
            if len(log["topics"]) != 3:
               continue
            if len(log["topics"][0]) != 64 or len(log["topics"][1]) != 64 or len(log["topics"][2]) != 64:
               continue
            contractaddr = log["address"]
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

            val = decode_hex(log["data"][24:])
            amount = int.from_bytes(val[0], byteorder='big')

            t20tx = TRC20Transaction(
                hash=logs["id"],
                block=blocksnum,
                fromAddr=fromaddr,
                toAddr=toaddr,
                block_at=transaction_at,
                amount=str(amount),
                contract_address=contractaddr,
                status=1,
            )
            list.append(t20tx)
            count = count + 1
    print(count)
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
        loglist = parseTxLog(logData, block_num, transaction_at)
        # 更新task当前高度
        new_height = block_num+1
        update_sql = 'update f_task set num = "' + str(new_height) + '" where name = "TRC20"'
        session.execute(text(update_sql))
        session.add_all(loglist)
        # 这里保证事物一次提交
        session.commit()
    except Exception as e:
        print(e)
        return


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

    # 这里是TRC和TRC10交易，以48896576为例，158笔 和浏览器对应
    tx_list = []
    for transaction in transactionsData['transactions']:
        if 'contract_address' not in transaction['raw_data']['contract'][0]['parameter']['value']:
            tx_detail = transaction['raw_data']['contract'][0]['parameter']['value']
            if "amount" in tx_detail:
                transactionAmount = tx_detail['amount']

                toAddr = keys.to_base58check_address(tx_detail["to_address"])
                fromAddr = keys.to_base58check_address(tx_detail["owner_address"])

                # write to mysql
                tx = Transaction(
                    hash=transaction['txID'],
                    block=block_num,
                    fromAddr=fromAddr,
                    toAddr=toAddr,
                    block_at=transaction_at,
                    amount=transactionAmount,
                    symbol="trx",
                )
                tx_list.append(tx)
    # 更新task当前高度
    new_height = block_num + 1
    update_sql = 'update f_task set num = "' + str(new_height) + '" where name = "TRC"'
    session.execute(text(update_sql))
    session.add_all(tx_list)
    session.commit()


Base.metadata.create_all(engine, checkfirst=True)

Init()

tronapi = Tronapi()

main = ThreadPool(2)
while True:
    try:
        GetNowBlock = tronapi.getConfirmedCurrentBlock()
    except Exception as e:
        # 过于频繁的请求波场接口可能会强制限制一段时间,此时sleep一下
        print(e)
        time.sleep(10)
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
        parseTxAndStoreTrc(int(start_height[0]), 0)

    pass
