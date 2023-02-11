# -*- coding: utf-8 -*-
import csv
import math
import os.path
from tronapi.tronapi import Tronapi
from vendor.ThreadPool import ThreadPool, WorkRequest
import time
from tronapi import keys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+mysqldb://root:csquan253905@localhost:3306/TronBlock')
Session = sessionmaker(bind=engine)
session = Session()

Base = declarative_base()


class NormalTransaction(Base):
    __tablename__ = 'normal_transaction'

    id = Column(Integer, primary_key=True)

    hash = Column(String(64), nullable=False, index=True)
    block = Column(Integer, nullable=False, index=False)
    fromAddr = Column(String(64), nullable=False, index=False)
    toAddr = Column(String(64), nullable=False, index=False)
    block_at = Column(String(64), nullable=False, index=False)
    amount = Column(Integer, nullable=False, index=False)
    symbol = Column(String(32), nullable=False, index=False)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.hash)


class TRC20Transaction(Base):
    __tablename__ = 'trc20_transaction'

    id = Column(Integer, primary_key=True)

    hash = Column(String(64), nullable=False, index=True)
    block = Column(Integer, nullable=False, index=False)
    fromAddr = Column(String(64), nullable=False, index=False)
    toAddr = Column(String(64), nullable=False, index=False)
    block_at = Column(String(64), nullable=False, index=False)
    amount = Column(Integer, nullable=False, index=False)
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


def handleThread(blocksnum, delay=0):
    time.sleep(delay)
    print(blocksnum)
    tronapi = Tronapi()
    try:
        transactionsData = tronapi.getWalletsolidityBlockByNum(blocksnum)
    except Exception as e:
        # 可能接口请求过于频繁,因此重新请求
        return handleThread(blocksnum, 5)
    try:
        transaction_at = (transactionsData['block_header']['raw_data']['timestamp']) / 1000
        transaction_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(transaction_at))
    except Exception as e:
        return
    if transactionsData.get("transactions") is None:
        return
    ContactArray = GetContactArray()
    WalletArray = GetWalletArray()
    with open("%s/data/%s.csv" % (os.getcwd(), blocksnum), "w", newline='') as f:
        writer = csv.writer(f)
        for transaction in transactionsData['transactions']:
            if 'contract_address' in transaction['raw_data']['contract'][0]['parameter']['value']:
                try:
                    contract = transaction['raw_data']['contract'][0]['parameter']['value']['contract_address']
                    contract = keys.to_base58check_address(contract)
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
                to_address = keys.to_base58check_address(
                    transaction['raw_data']["contract"][0]["parameter"]["value"]["data"][9:72].lstrip("0"))
                transaction_data = [
                    transaction['txID'],
                    blocksnum,
                    from_address,
                    to_address,
                    transaction_at,
                    transactionAmount,
                    contract,
                    transaction['ret'][0]['contractRet'].lower()
                ]
                # write to mysql
                t20tx = TRC20Transaction(
                    hash=transaction['txID'],
                    block=blocksnum,
                    fromAddr=from_address,
                    toAddr=to_address,
                    block_at=transaction_at,
                    amount=transactionAmount,
                    contract_address=contract,
                    status=transaction['ret'][0]['contractRet'].lower(),
                )
                session.add(t20tx)
                session.commit()

                # writer.writerow(transaction_data)

                for wallet in WalletArray:
                    if to_address == wallet[0]:
                        with open(os.getcwd() + "/config/transaction.csv", "a", newline='') as f_transaction:
                            writer_transaction = csv.writer(f_transaction)
                            writer_transaction.writerow(transaction_data)
            else:
                tx_detail = transaction['raw_data']['contract'][0]['parameter']['value']
                # write to mysql
                normal_tx = NormalTransaction(
                    hash=transaction['txID'],
                    block=blocksnum,
                    fromAddr=tx_detail["owner_address"],
                    toAddr=tx_detail["to_address"],
                    block_at=transaction_at,
                    amount=tx_detail['amount'],
                    symbol="trx",
                )
                session.add(normal_tx)
                session.commit()


Base.metadata.create_all(engine, checkfirst=True)

Init()

tronapi = Tronapi()

main = ThreadPool(2)
while True:
    try:
        GetNowBlock = tronapi.getConfirmedCurrentBlock()
        # print(GetNowBlock)
    except Exception as e:
        # 过于频繁的请求波场接口可能会强制限制一段时间,此时sleep一下
        print(e)
        time.sleep(10)
        continue
    now_block_num = int(GetNowBlock.get('block_header').get('raw_data').get('number'))
    min_block_num = now_block_num - 10
    handle_block_count = 0
    for block_num in range(min_block_num, now_block_num):
        block_date = time.strftime("%Y-%m-%d", time.localtime(
            int(GetNowBlock.get('block_header').get('raw_data').get('timestamp')) / 1000))
        block_file_name = "%s/data/%s.csv" % (os.getcwd(), block_num)
        if not os.path.exists(block_file_name):
            handle_block_count += 1
            with open(block_file_name, "w", newline='') as f:
                pass
            req = WorkRequest(handleThread, args=[block_num, handle_block_count], kwds={})
            main.putRequest(req)

    # Rehandle重处理判断
    # TODO 如果是数据库可按照创建时间和处理时间对比一下判断是否重查询

    # 如果区块较少的,可等待久点累积一下
    if handle_block_count < 5:
        time.sleep(10)
    else:
        time.sleep(5)
    pass
