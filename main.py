# -*- coding: utf-8 -*-
import os.path
import yaml
import _thread
import codecs
from tronapi.tronapi import Tronapi
import time
import sqlalchemy
from tronapi import keys
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from kafka3 import KafkaProducer, KafkaConsumer
import binascii
import pandas as pd
import json
import datetime

decode_hex = codecs.getdecoder("hex_codec")

# TODO 确认监听的TRC20都是该topic
TransferTopic = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

Base = sqlalchemy.orm.declarative_base()
engine = {}
kafka_server = []
tx_topic = "tx-topic"
matched_topic = "match-topic"
user_create_topic = "registrar-user-created"
trx_type = 4
trc20_type = 5
contract_list = []
contract_hex_list = []  # 为了查找方便，设置一个额外的数据结构

def get_config(name: str):
    conf = f"./conf/{name}.yml"
    # 有需要修改的内容请复制到 config.toml ，并进行修改
    with open(conf, encoding="utf-8") as f:
        data = yaml.full_load(f)
    return data

class tasks(Base):
    __tablename__ = "f_task"
    id = Column(Integer, primary_key=True)
    num = Column(Integer, nullable=False, index=True)
    name = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)

class Transaction(Base):
    #__tablename__ = "f_tx"
    __abstract__ = True  # 关键语句,定义所有数据库表对应的父类
    __table_args__ = {"extend_existing": True}  # 允许表已存在
    t_id = Column(Integer, primary_key=True)
    t_hash = Column(String(64), nullable=False, index=True)
    t_block = Column(Integer, nullable=False, index=False)
    t_fromAddr = Column(String(64), nullable=False, index=False)
    t_toAddr = Column(String(64), nullable=False, index=False)
    t_block_at = Column(String(64), nullable=False, index=False)
    t_amount = Column(String(64), nullable=False, index=False)
    t_is_contract = Column(String(64), nullable=False, index=False)
    t_contract_addr = Column(String(64), nullable=False, index=False)
    t_token_type = Column(Integer, nullable=False, index=False)
    t_index = Column(Integer, nullable=False, index=False)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.t_hash)

def get_tx_model_cls(cid, cid_class_dict={}):
    if cid not in cid_class_dict:
        cls_name = table_name = cid
        cls = type(cls_name, (Transaction, ), {'__tablename__': table_name})
        cid_class_dict[cid] = cls
        Base.metadata.create_all(engine, checkfirst=True)
    return cid_class_dict[cid]

class TRC20Transaction(Base):
    #__tablename__ = "f_trc20_tx"
    __abstract__ = True  # 关键语句,定义所有数据库表对应的父类
    __table_args__ = {"extend_existing": True}  # 允许表已存在
    t_id = Column(Integer, primary_key=True)
    t_hash = Column(String(64), nullable=False, index=True)
    t_block = Column(Integer, nullable=False, index=False)
    t_fromAddr = Column(String(64), nullable=False, index=False)
    t_toAddr = Column(String(64), nullable=False, index=False)
    t_block_at = Column(String(64), nullable=False, index=False)
    t_amount = Column(String(64), nullable=False, index=False)
    t_contract_addr = Column(String(64), nullable=False, index=False)
    t_status = Column(String(64), nullable=False, index=False)
    t_token_type = Column(Integer, nullable=False, index=False)
    t_index = Column(Integer, nullable=False, index=False)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.t_hash)

def get_trc20_model_cls(cid, trc20_class_dict={}):
    if cid not in trc20_class_dict:
        cls_name = table_name = cid
        cls = type(cls_name, (TRC20Transaction, ), {'__tablename__': table_name})
        trc20_class_dict[cid] = cls
        Base.metadata.create_all(engine, checkfirst=True)
    return trc20_class_dict[cid]

class Contract(Base):
    __tablename__ = "f_contract"
    id = Column(Integer, primary_key=True)
    t_contract_addr = Column(String(64), nullable=False, index=False)
    t_name = Column(String(64), nullable=False, index=False)
    t_symbol = Column(String(64), nullable=False, index=True)
    t_decimal = Column(String(64), nullable=False, index=False)
    t_total_supply = Column(String(64), nullable=False, index=False)

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.t_contract_addr)

class UserInfo:
    def __init__(self,uid):
        self.f_uid = pd.Series(uid)

def Init():
    trxContract.t_decimal = "6"
    trxContract.t_symbol = "trx"
    trxContract.t_name = "trx"


def on_send_success(record_metadata=None):
    print(record_metadata.topic)


def on_send_error(excp=None):
    print("I am an errback", exc_info=excp)


# TRC20/TRC发送kafka逻辑（充值）： 状态hash表：monitor_hash 监控表：monitor
# 1 从监控表中取tx20.toAdd地址对应的UID ，如果能取到，则进入下一阶段
# 2 从状态hash表中取出当前的交易hash，如果没找到，则进入下一阶段
# 3 db中取出token的精度（addtoken添加进db）
# 4 组装消息发送
def KafkaTxLogic(tx, contract_obj, block_num, monitor_dict):
    txKafka = {}
    if monitor_dict is None:
        return
    if tx.t_toAddr not in monitor_dict:
        return
    else:  # UID存在
        # 先这么着吧
        for value in monitor_dict[tx.t_toAddr].f_uid:
            txKafka["uid"] = value

        txKafka["token_type"] = tx.t_token_type
        if tx.t_token_type == 4:  # 本币
            txKafka["amount"] = str(tx.t_amount)
        elif tx.t_token_type == 5:  # trc20代币
            txKafka["amount"] = str(tx.t_amount * (10 ** int(contract_obj.t_decimal)))
        else:
            print("不支持的代币类型：", tx.t_token_type)
            return
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
            txKafka["log_index"] = tx.t_index

            aa_str = json.dumps(
                txKafka, default=lambda o: o.__dict__, sort_keys=True, indent=4
            )

            producer = KafkaProducer(bootstrap_servers=kafka_server)

            bb = bytes(aa_str, "utf-8")


            producer.send(
                topic=tx_topic,
                value=bb).add_callback(on_send_success).add_errback(on_send_error)


# TRC20/TRC发送kafka逻辑（充值）： 状态hash表：monitor_hash 监控表：monitor
# 1 从监控表中取tx20.toAdd地址对应的UID ，如果能取到，则进入下一阶段
# 2 从状态hash表中取出当前的交易hash，如果没找到，则进入下一阶段
# 3 db中取出token的精度（addtoken添加进db）
# 4 组装消息发送
def KafkaMatchTxLogic(tx, transaction, block_num, monitor_hash_dict, logData):
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

        query_sql = (
                'select f_order_id from t_monitor_hash where f_hash = "' + tx.t_hash + '"'
        )
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

        aa_str = json.dumps(
            txpush, default=lambda o: o.__dict__, sort_keys=True, indent=4
        )

        producer = KafkaProducer(bootstrap_servers=kafka_server)

        bb = bytes(aa_str, "utf-8")
        print(bb)
        # 测试屏蔽


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
    query_sql = "select f_addr, f_uid from t_monitor"
    df = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())
    if df.empty is True:
        return
    else:  # UID存在
        by_addr_dict = dict(tuple(df.groupby("f_addr")))
        return by_addr_dict

def GetMonitorHash():
    query_sql = "select * from t_monitor_hash"
    df = pd.read_sql_query(text(query_sql), con=monitor_engine.connect())
    if df.empty is True:
        return
    else:  # UID存在
        by_hash_dict = dict(tuple(df.groupby("f_hash")))
        return by_hash_dict

def hexStr_to_str(hex_str):
    hex = hex_str.decode("utf-8")
    str_bin = binascii.unhexlify(hex)
    return str_bin.decode("utf-8")

def getContractObj(contract_in_hex):
    # 1.先在当前的contract_list中查找 2.然后在db中查找 3.如果都找不到，再去远程接口取
    if contract_in_hex in contract_hex_list:
        index = contract_hex_list.index(contract_in_hex)
        return contract_list[index]
    else:
        # 从db中读取TRC合约信息
        contract_obj = (
            session.query(Contract).filter(Contract.t_contract_addr == keys.to_base58check_address(contract_in_hex)).first()
        )
        # TODO 理论上没有配的地址 可以忽略， 如果非要链上读取，可以放到一个multicall里
        if contract_obj is None:
            print("代币信息未在缓存和db中找到，开始取线上取:" , contract_in_hex, keys.to_base58check_address(contract_in_hex))
            res = tron_api.getContractInfo("name()", contract_in_hex)
            if "code" in res:  # 有错误
                print(contract_in_hex, "查不到代币name")
                name = "unknown"
            elif res["result"]["result"] is True:
                if res["constant_result"][0] == "":
                    print(contract_in_hex, "查不到代币name,应该没有name方法")
                    name = "unknown"
                else:
                    try:
                        length_str = res["constant_result"][0][64:128].lstrip("0")
                        length = int(str(length_str), 16)
                        name = bytes.fromhex(
                            res["constant_result"][0][128: 128 + length * 2]
                        ).decode()
                    except Exception as e1:
                        print(contract_in_hex, "查不到代币name,解析出错")
                        name = "unknown"

            res = tron_api.getContractInfo("symbol()", contract_in_hex)
            if "code" in res:  # 有错误
                print(contract_in_hex, "查不到代币symbol")
                symbol = "unknown"
            elif res["result"]["result"] is True:
                if res["constant_result"][0] == "":
                    print(contract_in_hex, "查不到代币symbol,应该没有symbol方法")
                    symbol = "unknown"
                else:
                    try:
                        symbol = bytes.fromhex(
                            res["constant_result"][0][128:192].rstrip("0")
                        ).decode()
                    except Exception as e2:
                        print(contract_in_hex, "查不到代币symbol,解析出错")
                        symbol = "unknown"
            res = tron_api.getContractInfo("decimals()", contract_in_hex)
            if "code" in res:  # 有错误
                print(contract_in_hex, "查不到代币decimals")
                decimals = "unknown"
            elif res["result"]["result"] is True:
                if res["constant_result"][0] == "":
                    print(contract_in_hex, "查不到代币decimals,应该没有decimals方法")
                    decimals = 18
                else:
                    try:
                        decimals = res["constant_result"][0].lstrip("0")
                    except Exception as e3:
                        print(contract_in_hex, "查不到代币decimals,解析出错")
                        decimals = 18
            res = tron_api.getContractInfo("totalSupply()", contract_in_hex)
            if "code" in res:  # 有错误
                print(contract_in_hex, "查不到代币totalSupply")
                totalSupply = 0
            elif res["result"]["result"] is True:
                if res["constant_result"][0] == "":
                    print(contract_in_hex, "查不到代币totalSupply,应该没有totalSupply方法")
                    totalSupply = 0
                else:
                    try:
                        totalSupply = int(res["constant_result"][0].lstrip("0"), 16)
                    except Exception as e4:
                        print(contract_in_hex, "查不到代币totalSupply,解析出错")
                        totalSupply = 0
            contract_obj = Contract(
                t_contract_addr=contract_in_hex,
                t_name=name,
                t_symbol=symbol,
                t_decimal=decimals,
                t_total_supply=str(totalSupply),
            )
        contract_hex_list.append(contract_in_hex)
        contract_list.append(contract_obj)
        return contract_obj

def parseTxAndStoreTrc(
        block_num, delay, transactionsData, logsData, monitor_dict, monitor_hash_dict
):
    time.sleep(delay)
    # 获取交易数据-本币交易从该数据出
    if transactionsData:
        pass
    else:
        try:
            transactionsData = tron_api.getWalletSolidityBlockByNum(block_num)
        except Exception as e:
            print("获取区块交易数据出错:")
            print(e)
            return parseTxAndStoreTrc(
                block_num,
                1,
                transactionsData,
                logsData,
                monitor_dict,
                monitor_hash_dict,
            )
    # 获取事件数据-TRC20交易从该数据出
    if logsData:
        pass
    else:
        try:
            logsData = tron_api.getTxInfoByNum(block_num)
        except Exception as e:
            print("获取区块事件数据出错:")
            print(e)
            return parseTxAndStoreTrc(
                block_num,
                1,
                transactionsData,
                logsData,
                monitor_dict,
                monitor_hash_dict,
            )
    # 解析区块时间
    try:
        transaction_time = (
                             transactionsData["block_header"]["raw_data"]["timestamp"]
                         ) / 1000
        transaction_at = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(transaction_time)
        )
        transaction_data = time.strftime(
            "%Y-%m-%d", time.localtime(transaction_time)
        )
    except Exception as e:
        print("解析区块事件数据出错:")
        print(e)
        return block_num
    f_tx_table = "f_tx_" + transaction_data
    f_trc20_tx_table = "f_trc20_tx_" + transaction_data
    # 解析交易数据
    if "transactions" in transactionsData:
        # 获取要监听的合约-怎么没用？
        # ContactArray = GetContactArray()
        # 这里是TRC和TRC10交易，以48896576为例，158笔 和浏览器对应
        tx_list = []
        tx20_list = []
        for transaction in transactionsData["transactions"]:
            # 一笔交易里会存在多笔转账
            txs = transaction["raw_data"]["contract"]
            # tx_hash和index来标识唯一一笔转账
            index = -1
            for tra in txs:
                index += 1
                tx_type = tra["type"]
                # 找到 txID
                txId = transaction["txID"]
                if txId == "5debd44813b0ef03537c62238b66b939558a9d1702c51b480b91bee254b7e6a0":
                    print(txId)
                # TransferContract： 本币交易 TransferAssetContract： TRC10交易  TriggerSmartContract：合约交易
                if tx_type == "TransferContract":
                    tx_detail = tra["parameter"]["value"]
                    if "amount" in tx_detail:
                        transactionAmount = tx_detail["amount"]
                        toAddr = keys.to_base58check_address(tx_detail["to_address"])
                        fromAddr = keys.to_base58check_address(
                            tx_detail["owner_address"]
                        )
                        # write to mysql
                        tx = get_tx_model_cls(f_tx_table)(
                            t_hash=txId,
                            t_block=block_num,
                            t_fromAddr=fromAddr,
                            t_toAddr=toAddr,
                            t_block_at=transaction_at,
                            t_amount=transactionAmount,
                            t_contract_addr="",
                            t_is_contract="False",
                            t_token_type=trx_type,
                            t_index=index,
                        )
                        tx_list.append(tx)
                        KafkaMatchTxLogic(
                            tx, transaction, block_num, monitor_hash_dict, logsData
                        )  # 状态hash匹配
                        KafkaTxLogic(tx, trxContract, block_num, monitor_dict)  # 充值交易
                    else:  # resource = "energy"
                        continue
                elif tx_type == "TriggerSmartContract":
                    if txId == "5debd44813b0ef03537c62238b66b939558a9d1702c51b480b91bee254b7e6a0":
                        print(txId)
                    # 找到logs
                    for logs in logsData:
                        if logs["id"] == txId:  # 找到了对应的tx_id
                            if logs["receipt"]["result"] != "SUCCESS":
                                continue  # 失败的交易直接忽略
                            # 在内部交易里过滤出
                            if "internal_transactions" in logs:
                                intxs = logs["internal_transactions"]
                                for intx in intxs:
                                    callValueInfos = intx["callValueInfo"]
                                    for callValueInfo in callValueInfos:
                                        if "callValue" in callValueInfo:
                                            callValue = callValueInfo["callValue"]
                                            fromAddr = keys.to_base58check_address(intx["caller_address"])
                                            toAddr = keys.to_base58check_address(intx["transferTo_address"])
                                            # write to mysql
                                            tx = get_tx_model_cls(f_tx_table)(
                                                t_hash=txId,
                                                t_block=block_num,
                                                t_fromAddr=fromAddr,
                                                t_toAddr=toAddr,
                                                t_block_at=transaction_at,
                                                t_amount=callValue,
                                                t_contract_addr="",
                                                t_is_contract="False",
                                                t_token_type=trx_type,
                                                t_index=index,
                                            )
                                            index += 1
                                            tx_list.append(tx)
                                            KafkaMatchTxLogic(
                                                tx, transaction, block_num, monitor_hash_dict, logsData
                                            )  # 状态hash匹配
                                            KafkaTxLogic(tx, trxContract, block_num, monitor_dict)  # 充值交易

                            if "log" in logs:
                                logList = logs["log"]
                                for log in logList:
                                    trc20hex = "41" + log["address"]
                                    trc20Address = keys.to_base58check_address(trc20hex)
                                    # TODO 检测 trc20Address 是否是监控的地址
                                    if "topics" not in log:
                                        continue
                                    topics = log["topics"]
                                    if topics:
                                        if topics[0] == TransferTopic:  # 转账交易
                                            # 当 mint 的时候没有data, 值在topics[3]
                                            # TODO 是否可以去调从24开始截取
                                            if "data" not in log:
                                                amount = int.from_bytes(
                                                    decode_hex(topics[3])[0],
                                                    byteorder="big",
                                                )
                                            else:
                                                amount = int.from_bytes(
                                                    decode_hex(log["data"])[0],
                                                    byteorder="big",
                                                )
                                            # write to mysql
                                            tx20 = get_trc20_model_cls(f_trc20_tx_table)(
                                                t_hash=txId,
                                                t_block=block_num,
                                                t_fromAddr=keys.to_base58check_address("41" + topics[1][24:]),
                                                t_toAddr=keys.to_base58check_address("41" + topics[2][24:]),
                                                t_block_at=transaction_at,
                                                t_amount=amount,
                                                t_contract_addr=trc20Address,
                                                t_status="success",
                                                t_token_type=trc20_type,
                                                t_index=index,
                                            )
                                            index += 1
                                            tx20_list.append(tx20)
                                            contract_obj = getContractObj(trc20hex)
                                            KafkaMatchTxLogic(
                                                tx20,
                                                transaction,
                                                block_num,
                                                monitor_hash_dict,
                                                logsData,
                                            )  # 状态hash匹配
                                            KafkaTxLogic(
                                                tx20, contract_obj, block_num, monitor_dict
                                            )  # 充值交易
                            continue
                else:  # 其它交易 忽略
                    pass
            session.add_all(tx_list)
            session.add_all(tx20_list)
            session.add_all(contract_list)

        # 更新task当前高度
        new_height = block_num + 1
        update_sql = (
                'update f_task set num = "' + str(new_height) + '" where name = "TRC"'
        )
        session.execute(text(update_sql))
        session.commit()
    return block_num + 1
# TODO 优化一个 定时刷新数据库增量数据的任务 防止kafka出错或者没读取到数据
def consumer_user_create():
    consumer = KafkaConsumer(user_create_topic, group_id='groupTrxSync', bootstrap_servers=kafka_server,
                             auto_offset_reset='earliest',
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for msg in consumer:
        try:
            user = msg.value
            monitor_dict[user['trx']] = UserInfo(user['uid'])
        except Exception as e:
            print("解析kafka数据出错", e)


def getNewBlockNumber():
    # 官方RPC地址 限制为一天10万次请求 每秒20次
    try:  # TODO 需要确认 目前从rpc获取来的是不可逆区块，区块比最新高度低19个区块
        GetNowBlock = tron_api.getConfirmedCurrentBlock()
        return GetNowBlock
    except Exception as e:
        # 过于频繁的请求波场接口可能会强制限制一段时间,此时sleep一下
        time.sleep(1)
        return getNewBlockNumber()
    if GetNowBlock is None:
        time.sleep(1)
        return getNewBlockNumber()

if __name__ == '__main__':
    config = get_config("config")
    kafka_server = config["kafka_server"]
    print(kafka_server)
    # engine = create_engine('mysql+mysqldb://root:fat-chain-root-password@my-sql:3306/TronBlock')
    #"mysql+mysqldb://root:zzzz2020@127.0.0.1:3306/TronBlock"
    engine = create_engine(config["mysql_tb"])
    Session = sessionmaker(bind=engine)
    session = Session()

    # monitor_engine = create_engine('mysql+mysqldb://root:fat-chain-root-password@my-sql:3306/Hui_Collect', pool_size=0, max_overflow=-1)
    monitor_engine = create_engine(
        # "mysql+mysqldb://root:zzzz2020@127.0.0.1:3306/Hui_Collect",
        config["mysql_hc"],
        pool_size=0,
        max_overflow=-1,
    )
    monitor_Session = sessionmaker(bind=monitor_engine)
    monitor_session = monitor_Session()

    monitor_dict = GetMonitor()
    monitor_hash_dict = GetMonitorHash()

    trxContract = Contract()  # 本币

    # 波场API对象
    tron_api = Tronapi(rpc=config["trx_rpc"])

    Base.metadata.create_all(engine, checkfirst=True)
    # 初始化
    Init()

    try:
        _thread.start_new_thread(consumer_user_create, ())
    except Exception as e:
        print("Error: 无法启动线程", e)

    # 从db中读取TRC任务高度
    sql = r'select * from f_task where name="TRC"'
    # 读取SQL数据库
    df = pd.read_sql_query(
        sql=text(sql), con=engine.connect()
    )  # 读取SQL数据库，并获得pandas数据帧。

    # 当前最新区块高度
    now_block_num = int(getNewBlockNumber().get("block_header").get("raw_data").get("number"))
    # 当数据库的高度比当前高度小(delay+1),官方RPC获取的是固话块delay=0,换其它RPC需要考虑deply是否也是0
    print("当前区块高度：" + str(now_block_num))
    # 任务开始区块
    start_height = [0]
    if df.empty is False:
        start_height = df.num
    taskBeginBlockNumber = start_height[0]
    # 死循环去扫块
    while True:
        if taskBeginBlockNumber <= now_block_num:
            cur_time = str(datetime.datetime.now())  # 获取当前时间
            print("开始处理TRX交易，当前处理的高度为： ", taskBeginBlockNumber,  " 当前时间：", cur_time)
            nextBlockNumber = parseTxAndStoreTrc(int(taskBeginBlockNumber), 0, {}, [], monitor_dict, monitor_hash_dict)
            taskBeginBlockNumber = nextBlockNumber
            if nextBlockNumber <= now_block_num:
                pass
            else:
                while True:
                    last_block_num = int(getNewBlockNumber().get("block_header").get("raw_data").get("number"))
                    if now_block_num == last_block_num:
                        time.sleep(2)
                    elif now_block_num < last_block_num:
                        now_block_num = last_block_num
                        break
                    else:
                        time.sleep(2)

        pass
