import sys
import collections
import datetime
import inspect
from threading import Thread

import logging
import time
import os.path

from ibapi import wrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper

from ibapi.common import *
from ibapi.order_condition import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.scanner import ScannerSubscription
from ibapi.ticktype import *

from ibapi.account_summary_tags import *

import pandas as pd
import numpy as np


def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")
    time.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'
    timefmt = '%y%m%d_%H:%M:%S'

    logging.basicConfig(filename=time.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
                        filemode='w',
                        level=logging.INFO,
                        format=recfmt,
                        datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)
    logger.addHandler(console)


def printWhenExecuting(fn):
    def fn2(self):
        print(" doing", fn.__name__)
        fn(self)
        print(" done w/", fn.__name__)

    return fn2


def printinstance(inst: Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))


class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsgId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass


class TestClient(EClient):
    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)

        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                self.clntMeth2callCount[methName] = 0
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam
                    if paramName == 'reqId':
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(TestClient, methName, self.countReqId(methName, meth))


class TestWrapper(wrapper.EWrapper):

    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam
                if 'error' not in methName and paramName == 'reqId':
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))


# %%
class TestApp(TestWrapper, TestClient):
    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)

        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.reqGlobalCancelOnly = False
        self.simplePlaceOid = None
        self.globalCancelOnly = False

        self.reqMarketDataType(1)

        self.connect('127.0.0.1', 7497, 999)

        thread = Thread(target=self.run)
        thread.start()

        setattr(self, "_thread", thread)

        self.optionchain_req_underlyingPrice = None
        self.optionchain_underlyingPrice = None

        self.optionchain_req_End = False
        self.optionchain_req_chain = None

        self.optionchain_strikes = []
        self.optionchain_expirations = []
        self.option_chain_multiplier = None
        self.optionchain_contractNum = 0
        self.optionchain = dict(symbol={}, right={}, multiplier={}, expirations={}, strikes={}, gamma={}, conId={}, exchange={},
                                theta={}, delta={}, vega={}, undPrice={}, impliedVol={}, optPrice={})

    def gammascarping(self, symbol, exchange, secType, conID):

        while self.nextValidOrderId is None:
            time.sleep(1)
        underlyingContract = Contract()
        underlyingContract.symbol = symbol
        underlyingContract.conId = conID
        underlyingContract.exchange = exchange

        self.optionchain_req_underlyingPrice = self.nextOrderId()
        self.reqMktData(self.optionchain_req_underlyingPrice, underlyingContract, "221", False, False, [])

        # 先连接到tws后调用此函数

        self.optionchain_req_chain = self.nextOrderId()
        self.reqSecDefOptParams(self.optionchain_req_chain, symbol, exchange, secType, conID)

        while self.optionchain_req_End == False:
            time.sleep(1)

        conExp = np.array(list(set(self.optionchain_expirations)))
        conExp.sort()
        tmp_time_s = np.searchsorted(conExp, (datetime.datetime.now()+datetime.timedelta(days=7)).strftime("%Y%m%d"))
        tmp_time_e = np.searchsorted(conExp, (datetime.datetime.now() + datetime.timedelta(days=60)).strftime("%Y%m%d"))
        for aexp in conExp[tmp_time_s:min(tmp_time_e, len(conExp))]:
            while self.optionchain_underlyingPrice is None:
                time.sleep(1)
            conStrikes = np.array(list(set(self.optionchain_strikes)))
            conStrikes.sort()
            tmp_pos = np.searchsorted(conStrikes, self.optionchain_underlyingPrice)

            for aprice in conStrikes[max(tmp_pos - 8, 0):min(tmp_pos + 8, len(conStrikes) - 1)]:
                contract_req_ID = self.nextOrderId()
                tmp_contract = Contract()
                tmp_contract.symbol = symbol
                if secType == 'STK':
                    tmp_contract.secType = "OPT"
                else:
                    tmp_contract.secType = "FOP"
                tmp_contract.exchange = exchange
                tmp_contract.currency = "USD"
                tmp_contract.lastTradeDateOrContractMonth = aexp
                tmp_contract.strike = aprice
                tmp_contract.right = "C"

                self.optionchain['symbol'][contract_req_ID] = symbol
                self.optionchain['right'][contract_req_ID] = 'C'
                self.optionchain['strikes'][contract_req_ID] = aprice
                self.optionchain['expirations'][contract_req_ID] = aexp

                self.reqMktData(contract_req_ID, tmp_contract, "", False, False, [])

                self.reqContractDetails(contract_req_ID, tmp_contract)
                while self.optionchain_contractNum < len(self.optionchain['expirations']) :
                    time.sleep(1)

                # self.disconnect()

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        printinstance(contractDetails.contract)
        if reqId in self.optionchain['expirations'].keys():
            self.optionchain['conId'][reqId] = contractDetails.contract.conId
            self.optionchain['multiplier'][reqId] = contractDetails.contract.multiplier
            self.optionchain['exchange'][reqId] = contractDetails.contract.exchange

    @iswrapper
    def bondContractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().bondContractDetails(reqId, contractDetails)

    @iswrapper
    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        print("ContractDetailsEnd. ", reqId, "\n")
        if reqId in self.optionchain['expirations'].keys():
            self.optionchain_contractNum += 1

    @iswrapper
    def tickOptionComputation(self, reqId: TickerId, tickType: TickType,
                              impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, impliedVol, delta,
                                      optPrice, pvDividend, gamma, vega, theta, undPrice)
        print("TickOptionComputation. TickerId:", reqId, "tickType:", tickType,
              "ImpliedVolatility:", impliedVol, "Delta:", delta, "OptionPrice:",
              optPrice, "pvDividend:", pvDividend, "Gamma: ", gamma, "Vega:", vega,
              "Theta:", theta, "UnderlyingPrice:", undPrice)

        if (reqId in self.optionchain['expirations'].keys()) and (tickType == 13):
            self.optionchain['gamma'][reqId] = gamma
            self.optionchain['theta'][reqId] = theta
            self.optionchain['delta'][reqId] = delta
            self.optionchain['vega'][reqId] = vega
            self.optionchain['undPrice'][reqId] = undPrice
            self.optionchain['impliedVol'][reqId] = impliedVol
            self.optionchain['optPrice'][reqId] = optPrice

            self.cancelMktData(reqId)

    @iswrapper
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        print("Tick Price. Ticker Id:", reqId, "tickType:", tickType,
              "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
              "PastLimit:", attrib.pastLimit, end=' ')
        if tickType == TickTypeEnum.BID or tickType == TickTypeEnum.ASK:
            print("PreOpen:", attrib.preOpen)
        else:
            print()

        if (reqId == self.optionchain_req_underlyingPrice) and (tickType == 9):
            logging.info('self.optionchain_underlyingPrice: ' + str(price))
            self.optionchain_underlyingPrice = price
            self.cancelMktData(self.optionchain_req_underlyingPrice)
            logging.info('self.optionchain_underlyingPrice STOP ')

        if (reqId in self.optionchain['expirations'].keys()) and (tickType == 4):
            self.optionchain['optPrice'][reqId] =  price

    @iswrapper
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                          underlyingConId: int, tradingClass: str, multiplier: str,
                                          expirations: SetOfString, strikes: SetOfFloat):
        super().securityDefinitionOptionParameter(reqId, exchange,
                                                  underlyingConId, tradingClass, multiplier, expirations, strikes)
        print("Security Definition Option Parameter. ReqId:%d Exchange:%s "
              "Underlying conId: %d TradingClass:%s Multiplier:%s Exp:%s Strikes:%s",
              reqId, exchange, underlyingConId, tradingClass, multiplier,
              ",".join(expirations), ",".join(str(strikes)))

        if (reqId == self.optionchain_req_chain):

            logging.debug('self.optionchain is building: ' + str(self.optionchain_req_chain))
            for adate in expirations:
                for aprice in strikes:
                    self.optionchain_expirations.append(adate)
                    self.optionchain_strikes.append(aprice)
                    self.option_chain_multiplier = multiplier

    @iswrapper
    def securityDefinitionOptionParameterEnd(self, reqId: int):
        super().securityDefinitionOptionParameterEnd(reqId)
        print("Security Definition Option Parameter End. Request: ", reqId)

        self.optionchain_req_End = True

    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.async_python35:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
        # ! [nextvalidid]

        # we can start now
        # self.start()

    def start(self):
        if self.started:
            return

        self.started = True

        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
            print("Executing requests")
            self.reqGlobalCancel()
            # self.marketDataType_req()
            # self.accountOperations_req()
            self.tickDataOperations_req()
            # self.marketDepthOperations_req()
            # self.realTimeBars_req()
            self.historicalDataRequests_req()
            self.optionsOperations_req()
            # self.marketScanners_req()
            # self.reutersFundamentals_req()
            # self.bulletins_req()
            # self.contractOperations_req()
            # self.contractNewsFeed_req()
            # self.miscelaneous_req()
            # self.linkingOperations()
            # self.financialAdvisorOperations()
            # self.orderOperations_req()
            # self.marketRuleOperations()
            # self.pnlOperations()
            self.historicalTicksRequests_req()
            # self.tickByTickOperations()
            self.whatIfOrder_req()
            print("Executing requests ... finished")

    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            self.done = True

    def stop(self):
        print("Executing cancels")
        self.orderOperations_cancel()
        self.accountOperations_cancel()
        self.tickDataOperations_cancel()
        self.marketDepthOperations_cancel()
        self.realTimeBars_cancel()
        self.historicalDataRequests_cancel()
        self.optionsOperations_cancel()
        self.marketScanners_cancel()
        self.reutersFundamentals_cancel()
        self.bulletins_cancel()
        print("Executing cancels ... finished")

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)

    # ! [error] self.reqId2nErr[reqId] += 1

    @iswrapper
    def winError(self, text: str, lastError: int):
        super().winError(text, lastError)

    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. ID:", orderId, contract.symbol, contract.secType,
              "@", contract.exchange, ":", order.action, order.orderType,
              order.totalQuantity, orderState.status)
        # ! [openorder]

        if order.whatIf:
            print("WhatIf: ", orderId, "initMarginBefore: ", orderState.initMarginBefore, " maintMarginBefore: ",
                  orderState.maintMarginBefore,
                  "equityWithLoanBefore ", orderState.equityWithLoanBefore, " initMarginChange ",
                  orderState.initMarginChange, " maintMarginChange: ", orderState.maintMarginChange,
                  " equityWithLoanChange: ", orderState.equityWithLoanChange, " initMarginAfter: ",
                  orderState.initMarginAfter, " maintMarginAfter: ", orderState.maintMarginAfter,
                  " equityWithLoanAfter: ", orderState.equityWithLoanAfter)

        order.contract = contract
        self.permId2ord[order.permId] = order

    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        print("OpenOrderEnd")
        # ! [openorderend]

        logging.debug("Received %d openOrders", len(self.permId2ord))

    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print("OrderStatus. Id: ", orderId, ", Status: ", status, ", Filled: ", filled,
              ", Remaining: ", remaining, ", AvgFillPrice: ", avgFillPrice,
              ", PermId: ", permId, ", ParentId: ", parentId, ", LastFillPrice: ",
              lastFillPrice, ", ClientId: ", clientId, ", WhyHeld: ",
              whyHeld, ", MktCapPrice: ", mktCapPrice)

    # ! [orderstatus]

    @printWhenExecuting
    def accountOperations_req(self):
        # Requesting managed accounts***/
        # ! [reqmanagedaccts]
        self.reqManagedAccts()
        # ! [reqmanagedaccts]
        # Requesting accounts' summary ***/

        # ! [reqaaccountsummary]
        self.reqAccountSummary(9001, "All", AccountSummaryTags.AllTags)
        # ! [reqaaccountsummary]

        # ! [reqaaccountsummaryledger]
        self.reqAccountSummary(9002, "All", "$LEDGER")
        # ! [reqaaccountsummaryledger]

        # ! [reqaaccountsummaryledgercurrency]
        self.reqAccountSummary(9003, "All", "$LEDGER:EUR")
        # ! [reqaaccountsummaryledgercurrency]

        # ! [reqaaccountsummaryledgerall]
        self.reqAccountSummary(9004, "All", "$LEDGER:ALL")
        # ! [reqaaccountsummaryledgerall]

        # Subscribing to an account's information. Only one at a time!
        # ! [reqaaccountupdates]
        self.reqAccountUpdates(True, self.account)
        # ! [reqaaccountupdates]

        # ! [reqaaccountupdatesmulti]
        self.reqAccountUpdatesMulti(9005, self.account, "", True)
        # ! [reqaaccountupdatesmulti]

        # Requesting all accounts' positions.
        # ! [reqpositions]
        self.reqPositions()
        # ! [reqpositions]

        # ! [reqpositionsmulti]
        self.reqPositionsMulti(9006, self.account, "")
        # ! [reqpositionsmulti]

        # ! [reqfamilycodes]
        self.reqFamilyCodes()
        # ! [reqfamilycodes]

    @printWhenExecuting
    def accountOperations_cancel(self):
        # ! [cancelaaccountsummary]
        self.cancelAccountSummary(9001)
        self.cancelAccountSummary(9002)
        self.cancelAccountSummary(9003)
        self.cancelAccountSummary(9004)
        # ! [cancelaaccountsummary]

        # ! [cancelaaccountupdates]
        self.reqAccountUpdates(False, self.account)
        # ! [cancelaaccountupdates]

        # ! [cancelaaccountupdatesmulti]
        self.cancelAccountUpdatesMulti(9005)
        # ! [cancelaaccountupdatesmulti]

        # ! [cancelpositions]
        self.cancelPositions()
        # ! [cancelpositions]

        # ! [cancelpositionsmulti]
        self.cancelPositionsMulti(9006)
        # ! [cancelpositionsmulti]

    def pnlOperations(self):
        # ! [reqpnl]
        self.reqPnL(17001, "DU242650", "")
        # ! [reqpnl]
        time.sleep(1)
        # ! [cancelpnl]
        self.cancelPnL(17001)
        # ! [cancelpnl]

        # ! [reqpnlsingle]
        self.reqPnLSingle(17002, "DU242650", "", 265598);
        # ! [reqpnlsingle]
        time.sleep(1)
        # ! [cancelpnlsingle]
        self.cancelPnLSingle(17002);
        # ! [cancelpnlsingle]

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        print("Account list: ", accountsList)
        # ! [managedaccounts]

        self.account = accountsList.split(",")[0]

    @iswrapper
    # ! [accountsummary]
    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        print("Acct Summary. ReqId:", reqId, "Acct:", account,
              "Tag: ", tag, "Value:", value, "Currency:", currency)

    # ! [accountsummary]

    @iswrapper
    # ! [accountsummaryend]
    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print("AccountSummaryEnd. Req Id: ", reqId)

    # ! [accountsummaryend]

    @iswrapper
    # ! [updateaccountvalue]
    def updateAccountValue(self, key: str, val: str, currency: str,
                           accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        print("UpdateAccountValue. Key:", key, "Value:", val,
              "Currency:", currency, "AccountName:", accountName)

    # ! [updateaccountvalue]

    @iswrapper
    # ! [updateportfolio]
    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        print("UpdatePortfolio.", contract.symbol, "", contract.secType, "@",
              contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
              "MarketValue:", marketValue, "AverageCost:", averageCost,
              "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
              "AccountName:", accountName)

    # ! [updateportfolio]

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        print("UpdateAccountTime. Time:", timeStamp)

    # ! [updateaccounttime]

    @iswrapper
    # ! [accountdownloadend]
    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        print("Account download finished:", accountName)

    # ! [accountdownloadend]

    @iswrapper
    # ! [position]
    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        print("Position.", account, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency,
              "Position:", position, "Avg cost:", avgCost)

    # ! [position]

    @iswrapper
    # ! [positionend]
    def positionEnd(self):
        super().positionEnd()
        print("PositionEnd")

    # ! [positionend]

    @iswrapper
    # ! [positionmulti]
    def positionMulti(self, reqId: int, account: str, modelCode: str,
                      contract: Contract, pos: float, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        print("Position Multi. Request:", reqId, "Account:", account,
              "ModelCode:", modelCode, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency, ",Position:",
              pos, "AvgCost:", avgCost)

    # ! [positionmulti]

    @iswrapper
    # ! [positionmultiend]
    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        print("Position Multi End. Request:", reqId)

    # ! [positionmultiend]

    @iswrapper
    # ! [accountupdatemulti]
    def accountUpdateMulti(self, reqId: int, account: str, modelCode: str,
                           key: str, value: str, currency: str):
        super().accountUpdateMulti(reqId, account, modelCode, key, value,
                                   currency)
        print("Account Update Multi. Request:", reqId, "Account:", account,
              "ModelCode:", modelCode, "Key:", key, "Value:", value,
              "Currency:", currency)

    # ! [accountupdatemulti]

    @iswrapper
    # ! [accountupdatemultiend]
    def accountUpdateMultiEnd(self, reqId: int):
        super().accountUpdateMultiEnd(reqId)
        print("Account Update Multi End. Request:", reqId)

    # ! [accountupdatemultiend]

    @iswrapper
    # ! [familyCodes]
    def familyCodes(self, familyCodes: ListOfFamilyCode):
        super().familyCodes(familyCodes)
        print("Family Codes:")
        for familyCode in familyCodes:
            print("Account ID: %s, Family Code Str: %s" % (
                familyCode.accountID, familyCode.familyCodeStr))

    # ! [familyCodes]

    @iswrapper
    # ! [pnl]
    def pnl(self, reqId: int, dailyPnL: float,
            unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        print("Daily PnL. Req Id: ", reqId, ", daily PnL: ", dailyPnL,
              ", unrealizedPnL: ", unrealizedPnL, ", realizedPnL: ", realizedPnL)

    # ! [pnl]

    @iswrapper
    # ! [pnlsingle]
    def pnlSingle(self, reqId: int, pos: int, dailyPnL: float,
                  unrealizedPnL: float, realizedPnL: float, value: float):
        super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        print("Daily PnL Single. Req Id: ", reqId, ", pos: ", pos,
              ", daily PnL: ", dailyPnL, ", unrealizedPnL: ", unrealizedPnL,
              ", realizedPnL: ", realizedPnL, ", value: ", value)

    # ! [pnlsingle]

    def marketDataType_req(self):
        # ! [reqmarketdatatype]
        # Switch to live (1) frozen (2) delayed (3) delayed frozen (4).
        self.reqMarketDataType(MarketDataTypeEnum.DELAYED)
        # ! [reqmarketdatatype]

    @iswrapper
    # ! [marketdatatype]
    def marketDataType(self, reqId: TickerId, marketDataType: int):
        super().marketDataType(reqId, marketDataType)
        print("MarketDataType. ", reqId, "Type:", marketDataType)

    # ! [marketdatatype]

    @printWhenExecuting
    def tickDataOperations_req(self):
        # Requesting real time market data

        # ! [reqmktdata]
        self.reqMktData(1000, ContractSamples.USStockAtSmart(), "", False, False, [])
        self.reqMktData(1001, ContractSamples.StockComboContract(), "", True, False, [])
        # ! [reqmktdata]

        # ! [reqmktdata_snapshot]
        self.reqMktData(1002, ContractSamples.FutureComboContract(), "", False, False, [])
        # ! [reqmktdata_snapshot]

        # ! [regulatorysnapshot]
        # Each regulatory snapshot request incurs a 0.01 USD fee
        self.reqMktData(1003, ContractSamples.USStock(), "", False, True, [])
        # ! [regulatorysnapshot]

        # ! [reqmktdata_genticks]
        # Requesting RTVolume (Time & Sales), shortable and Fundamental Ratios generic ticks
        self.reqMktData(1004, ContractSamples.USStock(), "233,236,258", False, False, [])
        # ! [reqmktdata_genticks]

        # ! [reqmktdata_contractnews]
        # Without the API news subscription this will generate an "invalid tick type" error
        self.reqMktData(1005, ContractSamples.USStock(), "mdoff,292:BZ", False, False, [])
        self.reqMktData(1006, ContractSamples.USStock(), "mdoff,292:BT", False, False, [])
        self.reqMktData(1007, ContractSamples.USStock(), "mdoff,292:FLY", False, False, [])
        self.reqMktData(1008, ContractSamples.USStock(), "mdoff,292:MT", False, False, [])
        # ! [reqmktdata_contractnews]

        # ! [reqmktdata_broadtapenews]
        self.reqMktData(1009, ContractSamples.BTbroadtapeNewsFeed(),
                        "mdoff,292", False, False, [])
        self.reqMktData(1010, ContractSamples.BZbroadtapeNewsFeed(),
                        "mdoff,292", False, False, [])
        self.reqMktData(1011, ContractSamples.FLYbroadtapeNewsFeed(),
                        "mdoff,292", False, False, [])
        self.reqMktData(1012, ContractSamples.MTbroadtapeNewsFeed(),
                        "mdoff,292", False, False, [])
        # ! [reqmktdata_broadtapenews]

        # ! [reqoptiondatagenticks]
        # Requesting data for an option contract will return the greek values
        self.reqMktData(1013, ContractSamples.OptionWithLocalSymbol(), "", False, False, [])
        # ! [reqoptiondatagenticks]

        # ! [reqsmartcomponents]
        # Requests description of map of single letter exchange codes to full exchange names
        self.reqSmartComponents(1013, "a6")
        # ! [reqsmartcomponents]

        # ! [reqfuturesopeninterest]
        self.reqMktData(1014, ContractSamples.SimpleFuture(), "mdoff,588", False, False, [])
        # ! [reqfuturesopeninterest]

        # ! [reqmktdatapreopenbidask]
        self.reqMktData(1015, ContractSamples.SimpleFuture(), "", False, False, [])
        # ! [reqmktdatapreopenbidask]

        # ! [reqavgoptvolume]
        self.reqMktData(1016, ContractSamples.USStockAtSmart(), "mdoff,105", False, False, [])
        # ! [reqavgoptvolume]

    @printWhenExecuting
    def tickDataOperations_cancel(self):
        # Canceling the market data subscription
        # ! [cancelmktdata]
        self.cancelMktData(1000)
        self.cancelMktData(1001)
        self.cancelMktData(1002)
        self.cancelMktData(1003)
        # ! [cancelmktdata]

        self.cancelMktData(1004)
        self.cancelMktData(1005)
        self.cancelMktData(1006)
        self.cancelMktData(1007)
        self.cancelMktData(1008)
        self.cancelMktData(1009)
        self.cancelMktData(1010)
        self.cancelMktData(1011)
        self.cancelMktData(1012)
        self.cancelMktData(1013)
        self.cancelMktData(1014)
        self.cancelMktData(1015)
        self.cancelMktData(1016)

    @iswrapper
    # ! [ticksize]
    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
        print("Tick Size. Ticker Id:", reqId, "tickType:", tickType, "Size:", size)

    # ! [ticksize]

    @iswrapper
    # ! [tickgeneric]
    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
        print("Tick Generic. Ticker Id:", reqId, "tickType:", tickType, "Value:", value)

    # ! [tickgeneric]

    @iswrapper
    # ! [tickstring]
    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
        print("Tick string. Ticker Id:", reqId, "Type:", tickType, "Value:", value)

    # ! [tickstring]

    @iswrapper
    # ! [ticksnapshotend]
    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        print("TickSnapshotEnd:", reqId)

    # ! [ticksnapshotend]

    @iswrapper
    # ! [rerouteMktDataReq]
    def rerouteMktDataReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        print("Re-route market data request. Req Id: ", reqId,
              ", ConId: ", conId, " Exchange: ", exchange)

    # ! [rerouteMktDataReq]

    @iswrapper
    # ! [marketRule]
    def marketRule(self, marketRuleId: int, priceIncrements: ListOfPriceIncrements):
        super().marketRule(marketRuleId, priceIncrements)
        print("Market Rule ID: ", marketRuleId)
        for priceIncrement in priceIncrements:
            print("Price Increment. Low Edge: ", priceIncrement.lowEdge,
                  ", Increment: ", priceIncrement.increment)

    # ! [marketRule]

    @printWhenExecuting
    def tickByTickOperations(self):
        # Requesting tick-by-tick data (only refresh)
        # ! [reqtickbytick]
        self.reqTickByTickData(19001, ContractSamples.USStockAtSmart(), "Last", 0, True)
        self.reqTickByTickData(19002, ContractSamples.USStockAtSmart(), "AllLast", 0, False)
        self.reqTickByTickData(19003, ContractSamples.USStockAtSmart(), "BidAsk", 0, True)
        self.reqTickByTickData(19004, ContractSamples.USStockAtSmart(), "MidPoint", 0, False)
        # ! [reqtickbytick]

        time.sleep(1)

        # ! [canceltickbytick]
        self.cancelTickByTickData(19001)
        self.cancelTickByTickData(19002)
        self.cancelTickByTickData(19003)
        self.cancelTickByTickData(19004)
        # ! [canceltickbytick]

        # Requesting tick-by-tick data (refresh + historicalticks)
        # ! [reqtickbytickwithhist]
        self.reqTickByTickData(19001, ContractSamples.EuropeanStock(), "Last", 10, False)
        self.reqTickByTickData(19002, ContractSamples.EuropeanStock(), "AllLast", 10, False)
        self.reqTickByTickData(19003, ContractSamples.EuropeanStock(), "BidAsk", 10, False)
        self.reqTickByTickData(19004, ContractSamples.EurGbpFx(), "MidPoint", 10, True)
        # ! [reqtickbytickwithhist]

        time.sleep(1)

        # ! [canceltickbytickwithhist]
        self.cancelTickByTickData(19005)
        self.cancelTickByTickData(19006)
        self.cancelTickByTickData(19007)
        self.cancelTickByTickData(19008)
        # ! [canceltickbytickwithhist]

    @iswrapper
    # ! [tickbytickalllast]
    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float,
                          size: int, attribs: TickAttrib, exchange: str,
                          specialConditions: str):
        super().tickByTickAllLast(reqId, tickType, time, price, size, attribs,
                                  exchange, specialConditions)
        if tickType == 1:
            print("Last.", end='')
        else:
            print("AllLast.", end='')
        print(" ReqId: ", reqId,
              " Time: ", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"),
              " Price: ", price, " Size: ", size, " Exch: ", exchange,
              "Spec Cond: ", specialConditions, end='')
        if attribs.pastLimit:
            print(" pastLimit ", end='')
        if attribs.unreported:
            print(" unreported", end='')
        print()

    # ! [tickbytickalllast]

    @iswrapper
    # ! [tickbytickbidask]
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float,
                         bidSize: int, askSize: int, attribs: TickAttrib):
        super().tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize,
                                 askSize, attribs)
        print("BidAsk. Req Id: ", reqId,
              " Time: ", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"),
              " BidPrice: ", bidPrice, " AskPrice: ", askPrice, " BidSize: ", bidSize,
              " AskSize: ", askSize, end='')
        if attribs.bidPastLow:
            print(" bidPastLow", end='')
        if attribs.askPastHigh:
            print(" askPastHigh", end='')
        print()

    # ! [tickbytickbidask]

    # ! [tickbytickmidpoint]
    @iswrapper
    def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
        super().tickByTickMidPoint(reqId, time, midPoint)
        print("Midpoint. Req Id: ", reqId,
              " Time: ", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"),
              " MidPoint: ", midPoint)

    # ! [tickbytickmidpoint]

    @printWhenExecuting
    def marketDepthOperations_req(self):
        # Requesting the Deep Book
        # ! [reqmarketdepth]
        self.reqMktDepth(2101, ContractSamples.USStock(), 5, [])
        self.reqMktDepth(2001, ContractSamples.EurGbpFx(), 5, [])
        # ! [reqmarketdepth]

        # Request list of exchanges sending market depth to UpdateMktDepthL2()
        # ! [reqMktDepthExchanges]
        self.reqMktDepthExchanges()
        # ! [reqMktDepthExchanges]

    @iswrapper
    # ! [updatemktdepth]
    def updateMktDepth(self, reqId: TickerId, position: int, operation: int,
                       side: int, price: float, size: int):
        super().updateMktDepth(reqId, position, operation, side, price, size)
        print("UpdateMarketDepth. ", reqId, "Position:", position, "Operation:",
              operation, "Side:", side, "Price:", price, "Size", size)

    # ! [updatemktdepth]

    @iswrapper
    # ! [updatemktdepthl2]
    def updateMktDepthL2(self, reqId: TickerId, position: int, marketMaker: str,
                         operation: int, side: int, price: float, size: int):
        super().updateMktDepthL2(reqId, position, marketMaker, operation, side,
                                 price, size)
        print("UpdateMarketDepthL2. ", reqId, "Position:", position, "Operation:",
              operation, "Side:", side, "Price:", price, "Size", size)

    # ! [updatemktdepthl2]

    @iswrapper
    # ! [rerouteMktDepthReq]
    def rerouteMktDepthReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        print("Re-route market data request. Req Id: ", reqId,
              ", ConId: ", conId, " Exchange: ", exchange)

    # ! [rerouteMktDepthReq]

    @printWhenExecuting
    def marketDepthOperations_cancel(self):
        # Canceling the Deep Book request
        # ! [cancelmktdepth]
        self.cancelMktDepth(2101)
        self.cancelMktDepth(2001)
        # ! [cancelmktdepth]

    @printWhenExecuting
    def realTimeBars_req(self):
        # Requesting real time bars
        # ! [reqrealtimebars]
        self.reqRealTimeBars(3101, ContractSamples.USStockAtSmart(), 5, "MIDPOINT", True, [])
        self.reqRealTimeBars(3001, ContractSamples.EurGbpFx(), 5, "MIDPOINT", True, [])
        # ! [reqrealtimebars]

    @iswrapper
    # ! [realtimebar]
    def realtimeBar(self, reqId: TickerId, time: int, open: float, high: float,
                    low: float, close: float, volume: int, wap: float, count: int):
        super().realtimeBar(reqId, time, open, high, low, close, volume, wap, count)
        print("RealTimeBars. ", reqId, ": time ", time, ", open: ", open,
              ", high: ", high, ", low: ", low, ", close: ", close, ", volume: ", volume,
              ", wap: ", wap, ", count: ", count)

    # ! [realtimebar]

    @printWhenExecuting
    def realTimeBars_cancel(self):
        # Canceling real time bars
        # ! [cancelrealtimebars]
        self.cancelRealTimeBars(3101)
        self.cancelRealTimeBars(3001)
        # ! [cancelrealtimebars]

    @printWhenExecuting
    def historicalDataRequests_req(self):
        # Requesting historical data
        # ! [reqHeadTimeStamp]
        self.reqHeadTimeStamp(4103, ContractSamples.USStockAtSmart(), "TRADES", 0, 1)
        # ! [reqHeadTimeStamp]

        time.sleep(1)

        # ! [cancelHeadTimestamp]
        self.cancelHeadTimeStamp(4103)
        # ! [cancelHeadTimestamp]

        # ! [reqhistoricaldata]
        queryTime = (datetime.datetime.today() -
                     datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")
        self.reqHistoricalData(4101, ContractSamples.USStockAtSmart(), queryTime,
                               "1 M", "1 day", "MIDPOINT", 1, 1, False, [])
        self.reqHistoricalData(4001, ContractSamples.EurGbpFx(), queryTime,
                               "1 M", "1 day", "MIDPOINT", 1, 1, False, [])
        self.reqHistoricalData(4002, ContractSamples.EuropeanStock(), queryTime,
                               "10 D", "1 min", "TRADES", 1, 1, False, [])
        # ! [reqhistoricaldata]

        # ! [reqHistogramData]
        self.reqHistogramData(4104, ContractSamples.USStock(), False, "3 days")
        # ! [reqHistogramData]
        time.sleep(2)
        # ! [cancelHistogramData]
        self.cancelHistogramData(4104)
        # ! [cancelHistogramData]

    @printWhenExecuting
    def historicalDataRequests_cancel(self):
        # Canceling historical data requests
        self.cancelHistoricalData(4101)
        self.cancelHistoricalData(4001)
        self.cancelHistoricalData(4002)

    @printWhenExecuting
    def historicalTicksRequests_req(self):
        # ! [reqhistoricalticks]
        self.reqHistoricalTicks(18001, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "TRADES", 1, True, [])
        self.reqHistoricalTicks(18002, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "BID_ASK", 1, True, [])
        self.reqHistoricalTicks(18003, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "MIDPOINT", 1, True, [])
        # ! [reqhistoricalticks]

    @iswrapper
    # ! [headTimestamp]
    def headTimestamp(self, reqId: int, headTimestamp: str):
        print("HeadTimestamp: ", reqId, " ", headTimestamp)

    # ! [headTimestamp]

    @iswrapper
    # ! [histogramData]
    def histogramData(self, reqId: int, items: HistogramDataList):
        print("HistogramData: ", reqId, " ", items)

    # ! [histogramData]

    @iswrapper
    # ! [historicaldata]
    def historicalData(self, reqId: int, bar: BarData):
        print("HistoricalData. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)

    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd ", reqId, "from", start, "to", end)

    # ! [historicaldataend]

    @iswrapper
    # ! [historicalDataUpdate]
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ", reqId, " Date:", bar.date, "Open:", bar.open,
              "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
              "Count:", bar.barCount, "WAP:", bar.average)

    # ! [historicalDataUpdate]

    @iswrapper
    # ! [historicalticks]
    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        for tick in ticks:
            print("Historical Tick. Req Id: ", reqId, ", time: ", tick.time,
                  ", price: ", tick.price, ", size: ", tick.size)

    # ! [historicalticks]

    @iswrapper
    # ! [historicalticksbidask]
    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk,
                              done: bool):
        for tick in ticks:
            print("Historical Tick Bid/Ask. Req Id: ", reqId, ", time: ", tick.time,
                  ", bid price: ", tick.priceBid, ", ask price: ", tick.priceAsk,
                  ", bid size: ", tick.sizeBid, ", ask size: ", tick.sizeAsk)

    # ! [historicalticksbidask]

    @iswrapper
    # ! [historicaltickslast]
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        for tick in ticks:
            print("Historical Tick Last. Req Id: ", reqId, ", time: ", tick.time,
                  ", price: ", tick.price, ", size: ", tick.size, ", exchange: ", tick.exchange,
                  ", special conditions:", tick.specialConditions)

    # ! [historicaltickslast]

    @printWhenExecuting
    def optionsOperations_req(self):
        # ! [reqsecdefoptparams]
        self.reqSecDefOptParams(0, "IBM", "", "STK", 8314)
        # ! [reqsecdefoptparams]

        # Calculating implied volatility
        # ! [calculateimpliedvolatility]
        self.calculateImpliedVolatility(5001, ContractSamples.OptionAtBOX(), 5, 85, [])
        # ! [calculateimpliedvolatility]

        # Calculating option's price
        # ! [calculateoptionprice]
        self.calculateOptionPrice(5002, ContractSamples.OptionAtBOX(), 0.22, 85, [])
        # ! [calculateoptionprice]

        # Exercising options
        # ! [exercise_options]
        self.exerciseOptions(5003, ContractSamples.OptionWithTradingClass(), 1,
                             1, self.account, 1)
        # ! [exercise_options]

    @printWhenExecuting
    def optionsOperations_cancel(self):
        # Canceling implied volatility
        self.cancelCalculateImpliedVolatility(5001)
        # Canceling option's price calculation
        self.cancelCalculateOptionPrice(5002)

    @printWhenExecuting
    def contractOperations_req(self):
        # ! [reqcontractdetails]
        self.reqContractDetails(209, ContractSamples.EurGbpFx())
        self.reqContractDetails(210, ContractSamples.OptionForQuery())
        self.reqContractDetails(211, ContractSamples.Bond())
        self.reqContractDetails(212, ContractSamples.FuturesOnOptions())
        # ! [reqcontractdetails]

        # ! [reqmatchingsymbols]
        self.reqMatchingSymbols(212, "IB")
        # ! [reqmatchingsymbols]

    @printWhenExecuting
    def contractNewsFeed_req(self):
        # ! [reqcontractdetailsnews]
        self.reqContractDetails(213, ContractSamples.NewsFeedForQuery())
        # ! [reqcontractdetailsnews]

        # Returns list of subscribed news providers
        # ! [reqNewsProviders]
        self.reqNewsProviders()
        # ! [reqNewsProviders]

        # Returns body of news article given article ID
        # ! [reqNewsArticle]
        self.reqNewsArticle(214, "BZ", "BZ$04507322", [])
        # ! [reqNewsArticle]

        # Returns list of historical news headlines with IDs
        # ! [reqHistoricalNews]
        self.reqHistoricalNews(215, 8314, "BZ+FLY", "", "", 10, [])
        # ! [reqHistoricalNews]

    @iswrapper
    # ! [tickNews]
    def tickNews(self, tickerId: int, timeStamp: int, providerCode: str,
                 articleId: str, headline: str, extraData: str):
        print("tickNews: ", tickerId, ", timeStamp: ", timeStamp,
              ", providerCode: ", providerCode, ", articleId: ", articleId,
              ", headline: ", headline, "extraData: ", extraData)

    # ! [tickNews]

    @iswrapper
    # ! [historicalNews]
    def historicalNews(self, reqId: int, time: str, providerCode: str,
                       articleId: str, headline: str):
        print("historicalNews: ", reqId, ", time: ", time,
              ", providerCode: ", providerCode, ", articleId: ", articleId,
              ", headline: ", headline)

    # ! [historicalNews]

    @iswrapper
    # ! [historicalNewsEnd]
    def historicalNewsEnd(self, reqId: int, hasMore: bool):
        print("historicalNewsEnd: ", reqId, ", hasMore: ", hasMore)

    # ! [historicalNewsEnd]

    @iswrapper
    # ! [newsProviders]
    def newsProviders(self, newsProviders: ListOfNewsProviders):
        print("newsProviders: ")
        for provider in newsProviders:
            print(provider)

    # ! [newsProviders]

    @iswrapper
    # ! [newsArticle]
    def newsArticle(self, reqId: int, articleType: int, articleText: str):
        print("newsArticle: ", reqId, ", articleType: ", articleType,
              ", articleText: ", articleText)

    # ! [newsArticle]

    @iswrapper
    # ! [symbolSamples]
    def symbolSamples(self, reqId: int,
                      contractDescriptions: ListOfContractDescription):
        super().symbolSamples(reqId, contractDescriptions)
        print("Symbol Samples. Request Id: ", reqId)

        for contractDescription in contractDescriptions:
            derivSecTypes = ""
            for derivSecType in contractDescription.derivativeSecTypes:
                derivSecTypes += derivSecType
                derivSecTypes += " "
            print("Contract: conId:%s, symbol:%s, secType:%s primExchange:%s, "
                  "currency:%s, derivativeSecTypes:%s" % (
                      contractDescription.contract.conId,
                      contractDescription.contract.symbol,
                      contractDescription.contract.secType,
                      contractDescription.contract.primaryExchange,
                      contractDescription.contract.currency, derivSecTypes))

    # ! [symbolSamples]

    @printWhenExecuting
    def marketScanners_req(self):
        # Requesting list of valid scanner parameters which can be used in TWS
        # ! [reqscannerparameters]
        self.reqScannerParameters()
        # ! [reqscannerparameters]

        # Triggering a scanner subscription
        # ! [reqscannersubscription]
        self.reqScannerSubscription(7001,
                                    ScannerSubscriptionSamples.HighOptVolumePCRatioUSIndexes(), [])
        # ! [reqscannersubscription]

    @printWhenExecuting
    def marketScanners_cancel(self):
        # Canceling the scanner subscription
        # ! [cancelscannersubscription]
        self.cancelScannerSubscription(7001)
        # ! [cancelscannersubscription]

    @iswrapper
    # ! [scannerparameters]
    def scannerParameters(self, xml: str):
        super().scannerParameters(xml)
        open('log/scanner.xml', 'w').write(xml)

    # ! [scannerparameters]

    @iswrapper
    # ! [scannerdata]
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark,
                            projection, legsStr)
        print("ScannerData. ", reqId, "Rank:", rank, "Symbol:", contractDetails.contract.symbol,
              "SecType:", contractDetails.contract.secType,
              "Currency:", contractDetails.contract.currency,
              "Distance:", distance, "Benchmark:", benchmark,
              "Projection:", projection, "Legs String:", legsStr)

    # ! [scannerdata]

    @iswrapper
    # ! [scannerdataend]
    def scannerDataEnd(self, reqId: int):
        super().scannerDataEnd(reqId)
        print("ScannerDataEnd. ", reqId)
        # ! [scannerdataend]

    @iswrapper
    # ! [smartcomponents]
    def smartComponents(self, reqId: int, map: SmartComponentMap):
        super().smartComponents(reqId, map)
        print("smartComponents: ")
        for exch in map:
            print(exch.bitNumber, ", Exchange Name: ", exch.exchange,
                  ", Letter: ", exch.exchangeLetter)

    # ! [smartcomponents]

    @iswrapper
    # ! [tickReqParams]
    def tickReqParams(self, tickerId: int, minTick: float,
                      bboExchange: str, snapshotPermissions: int):
        super().tickReqParams(tickerId, minTick, bboExchange, snapshotPermissions)
        print("tickReqParams: ", tickerId, " minTick: ", minTick,
              " bboExchange: ", bboExchange, " snapshotPermissions: ", snapshotPermissions)

    # ! [tickReqParams]

    @iswrapper
    # ! [mktDepthExchanges]
    def mktDepthExchanges(self, depthMktDataDescriptions: ListOfDepthExchanges):
        super().mktDepthExchanges(depthMktDataDescriptions)
        print("mktDepthExchanges:")
        for desc in depthMktDataDescriptions:
            printinstance(desc)

    # ! [mktDepthExchanges]

    @printWhenExecuting
    def reutersFundamentals_req(self):
        # Requesting Fundamentals
        # ! [reqfundamentaldata]
        self.reqFundamentalData(8001, ContractSamples.USStock(),
                                "ReportsFinSummary", [])
        # ! [reqfundamentaldata]

    @printWhenExecuting
    def reutersFundamentals_cancel(self):
        # Canceling fundamentals request ***/
        # ! [cancelfundamentaldata]
        self.cancelFundamentalData(8001)
        # ! [cancelfundamentaldata]

    @iswrapper
    # ! [fundamentaldata]
    def fundamentalData(self, reqId: TickerId, data: str):
        super().fundamentalData(reqId, data)
        print("FundamentalData. ", reqId, data)

    # ! [fundamentaldata]

    @printWhenExecuting
    def bulletins_req(self):
        # Requesting Interactive Broker's news bulletins
        # ! [reqnewsbulletins]
        self.reqNewsBulletins(True)
        # ! [reqnewsbulletins]

    @printWhenExecuting
    def bulletins_cancel(self):
        # Canceling IB's news bulletins
        # ! [cancelnewsbulletins]
        self.cancelNewsBulletins()
        # ! [cancelnewsbulletins]

    @iswrapper
    # ! [updatenewsbulletin]
    def updateNewsBulletin(self, msgId: int, msgType: int, newsMessage: str,
                           originExch: str):
        super().updateNewsBulletin(msgId, msgType, newsMessage, originExch)
        print("News Bulletins. ", msgId, " Type: ", msgType, "Message:", newsMessage,
              "Exchange of Origin: ", originExch)
        # ! [updatenewsbulletin]

        self.bulletins_cancel()

    def ocaSample(self):
        # OCA ORDER
        # ! [ocasubmit]
        ocaOrders = [OrderSamples.LimitOrder("BUY", 1, 10), OrderSamples.LimitOrder("BUY", 1, 11),
                     OrderSamples.LimitOrder("BUY", 1, 12)]
        OrderSamples.OneCancelsAll("TestOCA_" + self.nextValidOrderId, ocaOrders, 2)
        for o in ocaOrders:
            self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), o)
            # ! [ocasubmit]

    def conditionSamples(self):
        # ! [order_conditioning_activate]
        mkt = OrderSamples.MarketOrder("BUY", 100)
        # Order will become active if conditioning criteria is met
        mkt.conditionsCancelOrder = True
        mkt.conditions.append(
            OrderSamples.PriceCondition(PriceCondition.TriggerMethodEnum.Default,
                                        208813720, "SMART", 600, False, False))
        mkt.conditions.append(OrderSamples.ExecutionCondition("EUR.USD", "CASH", "IDEALPRO", True))
        mkt.conditions.append(OrderSamples.MarginCondition(30, True, False))
        mkt.conditions.append(OrderSamples.PercentageChangeCondition(15.0, 208813720, "SMART", True, True))
        mkt.conditions.append(OrderSamples.TimeCondition("20160118 23:59:59", True, False))
        mkt.conditions.append(OrderSamples.VolumeCondition(208813720, "SMART", False, 100, True))
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), mkt)
        # ! [order_conditioning_activate]

        # Conditions can make the order active or cancel it. Only LMT orders can be conditionally canceled.
        # ! [order_conditioning_cancel]
        lmt = OrderSamples.LimitOrder("BUY", 100, 20)
        # The active order will be cancelled if conditioning criteria is met
        lmt.conditionsCancelOrder = True
        lmt.conditions.append(
            OrderSamples.PriceCondition(PriceCondition.TriggerMethodEnum.Last,
                                        208813720, "SMART", 600, False, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), lmt)
        # ! [order_conditioning_cancel]

    def bracketSample(self):
        # BRACKET ORDER
        # ! [bracketsubmit]
        bracket = OrderSamples.BracketOrder(self.nextOrderId(), "BUY", 100, 30, 40, 20)
        for o in bracket:
            self.placeOrder(o.orderId, ContractSamples.EuropeanStock(), o)
            self.nextOrderId()  # need to advance this we'll skip one extra oid, it's fine
            # ! [bracketsubmit]

    def hedgeSample(self):
        # F Hedge order
        # ! [hedgesubmit]
        # Parent order on a contract which currency differs from your base currency
        parent = OrderSamples.LimitOrder("BUY", 100, 10)
        parent.orderId = self.nextOrderId()
        # Hedge on the currency conversion
        hedge = OrderSamples.MarketFHedge(parent.orderId, "BUY")
        # Place the parent first...
        self.placeOrder(parent.orderId, ContractSamples.EuropeanStock(), parent)
        # Then the hedge order
        self.placeOrder(self.nextOrderId(), ContractSamples.EurGbpFx(), hedge)
        # ! [hedgesubmit]

    def testAlgoSamples(self):
        # ! [algo_base_order]
        baseOrder = OrderSamples.LimitOrder("BUY", 1000, 1)
        # ! [algo_base_order]

        # ! [arrivalpx]
        AvailableAlgoParams.FillArrivalPriceParams(baseOrder, 0.1,
                                                   "Aggressive", "09:00:00 CET", "16:00:00 CET", True, True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [arrivalpx]

        # ! [darkice]
        AvailableAlgoParams.FillDarkIceParams(baseOrder, 10,
                                              "09:00:00 CET", "16:00:00 CET", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [darkice]

        # ! [ad]
        # The Time Zone in "startTime" and "endTime" attributes is ignored and always defaulted to GMT
        AvailableAlgoParams.FillAccumulateDistributeParams(baseOrder, 10, 60,
                                                           True, True, 1, True, True,
                                                           "20161010-12:00:00 GMT", "20161010-16:00:00 GMT")
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [ad]

        # ! [twap]
        AvailableAlgoParams.FillTwapParams(baseOrder, "Marketable",
                                           "09:00:00 CET", "16:00:00 CET", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [twap]

        # ! [vwap]
        AvailableAlgoParams.FillVwapParams(baseOrder, 0.2,
                                           "09:00:00 CET", "16:00:00 CET", True, True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [vwap]

        # ! [balanceimpactrisk]
        AvailableAlgoParams.FillBalanceImpactRiskParams(baseOrder, 0.1,
                                                        "Aggressive", True)
        self.placeOrder(self.nextOrderId(), ContractSamples.USOptionContract(), baseOrder)
        # ! [balanceimpactrisk]

        # ! [minimpact]
        AvailableAlgoParams.FillMinImpactParams(baseOrder, 0.3)
        self.placeOrder(self.nextOrderId(), ContractSamples.USOptionContract(), baseOrder)
        # ! [minimpact]

        # ! [adaptive]
        AvailableAlgoParams.FillAdaptiveParams(baseOrder, "Normal")
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [adaptive]

        # ! [closepx]
        AvailableAlgoParams.FillClosePriceParams(baseOrder, 0.5, "Neutral",
                                                 "12:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [closepx]

        # ! [pctvol]
        AvailableAlgoParams.FillPctVolParams(baseOrder, 0.5,
                                             "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvol]

        # ! [pctvolpx]
        AvailableAlgoParams.FillPriceVariantPctVolParams(baseOrder,
                                                         0.1, 0.05, 0.01, 0.2, "12:00:00 EST", "14:00:00 EST", True,
                                                         100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvolpx]

        # ! [pctvolsz]
        AvailableAlgoParams.FillSizeVariantPctVolParams(baseOrder,
                                                        0.2, 0.4, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvolsz]

        # ! [pctvoltm]
        AvailableAlgoParams.FillTimeVariantPctVolParams(baseOrder,
                                                        0.2, 0.4, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvoltm]

        # ! [jeff_vwap_algo]
        AvailableAlgoParams.FillJefferiesVWAPParams(baseOrder,
                                                    "10:00:00 EST", "16:00:00 EST", 10, 10, "Exclude_Both",
                                                    130, 135, 1, 10, "Patience", False, "Midpoint")

        self.placeOrder(self.nextOrderId(), ContractSamples.JefferiesContract(), baseOrder)
        # ! [jeff_vwap_algo]

        # ! [csfb_inline_algo]
        AvailableAlgoParams.FillCSFBInlineParams(baseOrder,
                                                 "10:00:00 EST", "16:00:00 EST", "Patient",
                                                 10, 20, 100, "Default", False, 40, 100, 100, 35)

        self.placeOrder(self.nextOrderId(), ContractSamples.CSFBContract(), baseOrder)
        # ! [csfb_inline_algo]

    @printWhenExecuting
    def financialAdvisorOperations(self):
        # Requesting FA information ***/
        # ! [requestfaaliases]
        self.requestFA(FaDataTypeEnum.ALIASES)
        # ! [requestfaaliases]

        # ! [requestfagroups]
        self.requestFA(FaDataTypeEnum.GROUPS)
        # ! [requestfagroups]

        # ! [requestfaprofiles]
        self.requestFA(FaDataTypeEnum.PROFILES)
        # ! [requestfaprofiles]

        # Replacing FA information - Fill in with the appropriate XML string. ***/
        # ! [replacefaonegroup]
        self.replaceFA(FaDataTypeEnum.GROUPS, FaAllocationSamples.FaOneGroup)
        # ! [replacefaonegroup]

        # ! [replacefatwogroups]
        self.replaceFA(FaDataTypeEnum.GROUPS, FaAllocationSamples.FaTwoGroups)
        # ! [replacefatwogroups]

        # ! [replacefaoneprofile]
        self.replaceFA(FaDataTypeEnum.PROFILES, FaAllocationSamples.FaOneProfile)
        # ! [replacefaoneprofile]

        # ! [replacefatwoprofiles]
        self.replaceFA(FaDataTypeEnum.PROFILES, FaAllocationSamples.FaTwoProfiles)
        # ! [replacefatwoprofiles]

        # ! [reqSoftDollarTiers]
        self.reqSoftDollarTiers(14001)
        # ! [reqSoftDollarTiers]

    @iswrapper
    # ! [receivefa]
    def receiveFA(self, faData: FaDataType, cxml: str):
        super().receiveFA(faData, cxml)
        print("Receiving FA: ", faData)
        open('log/fa.xml', 'w').write(cxml)

    # ! [receivefa]

    @iswrapper
    # ! [softDollarTiers]
    def softDollarTiers(self, reqId: int, tiers: list):
        super().softDollarTiers(reqId, tiers)
        print("Soft Dollar Tiers:", tiers)

    # ! [softDollarTiers]

    @printWhenExecuting
    def miscelaneous_req(self):
        # Request TWS' current time ***/
        self.reqCurrentTime()
        # Setting TWS logging level  ***/
        self.setServerLogLevel(1)

    @printWhenExecuting
    def linkingOperations(self):
        # ! [querydisplaygroups]
        self.queryDisplayGroups(19001)
        # ! [querydisplaygroups]

        # ! [subscribetogroupevents]
        self.subscribeToGroupEvents(19002, 1)
        # ! [subscribetogroupevents]

        # ! [updatedisplaygroup]
        self.updateDisplayGroup(19002, "8314@SMART")
        # ! [updatedisplaygroup]

        # ! [subscribefromgroupevents]
        self.unsubscribeFromGroupEvents(19002)
        # ! [subscribefromgroupevents]

    @iswrapper
    # ! [displaygrouplist]
    def displayGroupList(self, reqId: int, groups: str):
        super().displayGroupList(reqId, groups)
        print("DisplayGroupList. Request: ", reqId, "Groups", groups)

    # ! [displaygrouplist]

    @iswrapper
    # ! [displaygroupupdated]
    def displayGroupUpdated(self, reqId: int, contractInfo: str):
        super().displayGroupUpdated(reqId, contractInfo)
        print("displayGroupUpdated. Request:", reqId, "ContractInfo:", contractInfo)

    # ! [displaygroupupdated]

    @printWhenExecuting
    def whatIfOrder_req(self):

        whatIfOrder = OrderSamples.LimitOrder("SELL", 5, 70)
        whatIfOrder.whatIf = True
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), whatIfOrder)

        time.sleep(2)

    @printWhenExecuting
    def orderOperations_req(self):
        # Requesting the next valid id ***/
        # ! [reqids]
        # The parameter is always ignored.
        self.reqIds(-1)
        # ! [reqids]

        # Requesting all open orders ***/
        # ! [reqallopenorders]
        self.reqAllOpenOrders()
        # ! [reqallopenorders]

        # Taking over orders to be submitted via TWS ***/
        # ! [reqautoopenorders]
        self.reqAutoOpenOrders(True)
        # ! [reqautoopenorders]

        # Requesting this API client's orders ***/
        # ! [reqopenorders]
        self.reqOpenOrders()
        # ! [reqopenorders]

        # Placing/modifying an order - remember to ALWAYS increment the
        # nextValidId after placing an order so it can be used for the next one!
        # Note if there are multiple clients connected to an account, the
        # order ID must also be greater than all order IDs returned for orders
        # to orderStatus and openOrder to this client.

        # ! [order_submission]
        self.simplePlaceOid = self.nextOrderId()
        self.placeOrder(self.simplePlaceOid, ContractSamples.USStock(),
                        OrderSamples.LimitOrder("SELL", 1, 50))
        # ! [order_submission]

        # ! [faorderoneaccount]
        faOrderOneAccount = OrderSamples.MarketOrder("BUY", 100)
        # Specify the Account Number directly
        faOrderOneAccount.account = "DU119915"
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), faOrderOneAccount)
        # ! [faorderoneaccount]

        # ! [faordergroupequalquantity]
        faOrderGroupEQ = OrderSamples.LimitOrder("SELL", 200, 2000)
        faOrderGroupEQ.faGroup = "Group_Equal_Quantity"
        faOrderGroupEQ.faMethod = "EqualQuantity"
        self.placeOrder(self.nextOrderId(), ContractSamples.SimpleFuture(), faOrderGroupEQ)
        # ! [faordergroupequalquantity]

        # ! [faordergrouppctchange]
        faOrderGroupPC = OrderSamples.MarketOrder("BUY", 0)
        # You should not specify any order quantity for PctChange allocation method
        faOrderGroupPC.faGroup = "Pct_Change"
        faOrderGroupPC.faMethod = "PctChange"
        faOrderGroupPC.faPercentage = "100"
        self.placeOrder(self.nextOrderId(), ContractSamples.EurGbpFx(), faOrderGroupPC)
        # ! [faordergrouppctchange]

        # ! [faorderprofile]
        faOrderProfile = OrderSamples.LimitOrder("BUY", 200, 100)
        faOrderProfile.faProfile = "Percent_60_40"
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), faOrderProfile)
        # ! [faorderprofile]

        # ! [modelorder]
        modelOrder = OrderSamples.LimitOrder("BUY", 200, 100)
        modelOrder.account = "DF12345"
        modelOrder.modelCode = "Technology"  # model for tech stocks first created in TWS
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), modelOrder)
        # ! [modelorder]

        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                        OrderSamples.Block("BUY", 50, 20))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                        OrderSamples.BoxTop("SELL", 10))
        self.placeOrder(self.nextOrderId(), ContractSamples.FutureComboContract(),
                        OrderSamples.ComboLimitOrder("SELL", 1, 1, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.StockComboContract(),
                        OrderSamples.ComboMarketOrder("BUY", 1, True))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionComboContract(),
                        OrderSamples.ComboMarketOrder("BUY", 1, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.StockComboContract(),
                        OrderSamples.LimitOrderForComboWithLegPrices("BUY", 1, [10, 5], True))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.Discretionary("SELL", 1, 45, 0.5))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                        OrderSamples.LimitIfTouched("BUY", 1, 30, 34))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.LimitOnClose("SELL", 1, 34))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.LimitOnOpen("BUY", 1, 35))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketIfTouched("BUY", 1, 30))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketOnClose("SELL", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketOnOpen("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketOrder("SELL", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketToLimit("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtIse(),
                        OrderSamples.MidpointMatch("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.MarketToLimit("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.Stop("SELL", 1, 34.4))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.StopLimit("BUY", 1, 35, 33))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.StopWithProtection("SELL", 1, 45))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.SweepToFill("BUY", 1, 35))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.TrailingStop("SELL", 1, 0.5, 30))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                        OrderSamples.TrailingStopLimit("BUY", 1, 2, 5, 50))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtIse(),
                        OrderSamples.Volatility("SELL", 1, 5, 2))

        self.bracketSample()

        self.conditionSamples()

        self.hedgeSample()

        # NOTE: the following orders are not supported for Paper Trading
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), OrderSamples.AtAuction("BUY", 100, 30.0))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionLimit("SELL", 10, 30.0, 2))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionPeggedToStock("BUY", 10, 30, 0.5))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionRelative("SELL", 10, 0.6))
        # self.placeOrder(self.nextOrderId(), ContractSamples.SimpleFuture(), OrderSamples.MarketWithProtection("BUY", 1))
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), OrderSamples.PassiveRelative("BUY", 1, 0.5))

        # 208813720 (GOOG)
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
        #    OrderSamples.PeggedToBenchmark("SELL", 100, 33, True, 0.1, 1, 208813720, "ISLAND", 750, 650, 800))

        # STOP ADJUSTABLE ORDERS
        # Order stpParent = OrderSamples.Stop("SELL", 100, 30)
        # stpParent.OrderId = self.nextOrderId()
        # self.placeOrder(stpParent.OrderId, ContractSamples.EuropeanStock(), stpParent)
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToStop(stpParent, 35, 32, 33))
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToStopLimit(stpParent, 35, 33, 32, 33))
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToTrail(stpParent, 35, 32, 32, 1, 0))

        # Order lmtParent = OrderSamples.LimitOrder("BUY", 100, 30)
        # lmtParent.OrderId = self.nextOrderId()
        # self.placeOrder(lmtParent.OrderId, ContractSamples.EuropeanStock(), lmtParent)
        # Attached TRAIL adjusted can only be attached to LMT parent orders.
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToTrailAmount(lmtParent, 34, 32, 33, 0.008))
        self.testAlgoSamples()

        # Cancel all orders for all accounts ***/
        # ! [reqglobalcancel]
        self.reqGlobalCancel()
        # ! [reqglobalcancel]

        # Request the day's executions ***/
        # ! [reqexecutions]
        self.reqExecutions(10001, ExecutionFilter())
        # ! [reqexecutions]

    def orderOperations_cancel(self):
        if self.simplePlaceOid is not None:
            # ! [cancelorder]
            self.cancelOrder(self.simplePlaceOid)
            # ! [cancelorder]

    def marketRuleOperations(self):
        self.reqContractDetails(17001, ContractSamples.USStock())
        self.reqContractDetails(17002, ContractSamples.Bond())

        time.sleep(1)

        # ! [reqmarketrule]
        self.reqMarketRule(26)
        self.reqMarketRule(240)
        # ! [reqmarketrule]

    @iswrapper
    # ! [execdetails]
    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        print("ExecDetails. ", reqId, contract.symbol, contract.secType, contract.currency,
              execution.execId, execution.orderId, execution.shares, execution.lastLiquidity)

    # ! [execdetails]

    @iswrapper
    # ! [execdetailsend]
    def execDetailsEnd(self, reqId: int):
        super().execDetailsEnd(reqId)
        print("ExecDetailsEnd. ", reqId)

    # ! [execdetailsend]

    @iswrapper
    # ! [commissionreport]
    def commissionReport(self, commissionReport: CommissionReport):
        super().commissionReport(commissionReport)
        print("CommissionReport. ", commissionReport.execId, commissionReport.commission,
              commissionReport.currency, commissionReport.realizedPNL)
        # ! [commissionreport]


# %%
SetupLogger()
logging.debug("now is %s", datetime.datetime.now())
logging.getLogger().setLevel(logging.INFO)
# %%
app = TestApp()  
app.gammascarping("ES", "GLOBEX", "FUT", 289128563)
# app.reqSecDefOptParams(app.nextOrderId(), "IBM", "", "STK", 8314)

# %%
from pyomo.opt import SolverFactory
from pyomo.core import Var
from pyomo.environ import *
from pyomo.contrib.simplemodel import *

import itertools
import math

from scipy.special import comb

df = pd.DataFrame().from_dict(app.optionchain, 'columns')
df.to_csv('C:\\ibop\\OC_ES20181031.csv',encoding='gbk',header=True,index=False)
df['duration'] = (pd.to_datetime(df.expirations.map(str)) - pd.to_datetime('20181028'))

df.duration = df.duration.map(lambda x: x.days)
df['price'] = df.optPrice
df = df.dropna(how='any', subset=['gamma', 'theta', 'delta', 'price', 'multiplier'])
rawdata = df[
    ['conId','delta', 'gamma', 'theta', 'right', 'duration', 'strikes', 'price', 'symbol','exchange']]

datalist = []
#%%
for i in range(-15, 16):
    mid_data = rawdata.copy(deep=True)
    mid_data['delta'] = mid_data['delta'] * i
    mid_data['gamma'] = mid_data['gamma'] * i
    mid_data['theta'] = mid_data['theta'] * i
    mid_data['parameter'] = i
    datalist.append(mid_data)

inputdata = pd.concat(datalist, axis=0)

og = inputdata.gamma.values
od = inputdata.delta.values
ot = inputdata.theta.values


def searchForAlpha(m):
    return sum((m.x[i] * og[i - 1] + 10 * m.x[i] * ot[i - 1] for i in
                m.I))  # -150*abs(3-sum((m.x[i] for i in m.I)))-10*abs(sum((m.x[i]*od[i-1]for i in m.I)))


def threelegs_up(m):
    return sum((m.x[i] for i in m.I)) <= 3


def threelegs_down(m):
    return sum((m.x[i] for i in m.I)) >= -3


def deltaBound_up(m):
    return sum((m.x[i] * od[i - 1] for i in m.I)) <= 0.1


def deltaBound_down(m):
    return sum((m.x[i] * od[i - 1] for i in m.I)) >= -0.1


def thetaBound_up(m):
    return sum((m.x[i] * ot[i - 1] for i in m.I)) <= 0


def thetaBound_down(m):
    return sum((m.x[i] * ot[i - 1] for i in m.I)) >= -5


# %%
BinaryIdx = {i for i in range(0, inputdata.shape[0])}

model = ConcreteModel()

model.I = Set(initialize=RangeSet(inputdata.shape[0]))

model.x = Var(model.I, domain=Binary)

model.TotalProfit = Objective(rule=searchForAlpha, sense=maximize)

model.legbound_up = Constraint(rule=threelegs_up)
model.legbound_down = Constraint(rule=threelegs_down)

model.deltabound_up = Constraint(rule=deltaBound_up)
model.deltabound_down = Constraint(rule=deltaBound_down)

model.thetabound_up = Constraint(rule=thetaBound_up)
model.thetabound_down = Constraint(rule=thetaBound_down)

solver = SolverFactory("cplex", executable='C:\\cplex\\CPLEX_Optimizer\\cplex\\bin\\x86_win32\\cplex.exe', tee=True)
# solver = SolverFactory("ipopt",executable='C:\\Ipopt\\bin\\ipopt.exe',solver_io = 'nl')
results = solver.solve(model, tee=True)

x_values = []
for i in range(0, inputdata.shape[0]):
    x_values.append(value(model.x[i + 1]))
inputdata['selected'] = x_values
re_df = inputdata[inputdata.selected > 0.9]
#%%
print(df[df.conId.isin(re_df.conId.values)][['delta', 'gamma', 'theta', 'expirations']])

print(re_df[[u'right', u'duration', u'strikes', u'price', u'parameter']])
portfolio_delta = re_df.delta.sum()
portfolio_gamma = re_df.gamma.sum()
portfolio_theta = re_df.theta.sum()
print('delta:' + str(portfolio_delta))
print('gamma:' + str(portfolio_gamma))
print('theta:' + str(portfolio_theta))
print('Alpha:' + str(abs(portfolio_gamma / portfolio_theta)))
print('单独option的Alpha：')
print((df.gamma/df.theta.abs()).sort_values(ascending=False)[:20])

#%%
contract = Contract()
contract.symbol = re_df.symbol.unique()[0]
contract.secType = "BAG"
contract.currency = "USD"
contract.exchange = re_df.exchange.unique()[0]   
contract.comboLegs = []

comboprice = 0

for arow in enumerate(re_df.iterrows()):
    acontract = arow[1][1]
	       
    newComboLeg = ComboLeg()
    newComboLeg.conId = acontract.conId
    newComboLeg.ratio = abs(acontract.parameter)
    newComboLeg.exchange = acontract.exchange
    if acontract.parameter>0:
        newComboLeg.action = 'BUY'
    else:
        newComboLeg.action = 'SELL'
		
    contract.comboLegs.append(newComboLeg)
    comboprice += acontract.parameter*acontract.price

order = Order()
order.action = 'BUY'
order.orderType = "LMT"
order.totalQuantity = 10
order.lmtPrice = float(format(comboprice, '0.1f'))
        
app.placeOrder(app.nextOrderId(), contract,order)
# %%
app.disconnect()
