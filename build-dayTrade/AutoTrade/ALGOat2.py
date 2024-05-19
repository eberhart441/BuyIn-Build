from Algo import ALGOdt3
import pandas as pd
import yfinance as yf
import threading
import queue
import logging

# Setting up logging with detailed formatting
logging.basicConfig(
    filename="algoLog.log",
    level=logging.INFO,
    format="%(asctime)s:%(levelname)s:%(message)s",
)

# Set the logging level for yfinance to WARNING or higher
yfinanceLogger = logging.getLogger("yfinance")
yfinanceLogger.setLevel(logging.WARNING)


class AutoTrade:
    repeat = True

    def __init__(self, path, progressQueue):
        self.settings = {"path": path, "progressQueue": progressQueue}

        self.ALGOat()

    def ALGOat(self):
        instructionFile = pd.read_csv(self.settings["path"])

        while self.repeat == True:
            for index, row in instructionFile.iterrows():
                if self.repeat == True:
                    ticker = row["ticker"]
                    dataSize = row["dataSize"]

                    print(f"PROCESSING: {ticker}")
                    averageResult = self.calcData(ticker, "2d", dataSize)

                    if averageResult.iloc[-1] > 1.0025:
                        averageResult = self.calcData(ticker, "3d", dataSize)
                        if averageResult.iloc[-1] > 1.0025:
                            averageResult = self.calcData(ticker, "4d", dataSize)
                            if averageResult.iloc[-1] > 1.0025:
                                logging.info(f"LONG BUY: {ticker}")
                                print(f"LONG BUY: {ticker}")
                                hold = True

                                while hold == True:
                                    if self.repeat == True:
                                        holdExceptions = 0

                                        averageResult = self.calcData(
                                            ticker, "2d", dataSize
                                        )
                                        if averageResult.iloc[-1] < 1.000:
                                            holdExceptions += 1

                                        averageResult = self.calcData(
                                            ticker, "3d", dataSize
                                        )
                                        if averageResult.iloc[-1] < 1.000:
                                            holdExceptions += 1

                                        averageResult = self.calcData(
                                            ticker, "4d", dataSize
                                        )
                                        if averageResult.iloc[-1] < 1.000:
                                            holdExceptions += 1

                                        if holdExceptions >= 2:
                                            logging.info(f"LONG SELL: {ticker}")
                                            print(f"LONG SELL: {ticker}")

                                            hold = False

                                    else:
                                        logging.info(
                                            f"POSITION CLOSED BY BREAKPOINT: {ticker}"
                                        )
                                        print(
                                            f"POSITION CLOSED BY BREAKPOINT: {ticker}"
                                        )

                                        hold = False

                    elif averageResult.iloc[-1] < 0.9975:
                        averageResult = self.calcData(ticker, "3d", dataSize)
                        if averageResult.iloc[-1] < 0.9975:
                            averageResult = self.calcData(ticker, "4d", dataSize)
                            if averageResult.iloc[-1] < 0.9975:
                                logging.info(f"SHORT BUY: {ticker}")
                                print(f"SHORT BUY: {ticker}")
                                hold = True

                                while hold == True:
                                    if self.repeat == True:
                                        holdExceptions = 0

                                        averageResult = self.calcData(
                                            ticker, "2d", dataSize
                                        )
                                        if averageResult.iloc[-1] > 1.000:
                                            holdExceptions += 1

                                        averageResult = self.calcData(
                                            ticker, "3d", dataSize
                                        )
                                        if averageResult.iloc[-1] > 1.000:
                                            holdExceptions += 1

                                        averageResult = self.calcData(
                                            ticker, "4d", dataSize
                                        )
                                        if averageResult.iloc[-1] > 1.000:
                                            holdExceptions += 1

                                        if holdExceptions >= 2:
                                            logging.info(f"SHORT SELL: {ticker}")
                                            print(f"SHORT SELL: {ticker}")

                                            hold = False

                                    else:
                                        logging.info(
                                            f"POSITION CLOSED BY BREAKPOINT: {ticker}"
                                        )
                                        print(
                                            f"POSITION CLOSED BY BREAKPOINT: {ticker}"
                                        )

                                        hold = False

                    else:
                        print(f"NO MATCH: {ticker}")
                        continue

        print("ALGOat TERMINATED")

    def calcData(self, ticker, duration, dataSize):
        successfulDownload = False
        attempts = 0
        maxAttempts = 10
        while not successfulDownload and attempts < maxAttempts:
            try:
                self.tickerData = yf.download(
                    ticker,
                    group_by="Ticker",
                    period=duration,
                    interval="5m",
                    prepost=False,
                    repair=True,
                ).reset_index()["Close"]
                if not self.tickerData.empty:
                    successfulDownload = True
            except Exception as e:
                print(f"ERROR: {e}")
                attempts += 1
        if not successfulDownload:
            print("FAILED TO DOWNLOAD DATA AFTER SEVERAL ATTEMPTS")

        multiplyFactor = 1 / self.tickerData.iloc[-1]
        tickerDataMultiplied = self.tickerData.mul(multiplyFactor)

        predictionLen = tickerDataMultiplied.shape[0] / 5

        def algoNode0():
            # Call algo
            algoSettings = ALGOdt3.Algo(
                int(dataSize),
                predictionLen,
                tickerDataMultiplied,
                self.settings["progressQueue"],
            )

            nodeResultQueue.put(algoSettings.startPool())

        nodeResultQueue = queue.Queue()

        algoNode0Thread = threading.Thread(target=algoNode0)
        algoNode0Thread.start()
        algoNode0Thread.join()

        averageResult = pd.DataFrame()
        for result in range(1):
            averageResult = pd.concat([averageResult, nodeResultQueue.get()], axis=1)

        return averageResult.mean(axis=1)
