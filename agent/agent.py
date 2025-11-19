"""
Example Trading Agent using Amazon Chronos Time Series Forecasting

This is a PLACEHOLDER agent demonstrating the trading strategy interface.
Users will replace this with their own trading logic when they deploy agents.

The agent:
1. Uses Chronos T5-Small model to forecast the next price
2. Buys when predicted price > current price (bullish)
3. Sells when predicted price < current price (bearish)
4. Allocates 15% of cash per position

This is intentionally simple - a production agent would include:
- Risk management (stop losses, position limits)
- Multiple indicators and signals
- More sophisticated position sizing
- Proper handling of partial fills
- Error recovery logic

Note: Chronos requires significant memory. The base container pre-downloads
the model to speed up startup time.
"""

import backtrader as bt
import torch
import logging
from chronos import ChronosPipeline

logger = logging.getLogger(__name__)


class Agent(bt.Strategy):
    """
    Example trading strategy using Chronos time series forecasting.

    This strategy demonstrates the basic pattern for all trading agents:
    1. Initialize any models/indicators in __init__
    2. Make trading decisions in next() based on current data
    3. Use self.buy()/self.sell()/self.close() for order execution

    The FMEL analyzer will automatically track all data access and decisions.
    """

    params = (
        ('lookback', 20),  # Number of historical bars for prediction
        ('model_name', 'amazon/chronos-t5-small'),  # Chronos model variant
    )

    def __init__(self):
        """
        Initialize the forecasting model.

        Called once when the strategy is instantiated.
        Heavy operations (loading models) should happen here, not in next().
        """
        logger.info("Initializing Chronos forecasting agent")

        # Load Chronos model. Using CPU for paper trading - GPU would be overkill.
        # dtype=torch.float32 for CPU compatibility (not torch_dtype parameter).
        self.forecaster = ChronosPipeline.from_pretrained(
            self.params.model_name,
            device_map="cpu",
            dtype=torch.float32
        )
        logger.info(f"Loaded model: {self.params.model_name}")

    def next(self):
        """
        Called for each new bar (market data tick).

        This is where trading decisions are made. For each data feed:
        1. Get historical prices
        2. Forecast next price
        3. Execute trades based on prediction
        """
        for data in self.datas:
            # Wait until we have enough history for prediction.
            if len(data) < self.params.lookback:
                continue

            # Get historical prices for this symbol.
            # This access is tracked by FMEL for explainability.
            prices = list(data.close.get(size=self.params.lookback))
            context = torch.tensor(prices).unsqueeze(0)

            # Forecast next price using Chronos.
            forecast = self.forecaster.predict(context, prediction_length=1)
            predicted = forecast[0].mean().item()
            current = data.close[0]

            # Simple trading logic:
            # - Buy if we predict price will go up and we have no position
            # - Sell if we predict price will go down and we have a position
            position = self.getposition(data).size

            if not position and predicted > current:
                # Bullish signal - buy 15% of available cash worth.
                size = int(self.broker.getcash() * 0.15 / current)
                if size > 0:
                    self.buy(data=data, size=size)
                    logger.info(f"BUY {data._name}: {size} units @ ${current:.2f}")

            elif position > 0 and predicted < current:
                # Bearish signal - close position.
                self.close(data=data)
                logger.info(f"SELL {data._name}: closing position @ ${current:.2f}")
