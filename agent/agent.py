import backtrader as bt
import torch
import logging
from chronos import ChronosPipeline

logger = logging.getLogger(__name__)

class Agent(bt.Strategy):
    params = (
        ('lookback', 20),
        ('model_name', 'amazon/chronos-t5-small'),
    )
    
    def __init__(self):
        logger.info("Agent strategy initialized")
        # Load Chronos model with automatic device selection.
        # Uses GPU if available (backtesting), falls back to CPU (paper trading).
        self.forecaster = ChronosPipeline.from_pretrained(
            self.params.model_name,
            device_map="auto",
            torch_dtype=torch.float32  # Changed from torch_dtype
        )
        logger.info(f"Loaded Chronos model: {self.params.model_name}")

    def prenext(self):
        """Allow trading even when not all data feeds have data.

        With 12,000+ symbols, some feeds (e.g., equities on weekends) won't have data.
        This forwards prenext() to next() so we can still trade on feeds that DO have data.
        The len(d) < lookback check in next() safely skips feeds without enough bars.
        """
        self.next()

    def next(self):
        logger.info("Next method called")
        for d in self.datas:
            logger.info(f"Checking data")
            if len(d) < self.params.lookback:
                continue
                
            # Get historical prices for this data feed
            prices = list(d.close.get(size=self.params.lookback))
            context = torch.tensor(prices).unsqueeze(0)
            
            # Forecast next price
            forecast = self.forecaster.predict(context, prediction_length=1)
            predicted = forecast[0].mean().item()
            logger.info(f"Predicted: {predicted:.2f}, Current: {d.close[0]:.2f}")
            
            # Trade logic per data feed
            pos = self.getposition(d).size
            if not pos and predicted > d.close[0]:  # No position + bullish
                size = int(self.broker.getcash() * 0.15 / d.close[0])
                self.buy(data=d, size=size)
                logger.info(f"Bought: {size} of {d}")
            elif pos and predicted < d.close[0]:  # Has position + bearish
                self.close(data=d)
                logger.info(f"Sold position in {d}")
