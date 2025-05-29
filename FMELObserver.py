# fmel_observer.py
import backtrader as bt
import json
from google.cloud import pubsub_v1

class FMELObserver(bt.Observer):
    """
    Foundation Model Explainability Layer Observer
    
    Records trading decisions with market context for explainability.
    Follows Backtrader's observer lifecycle: prenext -> nextstart -> next.
    """
    
    lines = ('action',)  # -1: NET_SELL, 0: HOLD, 1: NET_BUY
    
    params = (
        ('agent_id', None),
        ('project_id', None), 
        ('topic_name', 'fmel-decisions'),
    )
    
    def __init__(self):
        # Pub/Sub setup
        self.publisher = None
        if self.p.project_id:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.topic_path = self.publisher.topic_path(
                    self.p.project_id, 
                    self.p.topic_name
                )
            except Exception as e:
                print(f"FMEL: Pub/Sub init failed: {e}")
    
    def prenext(self):
        """
        Called before minimum period is met.
        DO NOTHING - wait for all data feeds to be ready.
        This prevents IndexError when feeds are out of sync.
        """
        pass
    
    def nextstart(self):
        """
        Called once when minimum period is first met.
        Default behavior calls next() - let it happen.
        """
        self.next()
    
    def next(self):
        """Process bar after strategy execution."""
        strategy = self._owner
        
        # Get executed orders
        executed_orders = self._get_executed_orders(strategy)
        
        # Classify action
        action, net_change = self._classify_action(executed_orders)
        self.lines.action[0] = {'NET_SELL': -1, 'HOLD': 0, 'NET_BUY': 1}[action]
        
        # Build decision record
        decision = self._build_decision_record(
            strategy, executed_orders, action, net_change
        )
        
        # Publish if orders executed
        if executed_orders and self.publisher and decision:
            self._publish_decision(decision)
    
    def _get_executed_orders(self, strategy):
        """Extract executed orders."""
        executed = []
        
        for order in strategy._orderspending:
            if order.executed.size > 0:
                executed.append({
                    'symbol': order.data._name,
                    'action': 'BUY' if order.isbuy() else 'SELL',
                    'size': order.executed.size,
                    'price': order.executed.price,
                })
        
        return executed
    
    def _classify_action(self, executed_orders):
        """Determine net action from orders."""
        if not executed_orders:
            return 'HOLD', 0
        
        total_bought = sum(o['size'] for o in executed_orders if o['action'] == 'BUY')
        total_sold = sum(o['size'] for o in executed_orders if o['action'] == 'SELL')
        net_change = total_bought - total_sold
        
        if net_change > 0:
            return 'NET_BUY', net_change
        elif net_change < 0:
            return 'NET_SELL', net_change
        else:
            return 'HOLD', 0
    
    def _capture_market_data(self, strategy):
        """Safely extract market data from all feeds."""
        market_data = {}
        
        for data in strategy.datas:
            symbol = data._name
            snapshot = {}
            
            # Check line length before accessing
            if len(data.close):
                snapshot['close'] = float(data.close[0])
            if len(data.close) > 1:
                snapshot['prev_close'] = float(data.close[-1])
            
            # Other price lines might have different lengths
            if hasattr(data, 'open') and len(data.open):
                snapshot['open'] = float(data.open[0])
            if hasattr(data, 'high') and len(data.high):
                snapshot['high'] = float(data.high[0])
            if hasattr(data, 'low') and len(data.low):
                snapshot['low'] = float(data.low[0])
            if hasattr(data, 'volume') and len(data.volume):
                snapshot['volume'] = float(data.volume[0])
            
            market_data[symbol] = snapshot
        
        return market_data
    
    def _extract_indicators(self, strategy):
        """Extract indicator values from data feeds."""
        indicators = {}
        
        for data in strategy.datas:
            # Check common indicator names
            for ind_name in ['sma', 'ema', 'fast_ma', 'slow_ma', 'crossover']:
                if hasattr(data, ind_name):
                    ind = getattr(data, ind_name)
                    if isinstance(ind, bt.Indicator) and len(ind):
                        indicators[f"{data._name}_{ind_name}"] = float(ind[0])
        
        return indicators
    
    def _build_decision_record(self, strategy, executed_orders, action, net_change):
        """Assemble decision context."""
        # Safe timestamp
        try:
            timestamp = self.data.datetime.datetime(0).isoformat()
        except:
            from datetime import datetime
            timestamp = datetime.utcnow().isoformat()
        
        return {
            'timestamp': timestamp,
            'agent_id': self.p.agent_id,
            
            # Market context
            'market_data': self._capture_market_data(strategy),
            'indicators': self._extract_indicators(strategy),
            
            # Portfolio state
            'positions': {
                d._name: strategy.getposition(d).size 
                for d in strategy.datas
            },
            'portfolio': {
                'cash': strategy.broker.getcash(),
                'value': strategy.broker.getvalue()
            },
            
            # Trading actions
            'orders': executed_orders,
            'net_action': action,
            'net_position_change': net_change
        }
    
    def _publish_decision(self, decision):
        """Publish to Pub/Sub."""
        try:
            message = json.dumps(decision).encode('utf-8')
            self.publisher.publish(
                self.topic_path,
                message,
                agent_id=self.p.agent_id or 'unknown',
                action=decision['net_action']
            )
        except:
            pass  # Silent failure