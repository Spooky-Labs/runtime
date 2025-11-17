# fmel_observer.py
import backtrader as bt
import json
import logging
from datetime import datetime
from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)

class FMELObserver(bt.Observer):
    """
    Foundation Model Explainability Layer Observer
    
    Records every data exposure and agent response across all lifecycle methods.
    Captures complete state regardless of indicator readiness.
    """
    
    lines = ('action',)
    
    params = (
        ('agent_id', None),
        ('project_id', None), 
        ('topic_name', 'fmel-decisions'),
    )
    
    def __init__(self):
        logger.info("Observer init method called")

        self.publisher = None
        if self.p.project_id:
            try:
                self.publisher = pubsub_v1.PublisherClient()
                self.topic_path = self.publisher.topic_path(
                    self.p.project_id, 
                    self.p.topic_name
                )
            except Exception as e:
                logger.error(f"FMEL: Pub/Sub init failed: {e}")
        
        self.bar_count = 0
        self.session_id = f"{self.p.agent_id}_{int(datetime.utcnow().timestamp())}"
        
    def prenext(self):
        """Record pre-strategy observations"""
        logger.info("Observer prenext method called")
        self.bar_count += 1
        # self._record_observation('PRENEXT')
    
    def nextstart(self):
        """Record first tradeable observation"""
        logger.info("Observer nextstart method called")
        # self._record_observation('NEXTSTART')
        # self.next()  # Continue to next() processing
    
    def next(self):
        """Record standard observations"""
        logger.info("Observer next method called")
        if self.bar_count > 0:  # Skip if nextstart already incremented
            self.bar_count += 1
        # self._record_observation('NEXT')
    
    def _record_observation(self, stage):
        """Capture and publish complete state snapshot"""
        logger.info("Observer _record_observation method called")
        strategy = self._owner
        
        # Capture all data feeds - same structure in prenext/next
        data_state = {}
        for data in strategy.datas:
            feed_state = {}
            
            # Capture every line in the feed
            for line_name in data.lines.getlinealiases():
                if line_name == 'datetime':
                    continue
                    
                line = getattr(data.lines, line_name)
                if len(line) > 0:
                    feed_state[line_name] = float(line[0])
                else:
                    feed_state[line_name] = None
                    
            data_state[data._name] = feed_state
        
        # Capture indicators - may be invalid but still have values
        indicators = {}
        for data in strategy.datas:
            for attr_name in dir(data):
                attr = getattr(data, attr_name)
                if isinstance(attr, bt.Indicator) and len(attr) > 0:
                    indicators[f"{data._name}_{attr_name}"] = float(attr[0])
        
        # Capture portfolio state
        portfolio = {
            'cash': strategy.broker.getcash(),
            'value': strategy.broker.getvalue(),
            'positions': {
                d._name: strategy.getposition(d).size 
                for d in strategy.datas
                if strategy.getposition(d).size != 0
            }
        }
        
        # Capture executed orders (only possible in NEXT/NEXTSTART)
        orders = []
        action = 'INACTIVE' if stage == 'PRENEXT' else 'HOLD'
        
        if stage != 'PRENEXT':
            current_dt = strategy.datetime[0]
            for order in strategy._orderspending:
                if order.executed.size > 0 and order.executed.dt == current_dt:
                    orders.append({
                        'symbol': order.data._name,
                        'side': 'BUY' if order.isbuy() else 'SELL',
                        'size': order.executed.size,
                        'price': order.executed.price,
                    })
            
            # Determine action from orders
            if orders:
                buys = sum(o['size'] for o in orders if o['side'] == 'BUY')
                sells = sum(o['size'] for o in orders if o['side'] == 'SELL')
                if buys > sells:
                    action = 'BUY'
                elif sells > buys:
                    action = 'SELL'
                else:
                    action = 'HOLD'
        
        # Update line
        action_map = {'INACTIVE': -2, 'SELL': -1, 'HOLD': 0, 'BUY': 1}
        self.lines.action[0] = action_map.get(action, 0)
        
        # Build observation
        observation = {
            'session_id': self.session_id,
            'agent_id': self.p.agent_id,
            'bar': self.bar_count,
            'stage': stage,
            'timestamp': self._get_timestamp(),
            'data': data_state,
            'indicators': indicators,
            'portfolio': portfolio,
            'orders': orders,
            'action': action
        }
        
        # Publish
        self._publish(observation)
    
    def _get_timestamp(self):
        """Extract bar timestamp or use current time"""
        logger.info("Observer _get_timestamp method called")
        try:
            return self.data.datetime.datetime(0).isoformat()
        except:
            return datetime.utcnow().isoformat()
    
    def _publish(self, observation):
        """Publish to Pub/Sub"""
        logger.info("Observer _publish method called")
        # if not self.publisher:
        #     return
            
        # try:
        #     message = json.dumps(observation).encode('utf-8')
        #     self.publisher.publish(
        #         self.topic_path,
        #         message,
        #         **{k: str(v) for k, v in {
        #             'session_id': observation['session_id'],
        #             'agent_id': observation['agent_id'],
        #             'stage': observation['stage'],
        #             'action': observation['action'],
        #             'bar': observation['bar']
        #         }.items()}
        #     )
        # except Exception as e:
        #     logger.error(f"FMEL: Publish failed: {e}")