import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSystem:
    def __init__(self):
        self.alert_history = []
    
    def log_alert_to_file(self, alert_data):
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'alert_timestamp': alert_data.get('timestamp'),
            'score': alert_data.get('anomaly_score'),
            'recommendation': alert_data.get('recommendation'),
            'status_counts': alert_data.get('status_counts'),
            'anomalies': alert_data.get('anomalies', [])
        }
        
        os.makedirs('outputs/alerts', exist_ok=True)
        date_str = datetime.now().strftime('%Y-%m-%d')
        filename = f'outputs/alerts/alerts_{date_str}.json'
        
        with open(filename, 'a') as f:
            f.write(json.dumps(log_entry) + '\n')
    
    def process_alert(self, alert_data):
        self.log_alert_to_file(alert_data)
        
        self.alert_history.append({
            'timestamp': datetime.now().isoformat(),
            'alert': alert_data
        })
        
        if len(self.alert_history) > 1000:
            self.alert_history = self.alert_history[-1000:]

alert_system = AlertSystem()