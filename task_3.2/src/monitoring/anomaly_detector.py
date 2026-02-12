import numpy as np
import pandas as pd
from collections import defaultdict, deque

class TransactionAnomalyDetector:
    def __init__(self, window_size=60, z_threshold=3.0):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.history = defaultdict(lambda: deque(maxlen=window_size))
        self.status_stats = {}
        self.training_complete = False
        self.training_samples = 0
        self.training_needed = 50
        self.min_training_samples = 30
        self.consecutive_alerts = defaultdict(int)
        
        self.rules = {
            'failed_ratio_threshold': 0.20,
            'denied_ratio_threshold': 0.15,
            'reversed_ratio_threshold': 0.08,
            'failed_absolute_threshold': 25,
            'denied_absolute_threshold': 20,
            'reversed_absolute_threshold': 12,
            'sudden_drop_threshold': 0.50,
            'sudden_spike_threshold': 2.5,
            'zero_approved_threshold': True,
            'min_volume_threshold': 10,
            'spike_multiplier': 4.0,
            'critical_zscore': 5.0
        }
        
        self.alerts_history = []
    
    def fit_from_historical(self, df):
        if df.empty or len(df) < self.min_training_samples:
            return False
        
        for status in ['failed', 'denied', 'reversed', 'approved', 'total']:
            if status in df.columns:
                values = df[status].values
                non_zero = values[values > 0]
                
                if len(non_zero) >= 10:
                    mean_val = np.mean(non_zero)
                    std_val = max(np.std(non_zero), 2.0)
                    p95 = np.percentile(non_zero, 95)
                    p99 = np.percentile(non_zero, 99)
                else:
                    mean_val = max(np.mean(values), 5.0)
                    std_val = max(np.std(values), 2.0)
                    p95 = np.percentile(values, 95) if len(values) > 0 else mean_val * 2
                    p99 = np.percentile(values, 99) if len(values) > 0 else mean_val * 3
                
                self.status_stats[status] = {
                    'mean': float(mean_val),
                    'std': float(std_val),
                    'p95': float(p95),
                    'p99': float(p99)
                }
                
                self.history[status].extend(values[-self.window_size:])
        
        failed_p95 = self.status_stats.get('failed', {}).get('p95', 25)
        denied_p95 = self.status_stats.get('denied', {}).get('p95', 20)
        reversed_p95 = self.status_stats.get('reversed', {}).get('p95', 12)
        
        self.rules['failed_absolute_threshold'] = max(int(failed_p95), 20)
        self.rules['denied_absolute_threshold'] = max(int(denied_p95), 15)
        self.rules['reversed_absolute_threshold'] = max(int(reversed_p95), 10)
        
        self.training_complete = True
        return True
    
    def detect_anomalies(self, timestamp, status_counts):
        anomalies = []
        anomaly_score = 0
        
        for status in ['approved', 'failed', 'denied', 'reversed', 'total']:
            if status not in status_counts:
                status_counts[status] = 0
        
        total = status_counts.get('total', 0)
        
        if not self.training_complete or self.training_samples < self.training_needed:
            self.training_samples += 1
            
            for status in ['failed', 'denied', 'reversed', 'total']:
                if status in self.history:
                    self.history[status].append(status_counts.get(status, 0))
            
            return {
                'timestamp': timestamp,
                'status_counts': status_counts,
                'anomalies': [],
                'anomaly_score': 0,
                'recommendation': 'TRAINING',
                'should_alert': False
            }
        
        if total < self.rules['min_volume_threshold']:
            return {
                'timestamp': timestamp,
                'status_counts': status_counts,
                'anomalies': [],
                'anomaly_score': 0,
                'recommendation': 'LOW_VOLUME',
                'should_alert': False
            }
        
        failed_count = status_counts.get('failed', 0)
        denied_count = status_counts.get('denied', 0)
        reversed_count = status_counts.get('reversed', 0)
        
        failed_ratio = failed_count / total if total > 0 else 0
        denied_ratio = denied_count / total if total > 0 else 0
        reversed_ratio = reversed_count / total if total > 0 else 0
        
        if failed_count >= self.rules['failed_absolute_threshold'] and failed_ratio >= self.rules['failed_ratio_threshold']:
            severity = 'CRITICAL' if failed_count > self.status_stats.get('failed', {}).get('p95', 999) else 'WARNING'
            anomalies.append({
                'type': 'high_failed_volume',
                'severity': severity,
                'message': f'Failed: {failed_count} (threshold: {self.rules["failed_absolute_threshold"]}, rate: {failed_ratio:.1%})',
                'value': failed_count,
                'threshold': self.rules['failed_absolute_threshold'],
                'ratio': failed_ratio,
                'ratio_threshold': self.rules['failed_ratio_threshold']
            })
            anomaly_score += 35
            self.consecutive_alerts['failed'] += 1
        else:
            self.consecutive_alerts['failed'] = 0
        
        if denied_count >= self.rules['denied_absolute_threshold'] and denied_ratio >= self.rules['denied_ratio_threshold']:
            anomalies.append({
                'type': 'high_denied_volume',
                'severity': 'WARNING',
                'message': f'Denied: {denied_count} (threshold: {self.rules["denied_absolute_threshold"]}, rate: {denied_ratio:.1%})',
                'value': denied_count,
                'threshold': self.rules['denied_absolute_threshold'],
                'ratio': denied_ratio,
                'ratio_threshold': self.rules['denied_ratio_threshold']
            })
            anomaly_score += 25
            self.consecutive_alerts['denied'] += 1
        else:
            self.consecutive_alerts['denied'] = 0
        
        if reversed_count >= self.rules['reversed_absolute_threshold'] and reversed_ratio >= self.rules['reversed_ratio_threshold']:
            severity = 'CRITICAL' if reversed_count > self.status_stats.get('reversed', {}).get('p95', 999) else 'WARNING'
            anomalies.append({
                'type': 'high_reversed_volume',
                'severity': severity,
                'message': f'Reversed: {reversed_count} (threshold: {self.rules["reversed_absolute_threshold"]}, rate: {reversed_ratio:.1%})',
                'value': reversed_count,
                'threshold': self.rules['reversed_absolute_threshold'],
                'ratio': reversed_ratio,
                'ratio_threshold': self.rules['reversed_ratio_threshold']
            })
            anomaly_score += 40
            self.consecutive_alerts['reversed'] += 1
        else:
            self.consecutive_alerts['reversed'] = 0
        
        for status in ['failed', 'denied', 'reversed']:
            if status in self.status_stats and status in status_counts:
                value = status_counts[status]
                mean = self.status_stats[status].get('mean', 10)
                std = self.status_stats[status].get('std', 5)
                
                if std > 2.0 and mean > 5 and value > mean * 1.5:
                    z_score = (value - mean) / std
                    
                    if z_score > self.rules['critical_zscore']:
                        anomalies.append({
                            'type': f'{status}_severe_spike',
                            'severity': 'CRITICAL',
                            'message': f'Critical {status} spike: {value} (z-score: {z_score:.2f})',
                            'value': value,
                            'z_score': z_score
                        })
                        anomaly_score += 50
                    elif z_score > self.z_threshold:
                        anomalies.append({
                            'type': f'{status}_spike',
                            'severity': 'WARNING',
                            'message': f'{status} spike: {value} (z-score: {z_score:.2f})',
                            'value': value,
                            'z_score': z_score
                        })
                        anomaly_score += 30
        
        if len(self.history['total']) > 0:
            prev_total = self.history['total'][-1] if self.history['total'] else total
            if prev_total > 0:
                change_pct = (total - prev_total) / prev_total
                
                if change_pct > self.rules['sudden_spike_threshold']:
                    anomalies.append({
                        'type': 'sudden_volume_spike',
                        'severity': 'CRITICAL',
                        'message': f'Sudden volume spike: {change_pct:.1%} increase',
                        'value': total,
                        'previous': prev_total,
                        'change_pct': change_pct
                    })
                    anomaly_score += 45
                
                elif change_pct < -self.rules['sudden_drop_threshold']:
                    anomalies.append({
                        'type': 'sudden_volume_drop',
                        'severity': 'WARNING',
                        'message': f'Sudden volume drop: {abs(change_pct):.1%} decrease',
                        'value': total,
                        'previous': prev_total,
                        'change_pct': change_pct
                    })
                    anomaly_score += 30
        
        for status in ['failed', 'denied', 'reversed', 'total']:
            if status in self.history:
                self.history[status].append(status_counts.get(status, 0))
        
        consecutive_threshold = 3
        for key, count in self.consecutive_alerts.items():
            if count >= consecutive_threshold:
                anomalies.append({
                    'type': f'consecutive_{key}_alerts',
                    'severity': 'CRITICAL',
                    'message': f'Consecutive {key} alerts: {count} in a row',
                    'count': count,
                    'threshold': consecutive_threshold
                })
                anomaly_score += 50
        
        anomaly_score = min(100, anomaly_score)
        
        if anomaly_score >= 70:
            recommendation = 'IMMEDIATE_ACTION'
        elif anomaly_score >= 50:
            recommendation = 'INVESTIGATE'
        elif anomaly_score >= 30:
            recommendation = 'MONITOR'
        else:
            recommendation = 'NORMAL'
        
        should_alert = len(anomalies) > 0 and anomaly_score >= 40
        
        result = {
            'timestamp': timestamp,
            'status_counts': dict(status_counts),
            'anomalies': anomalies,
            'anomaly_score': round(anomaly_score, 2),
            'recommendation': recommendation,
            'should_alert': should_alert
        }
        
        if should_alert:
            self.alerts_history.append(result)
            if len(self.alerts_history) > 1000:
                self.alerts_history = self.alerts_history[-1000:]
        
        return result
    
    def get_stats(self):
        return {
            'mean': self.status_stats.get('total', {}).get('mean', 0),
            'std': self.status_stats.get('total', {}).get('std', 0),
            'history_size': len(self.history['total']) if 'total' in self.history else 0,
            'alerts_count': len(self.alerts_history),
            'training_complete': self.training_complete,
            'training_samples': self.training_samples,
            'thresholds': {
                'failed': self.rules['failed_absolute_threshold'],
                'denied': self.rules['denied_absolute_threshold'],
                'reversed': self.rules['reversed_absolute_threshold'],
                'failed_ratio': self.rules['failed_ratio_threshold'],
                'denied_ratio': self.rules['denied_ratio_threshold'],
                'reversed_ratio': self.rules['reversed_ratio_threshold'],
                'z_threshold': self.z_threshold,
                'critical_zscore': self.rules['critical_zscore']
            }
        }