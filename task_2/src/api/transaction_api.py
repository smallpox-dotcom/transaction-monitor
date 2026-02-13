# src/api/transaction_api.py
from flask import Flask, request, jsonify
import sqlite3
from datetime import datetime
import threading
import queue
import pandas as pd
import numpy as np
import os
import sys
import json
import time
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from monitoring.anomaly_detector import TransactionAnomalyDetector
from monitoring.alert_system import AlertSystem, alert_system

app = Flask(__name__)

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

detector = None
alert_queue = queue.Queue()
alerts_history = []
minute_buffer = {}
alert_callbacks = []

ALERT_HISTORY_LIMIT = 1000
MINUTE_BUFFER_LIMIT = 120
ALERT_WORKER_SLEEP = 0.1

VALID_STATUSES = ['approved', 'failed', 'denied', 'reversed']

# Initialize alert system
alert_system = AlertSystem()

def get_database_path():
    possible_paths = [
        "data/processed/transactions.db",
        "../data/processed/transactions.db"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    os.makedirs("data/processed", exist_ok=True)
    return "data/processed/transactions.db"

def load_historical_data():
    db_path = get_database_path()
    
    if not os.path.exists(db_path):
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if 'transactions' in tables:
            cursor.execute("PRAGMA table_info(transactions)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'status' in columns and 'count' in columns:
                query = """
                    SELECT 
                        timestamp,
                        SUM(CASE WHEN status = 'failed' THEN count ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'denied' THEN count ELSE 0 END) as denied,
                        SUM(CASE WHEN status = 'reversed' THEN count ELSE 0 END) as reversed,
                        SUM(CASE WHEN status = 'approved' THEN count ELSE 0 END) as approved,
                        SUM(count) as total
                    FROM transactions
                    GROUP BY timestamp
                    ORDER BY timestamp
                """
                df = pd.read_sql_query(query, conn)
                conn.close()
                return df
        
        conn.close()
        return None
            
    except Exception:
        return None

def create_synthetic_training_data():
    end_time = datetime.now()
    timestamps = pd.date_range(end=end_time, periods=100, freq='1min')
    
    data = []
    for ts in timestamps:
        approved = max(0, int(np.random.normal(95, 15)))
        failed = max(0, int(np.random.normal(12, 5)))
        denied = max(0, int(np.random.normal(7, 3)))
        reversed_tx = max(0, int(np.random.normal(3, 2)))
        
        data.append({
            'timestamp': ts,
            'approved': approved,
            'failed': failed,
            'denied': denied,
            'reversed': reversed_tx,
            'total': approved + failed + denied + reversed_tx
        })
    
    return pd.DataFrame(data)

def alert_worker():
    while True:
        try:
            alert_data = alert_queue.get(timeout=ALERT_WORKER_SLEEP)
            
            if 'timestamp' not in alert_data:
                alert_data['timestamp'] = datetime.now().isoformat()
            
            alert_system.process_alert(alert_data)
            alerts_history.append(alert_data)
            
            if len(alerts_history) > ALERT_HISTORY_LIMIT:
                alerts_history.pop(0)
            
            alert_queue.task_done()
            
        except queue.Empty:
            continue
        except Exception:
            continue

def initialize_detector():
    global detector
    
    detector = TransactionAnomalyDetector(window_size=60, z_threshold=3.0)
    
    historical_df = load_historical_data()
    
    if historical_df is not None and not historical_df.empty:
        detector.fit_from_historical(historical_df)
    else:
        synthetic_df = create_synthetic_training_data()
        detector.fit_from_historical(synthetic_df)
    
    return True

def get_minute_key(timestamp):
    try:
        dt = pd.to_datetime(timestamp)
        return dt.floor('1min').strftime('%Y-%m-%d %H:%M:%S')
    except:
        return str(timestamp)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'detector_initialized': detector is not None,
        'alerts_pending': alert_queue.qsize(),
        'alerts_history': len(alerts_history),
        'minutes_in_buffer': len(minute_buffer)
    })

@app.route('/api/transaction', methods=['POST'])
def receive_transaction():
    global minute_buffer, detector
    
    try:
        if detector is None:
            return jsonify({'error': 'Detector not initialized'}), 503
        
        data = request.json
        
        if not data:
            return jsonify({'error': 'No data provided'}), 400
        
        timestamp = data.get('timestamp')
        status = data.get('status', '').lower()
        
        if not timestamp or not status:
            return jsonify({'error': 'Missing timestamp or status'}), 400
        
        if status not in VALID_STATUSES:
            return jsonify({'error': f'Invalid status. Must be one of: {VALID_STATUSES}'}), 400
        
        minute_key = get_minute_key(timestamp)
        
        if minute_key not in minute_buffer:
            minute_buffer[minute_key] = {
                'timestamp': minute_key,
                'approved': 0,
                'failed': 0,
                'denied': 0,
                'reversed': 0,
                'total': 0
            }
        
        minute_buffer[minute_key][status] += 1
        minute_buffer[minute_key]['total'] += 1
        
        if len(minute_buffer) > MINUTE_BUFFER_LIMIT:
            oldest = sorted(minute_buffer.keys())[0]
            del minute_buffer[oldest]
        
        result = detector.detect_anomalies(
            minute_key,
            minute_buffer[minute_key].copy()
        )
        
        if result['should_alert']:
            alert_queue.put(result)
        
        return jsonify({
            'status': 'processed',
            'transaction': {
                'timestamp': timestamp,
                'status': status
            },
            'minute': minute_buffer[minute_key],
            'anomaly_detection': {
                'detected': result['should_alert'],
                'score': round(result['anomaly_score'], 2),
                'recommendation': result.get('recommendation', 'NORMAL'),
                'anomalies': result['anomalies'][:3]
            }
        }), 200
        
    except Exception:
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/status/current', methods=['GET'])
def get_current_status():
    try:
        if not minute_buffer:
            return jsonify({
                'current_minute': None,
                'current_minute_data': {
                    'approved': 0, 'failed': 0, 'denied': 0, 'reversed': 0, 'total': 0
                },
                'statistics': {
                    'total_transactions': 0,
                    'success_rate': 0,
                    'minutes_in_buffer': 0
                }
            }), 200
        
        recent_minutes = sorted(minute_buffer.keys(), reverse=True)
        current_minute = recent_minutes[0]
        current_data = minute_buffer[current_minute]
        
        total_tx = sum(m.get('total', 0) for m in minute_buffer.values())
        total_approved = sum(m.get('approved', 0) for m in minute_buffer.values())
        success_rate = (total_approved / total_tx * 100) if total_tx > 0 else 0
        
        return jsonify({
            'current_minute': current_minute,
            'current_minute_data': current_data,
            'statistics': {
                'total_transactions': total_tx,
                'success_rate': round(success_rate, 2),
                'minutes_in_buffer': len(minute_buffer)
            }
        }), 200
        
    except Exception:
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    try:
        limit = min(request.args.get('limit', 50, type=int), 500)
        recent_alerts = alerts_history[-limit:] if alerts_history else []
        
        formatted_alerts = []
        for alert in recent_alerts:
            formatted_alerts.append({
                'timestamp': alert.get('timestamp'),
                'anomaly_score': round(alert.get('anomaly_score', 0), 2),
                'recommendation': alert.get('recommendation', 'NORMAL'),
                'anomalies': alert.get('anomalies', [])[:2]
            })
        
        return jsonify({
            'total_alerts': len(alerts_history),
            'alerts': formatted_alerts
        }), 200
        
    except Exception:
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/query/transactions', methods=['GET'])
def query_transactions():
    try:
        limit = request.args.get('limit', 100, type=int)
        db_path = get_database_path()
        conn = sqlite3.connect(db_path)
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        if 'transactions' in tables:
            cursor.execute("PRAGMA table_info(transactions)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'status' in columns and 'count' in columns:
                query = """
                    SELECT 
                        timestamp,
                        SUM(CASE WHEN status = 'failed' THEN count ELSE 0 END) as failed,
                        SUM(CASE WHEN status = 'denied' THEN count ELSE 0 END) as denied,
                        SUM(CASE WHEN status = 'reversed' THEN count ELSE 0 END) as reversed,
                        SUM(CASE WHEN status = 'approved' THEN count ELSE 0 END) as approved,
                        SUM(count) as total
                    FROM transactions
                    GROUP BY timestamp
                    ORDER BY timestamp DESC
                    LIMIT ?
                """
                df = pd.read_sql_query(query, conn, params=(limit,))
            else:
                query = "SELECT timestamp, count as total FROM transactions ORDER BY timestamp DESC LIMIT ?"
                df = pd.read_sql_query(query, conn, params=(limit,))
                df['failed'] = df['total'] * 0.15
                df['denied'] = df['total'] * 0.10
                df['reversed'] = df['total'] * 0.05
                df['approved'] = df['total'] * 0.70
        else:
            conn.close()
            return jsonify({'error': 'No transactions table found'}), 404
        
        conn.close()
        
        stats = {}
        if not df.empty:
            for col in ['failed', 'denied', 'reversed', 'approved', 'total']:
                if col in df.columns:
                    stats[col] = {
                        'mean': float(df[col].mean()),
                        'max': int(df[col].max()),
                        'total': int(df[col].sum())
                    }
        
        return jsonify({
            'statistics': stats,
            'data': df.to_dict(orient='records') if not df.empty else [],
            'row_count': len(df)
        }), 200
        
    except Exception:
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/stats', methods=['GET'])
def get_system_stats():
    if not detector:
        return jsonify({'error': 'Detector not initialized'}), 503
    
    stats = detector.get_stats()
    
    stats.update({
        'api': {
            'alerts_pending': alert_queue.qsize(),
            'alerts_history': len(alerts_history),
            'minutes_in_buffer': len(minute_buffer),
            'total_transactions': sum(m['total'] for m in minute_buffer.values())
        }
    })
    
    return jsonify(stats)

@app.route('/api/reset', methods=['POST'])
def reset_system():
    global minute_buffer, alerts_history, detector
    
    minute_buffer = {}
    alerts_history = []
    
    while not alert_queue.empty():
        try:
            alert_queue.get_nowait()
            alert_queue.task_done()
        except queue.Empty:
            break
    
    initialize_detector()
    
    return jsonify({'message': 'System reset successfully'}), 200

def start_api(host='0.0.0.0', port=5000):
    print("\n" + "=" * 60)
    print("TRANSACTION MONITORING API")
    print("=" * 60)
    
    initialize_detector()
    
    worker_thread = threading.Thread(target=alert_worker, daemon=True)
    worker_thread.start()
    
    print("\nServer running on http://{}:{}".format(host, port))
    print("=" * 60 + "\n")
    
    app.run(host=host, port=port, threaded=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=5000)
    args = parser.parse_args()
    start_api(host=args.host, port=args.port)