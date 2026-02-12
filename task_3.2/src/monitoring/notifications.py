import requests
import smtplib
import json
from email.mime.text import MIMEText
from datetime import datetime
import os

class NotificationManager:
    def __init__(self):
        self.slack_webhook = os.getenv('SLACK_WEBHOOK')
        self.email_config = {
            'smtp_server': os.getenv('SMTP_SERVER', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', 587)),
            'sender': os.getenv('ALERT_EMAIL'),
            'password': os.getenv('EMAIL_PASSWORD'),
            'recipients': os.getenv('ALERT_RECIPIENTS', '').split(',')
        }
    
    def send_slack_alert(self, alert):
        if not self.slack_webhook:
            return
        
        message = {
            "attachments": [{
                "color": "danger" if alert['anomaly_score'] >= 70 else "warning",
                "title": f"🚨 Transaction Anomaly Detected",
                "fields": [
                    {"title": "Score", "value": alert['anomaly_score'], "short": True},
                    {"title": "Time", "value": alert['timestamp'][-8:], "short": True},
                    {"title": "Status Counts", "value": 
                     f"✅ {alert['status_counts'].get('approved',0)} "
                     f"❌ {alert['status_counts'].get('failed',0)} "
                     f"⛔ {alert['status_counts'].get('denied',0)} "
                     f"↩️ {alert['status_counts'].get('reversed',0)}", 
                     "short": False},
                    {"title": "Anomalies", "value": 
                     "\n".join([a['message'] for a in alert['anomalies'][:3]]), 
                     "short": False}
                ]
            }]
        }
        
        try:
            requests.post(self.slack_webhook, json=message, timeout=2)
        except:
            pass
    
    def send_email_alert(self, alert):
        if not all([self.email_config['sender'], self.email_config['password']]):
            return
        
        try:
            msg = MIMEText(f"""
            Anomaly Score: {alert['anomaly_score']}
            Time: {alert['timestamp']}
            
            Status Counts:
            - Approved: {alert['status_counts'].get('approved', 0)}
            - Failed: {alert['status_counts'].get('failed', 0)}
            - Denied: {alert['status_counts'].get('denied', 0)}
            - Reversed: {alert['status_counts'].get('reversed', 0)}
            
            Anomalies:
            {chr(10).join(['- ' + a['message'] for a in alert['anomalies'][:5]])}
            
            Recommendation: {alert['recommendation']}
            """)
            
            msg['Subject'] = f"[ALERT] Transaction Anomaly - Score: {alert['anomaly_score']}"
            msg['From'] = self.email_config['sender']
            msg['To'] = ', '.join(self.email_config['recipients'])
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['sender'], self.email_config['password'])
            server.send_message(msg)
            server.quit()
        except:
            pass