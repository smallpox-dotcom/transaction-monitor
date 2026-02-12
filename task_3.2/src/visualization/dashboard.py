import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import time
import uuid

API_URL = "http://localhost:5000"

st.set_page_config(page_title="Transaction Monitor", layout="wide")
st.title("Transaction Monitoring System")

if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    st.session_state.alert_history = []
    st.session_state.buffer_data = []
    st.session_state.alerts_page = 1
    st.session_state.alerts_filter = 'All'
    st.session_state.spike_alert = None
    st.session_state.spike_timer = 0
    st.session_state.chart_initialized = False
    st.session_state.last_chart_time = 0
    st.session_state.fig = None
    st.session_state.chart_key = str(uuid.uuid4())
    st.session_state.current_tab = "Dashboard"
    st.session_state.last_update = time.time()
    st.session_state.update_counter = 0
    st.session_state.chart_rendered = False
    st.session_state.alert_counter = 0
    st.session_state.alert_details = {}

with st.sidebar:
    st.header("Navigation")
    
    selected_tab = st.radio(
        "Menu",
        ["Dashboard", "Alerts", "Analytics", "Settings"],
        index=["Dashboard", "Alerts", "Analytics", "Settings"].index(st.session_state.current_tab),
        key="nav_radio"
    )
    
    if selected_tab != st.session_state.current_tab:
        st.session_state.current_tab = selected_tab
        st.session_state.chart_key = str(uuid.uuid4())
        st.session_state.chart_rendered = False
        st.rerun()
    
    st.divider()
    
    if st.session_state.current_tab == "Settings":
        st.subheader("Threshold Configuration")
        threshold_failed = st.slider("Failed Threshold", 0, 50, 25, key="set_failed")
        threshold_denied = st.slider("Denied Threshold", 0, 40, 20, key="set_denied")
        threshold_reversed = st.slider("Reversed Threshold", 0, 30, 12, key="set_reversed")
        
        st.divider()
        st.subheader("Display Settings")
        refresh_rate = st.slider("Refresh Rate (seconds)", 0.5, 5.0, 1.0, step=0.5, key="set_refresh")
        chart_minutes = st.slider("Minutes to Display", 5, 60, 30, key="set_minutes")
        spike_threshold = st.slider("Spike Threshold (alerts/min)", 2, 20, 8, key="set_spike")
        
        st.divider()
        if st.button("Reset System", key="reset_btn"):
            try:
                requests.post(f"{API_URL}/api/reset", timeout=2)
                st.success("System reset completed")
                st.session_state.alert_history = []
                st.session_state.buffer_data = []
                st.session_state.alerts_page = 1
                st.session_state.chart_initialized = False
                st.session_state.spike_alert = None
                st.session_state.fig = None
                st.session_state.chart_key = str(uuid.uuid4())
                st.session_state.chart_rendered = False
                st.session_state.alert_counter = 0
                st.session_state.alert_details = {}
                time.sleep(1)
                st.rerun()
            except:
                st.error("Reset failed")
    else:
        threshold_failed = 25
        threshold_denied = 20
        threshold_reversed = 12
        refresh_rate = 1.0
        chart_minutes = 30
        spike_threshold = 8

def should_update_chart():
    current_second = datetime.now().second
    return current_second in [0, 15, 30, 45]

if st.session_state.current_tab == "Dashboard":
    spike_placeholder = st.empty()
    metrics_placeholder = st.empty()
    chart_placeholder = st.empty()
    stats_placeholder = st.empty()
    
    st.session_state.chart_rendered = False
    
    while st.session_state.current_tab == "Dashboard":
        try:
            current_time_val = time.time()
            if current_time_val - st.session_state.last_update >= refresh_rate:
                st.session_state.last_update = current_time_val
                st.session_state.update_counter += 1
            
            status = requests.get(f"{API_URL}/api/status/current", timeout=1).json()
            alerts = requests.get(f"{API_URL}/api/alerts?limit=100", timeout=1).json()
            system_stats = requests.get(f"{API_URL}/api/stats", timeout=1).json()
            
            current_data = status.get('current_minute_data', {})
            current_minute = status.get('current_minute', None)
            
            if current_minute:
                try:
                    current_time = datetime.now()
                    current_minute_dt = current_time.replace(second=0, microsecond=0)
                    
                    new_entry = {
                        'timestamp': current_minute_dt,
                        'failed': current_data.get('failed', 0),
                        'denied': current_data.get('denied', 0),
                        'reversed': current_data.get('reversed', 0),
                        'approved': current_data.get('approved', 0),
                        'total': current_data.get('total', 0)
                    }
                    
                    if not st.session_state.buffer_data or st.session_state.buffer_data[-1]['timestamp'] != current_minute_dt:
                        st.session_state.buffer_data.append(new_entry)
                        st.session_state.chart_initialized = True
                    
                    cutoff_time = current_time - timedelta(minutes=chart_minutes)
                    st.session_state.buffer_data = [
                        entry for entry in st.session_state.buffer_data 
                        if entry['timestamp'] >= cutoff_time
                    ]
                except:
                    pass
            
            df = pd.DataFrame(st.session_state.buffer_data)
            
            total_tx = current_data.get('total', 0)
            failed_tx = current_data.get('failed', 0)
            denied_tx = current_data.get('denied', 0)
            reversed_tx = current_data.get('reversed', 0)
            
            failed_rate = (failed_tx / total_tx * 100) if total_tx > 0 else 0
            denied_rate = (denied_tx / total_tx * 100) if total_tx > 0 else 0
            reversed_rate = (reversed_tx / total_tx * 100) if total_tx > 0 else 0
            
            failed_delta = failed_tx - threshold_failed if failed_tx > threshold_failed else None
            denied_delta = denied_tx - threshold_denied if denied_tx > threshold_denied else None
            reversed_delta = reversed_tx - threshold_reversed if reversed_tx > threshold_reversed else None
            
            with metrics_placeholder.container():
                mcol1, mcol2, mcol3, mcol4 = st.columns(4)
                mcol1.metric("Current Time", datetime.now().strftime("%H:%M:%S"), f"{total_tx} tx/min")
                mcol2.metric("Failed", f"{failed_tx} ({failed_rate:.1f}%)", delta=failed_delta, delta_color="inverse")
                mcol3.metric("Denied", f"{denied_tx} ({denied_rate:.1f}%)", delta=denied_delta, delta_color="inverse")
                mcol4.metric("Reversed", f"{reversed_tx} ({reversed_rate:.1f}%)", delta=reversed_delta, delta_color="inverse")
            
            critical_alerts_last_minute = []
            now = datetime.now()
            for a in alerts.get('alerts', []):
                try:
                    alert_time_str = a.get('timestamp', '')
                    if alert_time_str:
                        alert_time = datetime.fromisoformat(alert_time_str.replace(' ', 'T'))
                        if now - alert_time < timedelta(minutes=1) and a.get('anomaly_score', 0) >= 70:
                            critical_alerts_last_minute.append(a)
                except:
                    continue
            
            if len(critical_alerts_last_minute) >= spike_threshold:
                st.session_state.spike_alert = {
                    'timestamp': datetime.now().strftime('%H:%M:%S'),
                    'count': len(critical_alerts_last_minute),
                    'threshold': spike_threshold,
                    'scores': [a.get('anomaly_score', 0) for a in critical_alerts_last_minute[:5]]
                }
                st.session_state.spike_timer = time.time()
            
            if st.session_state.spike_alert and time.time() - st.session_state.spike_timer < 10:
                with spike_placeholder.container():
                    st.error(f"ANOMALY SPIKE DETECTED - {st.session_state.spike_alert['count']} critical alerts in last minute (threshold: {st.session_state.spike_alert['threshold']})")
                    st.caption(f"Scores: {', '.join([str(s) for s in st.session_state.spike_alert['scores']])} at {st.session_state.spike_alert['timestamp']}")
            else:
                spike_placeholder.empty()
                st.session_state.spike_alert = None
            
            if st.session_state.chart_initialized and not df.empty and len(df) >= 1:
                current_second = datetime.now().second
                next_update = 15 - (current_second % 15)
                
                if should_update_chart() and current_second != st.session_state.last_chart_time:
                    st.session_state.last_chart_time = current_second
                    
                    fig = make_subplots(
                        rows=3, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.08,
                        subplot_titles=('Failed Transactions - Real Time', 'Denied Transactions - Real Time', 'Reversed Transactions - Real Time')
                    )
                    
                    fig.add_trace(
                        go.Scatter(
                            x=df['timestamp'],
                            y=df['failed'],
                            name='Failed',
                            line=dict(color='red', width=2),
                            mode='lines+markers',
                            marker=dict(size=6)
                        ),
                        row=1, col=1
                    )
                    fig.add_hline(y=threshold_failed, line_dash="dash", line_color="red", 
                                 annotation_text=f"Threshold: {threshold_failed}", 
                                 annotation_position="top right",
                                 row=1, col=1)
                    
                    fig.add_trace(
                        go.Scatter(
                            x=df['timestamp'],
                            y=df['denied'],
                            name='Denied',
                            line=dict(color='orange', width=2),
                            mode='lines+markers',
                            marker=dict(size=6)
                        ),
                        row=2, col=1
                    )
                    fig.add_hline(y=threshold_denied, line_dash="dash", line_color="orange",
                                 annotation_text=f"Threshold: {threshold_denied}",
                                 annotation_position="top right",
                                 row=2, col=1)
                    
                    fig.add_trace(
                        go.Scatter(
                            x=df['timestamp'],
                            y=df['reversed'],
                            name='Reversed',
                            line=dict(color='purple', width=2),
                            mode='lines+markers',
                            marker=dict(size=6)
                        ),
                        row=3, col=1
                    )
                    fig.add_hline(y=threshold_reversed, line_dash="dash", line_color="purple",
                                 annotation_text=f"Threshold: {threshold_reversed}",
                                 annotation_position="top right",
                                 row=3, col=1)
                    
                    now_time = datetime.now()
                    time_range = f"{df['timestamp'].min().strftime('%H:%M')} - {df['timestamp'].max().strftime('%H:%M')}"
                    
                    fig.update_layout(
                        height=650,
                        showlegend=False,
                        hovermode='x unified',
                        margin=dict(l=50, r=50, t=80, b=50),
                        title_text=f"Real-Time Transaction Status - Last {len(df)} Minutes ({time_range}) - Updated at {now_time.strftime('%H:%M:%S')} (Next: {next_update}s)"
                    )
                    
                    fig.update_xaxes(
                        title_text="Time (HH:MM)",
                        row=3, col=1,
                        tickangle=45,
                        tickformat="%H:%M"
                    )
                    fig.update_yaxes(title_text="Count", row=1, col=1)
                    fig.update_yaxes(title_text="Count", row=2, col=1)
                    fig.update_yaxes(title_text="Count", row=3, col=1)
                    
                    st.session_state.fig = fig
                    st.session_state.chart_key = str(uuid.uuid4())
                    st.session_state.chart_rendered = False
                
                if st.session_state.fig and not st.session_state.chart_rendered:
                    chart_placeholder.plotly_chart(
                        st.session_state.fig,
                        width='stretch',
                        key=f"dashboard_chart_{st.session_state.chart_key}"
                    )
                    st.session_state.chart_rendered = True
            
            with stats_placeholder.container():
                st.subheader("System Health")
                scol1, scol2, scol3, scol4 = st.columns(4)
                
                detector_stats = system_stats.get('status_stats', {}).get('total', {})
                api_stats = system_stats.get('api', {})
                
                scol1.metric("Total Alerts", api_stats.get('alerts_history', 0))
                scol2.metric("Active Minutes", api_stats.get('minutes_in_buffer', 0))
                scol3.metric("Avg Transaction/min", f"{detector_stats.get('mean', 0):.1f}")
                scol4.metric("Z-Score Threshold", system_stats.get('z_threshold', 3.0))
            
            if st.session_state.current_tab != "Dashboard":
                break
            
            time.sleep(0.1)
            
        except Exception as e:
            st.error(f"Connection Error: {e}")
            time.sleep(2)
            continue

elif st.session_state.current_tab == "Alerts":
    st.header("Alert History")
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("Filters")
        
        filter_options = ['All', 'CRITICAL', 'WARNING', 'Failed', 'Denied', 'Reversed', 'Statistical']
        selected_filter = st.selectbox("Filter by type", filter_options, key="alerts_filter_select")
        st.session_state.alerts_filter = selected_filter
        
        st.divider()
        st.subheader("Statistics")
        
        total_alerts = len(st.session_state.alert_history)
        critical_count = sum(1 for a in st.session_state.alert_history if a.get('severity') == 'CRITICAL')
        warning_count = sum(1 for a in st.session_state.alert_history if a.get('severity') == 'WARNING')
        
        failed_count = 0
        denied_count = 0
        reversed_count = 0
        statistical_count = 0
        
        for alert in st.session_state.alert_history:
            for anomaly in alert.get('anomalies', []):
                anomaly_type = anomaly.get('type', '')
                if 'failed' in anomaly_type:
                    failed_count += 1
                if 'denied' in anomaly_type:
                    denied_count += 1
                if 'reversed' in anomaly_type:
                    reversed_count += 1
                if 'outlier' in anomaly_type or 'statistical' in anomaly_type or 'spike' in anomaly_type:
                    statistical_count += 1
        
        st.metric("Total Alerts", total_alerts)
        st.metric("Critical", critical_count)
        st.metric("Warning", warning_count)
        st.metric("Failed Alerts", failed_count)
        st.metric("Denied Alerts", denied_count)
        st.metric("Reversed Alerts", reversed_count)
        st.metric("Statistical", statistical_count)
        
        st.divider()
        
        if st.button("Clear All Alerts", key="clear_alerts_btn"):
            st.session_state.alert_history = []
            st.session_state.alerts_page = 1
            st.session_state.alert_counter = 0
            st.session_state.alert_details = {}
            st.rerun()
        
        alerts_per_page = st.selectbox("Alerts per page", [5, 10, 20, 50], index=1, key="alerts_per_page_select")
    
    with col2:
        try:
            response = requests.get(f"{API_URL}/api/alerts?limit=1000", timeout=2)
            if response.status_code == 200:
                api_alerts = response.json().get('alerts', [])
                
                for alert in api_alerts:
                    alert_id = alert.get('timestamp', '') + str(alert.get('anomaly_score', '')) + str(hash(str(alert.get('anomalies', ''))))
                    
                    if alert_id not in st.session_state.alert_details:
                        st.session_state.alert_counter += 1
                        alert_data = {
                            'id': st.session_state.alert_counter,
                            'alert_id': alert_id,
                            'timestamp': alert.get('timestamp', ''),
                            'score': alert.get('anomaly_score', 0),
                            'severity': 'CRITICAL' if alert.get('anomaly_score', 0) >= 70 else 'WARNING',
                            'recommendation': alert.get('recommendation', 'NORMAL'),
                            'anomalies': alert.get('anomalies', []),
                            'deliberations': [],
                            'status': 'new',
                            'acknowledged': False,
                            'resolved': False,
                            'notes': ''
                        }
                        st.session_state.alert_details[alert_id] = alert_data
                        st.session_state.alert_history.append(alert_data)
                
                st.session_state.alert_history.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        except:
            pass
        
        filtered_alerts = []
        
        if selected_filter == 'All':
            filtered_alerts = st.session_state.alert_history
        elif selected_filter == 'CRITICAL':
            filtered_alerts = [a for a in st.session_state.alert_history if a.get('severity') == 'CRITICAL']
        elif selected_filter == 'WARNING':
            filtered_alerts = [a for a in st.session_state.alert_history if a.get('severity') == 'WARNING']
        elif selected_filter in ['Failed', 'Denied', 'Reversed', 'Statistical']:
            filter_lower = selected_filter.lower()
            for alert in st.session_state.alert_history:
                for anomaly in alert.get('anomalies', []):
                    anomaly_type = anomaly.get('type', '').lower()
                    if filter_lower in anomaly_type:
                        filtered_alerts.append(alert)
                        break
        
        total_pages = max(1, (len(filtered_alerts) + alerts_per_page - 1) // alerts_per_page)
        
        if st.session_state.alerts_page > total_pages:
            st.session_state.alerts_page = 1
        
        if filtered_alerts:
            start_idx = (st.session_state.alerts_page - 1) * alerts_per_page
            end_idx = min(start_idx + alerts_per_page, len(filtered_alerts))
            page_alerts = filtered_alerts[start_idx:end_idx]
            
            st.subheader(f"Showing {start_idx + 1}-{end_idx} of {len(filtered_alerts)} alerts")
            
            for idx, alert in enumerate(page_alerts):
                alert_id = alert.get('alert_id', str(idx))
                severity = alert.get('severity', 'WARNING')
                score = alert.get('score', 0)
                timestamp = alert.get('timestamp', 'Unknown')
                
                cols = st.columns([4, 1])
                
                with cols[0]:
                    if severity == 'CRITICAL':
                        st.error(f"ALERT #{alert.get('id', '')} - [{timestamp}] CRITICAL - Score: {score}")
                    else:
                        st.warning(f"ALERT #{alert.get('id', '')} - [{timestamp}] WARNING - Score: {score}")
                
                with cols[1]:
                    status_options = ['new', 'investigating', 'mitigated', 'resolved', 'false_positive']
                    current_status = alert.get('status', 'new')
                    new_status = st.selectbox(
                        "Status",
                        status_options,
                        index=status_options.index(current_status) if current_status in status_options else 0,
                        key=f"status_{alert_id}_{idx}"
                    )
                    alert['status'] = new_status
                
                expander_key = f"expander_{alert_id}_{idx}_{st.session_state.chart_key}"
                with st.expander(f"View details - Alert #{alert.get('id', '')}", expanded=False):
                    tab1, tab2, tab3 = st.tabs(["Anomaly Details", "Deliberations", "Resolution"])
                    
                    with tab1:
                        st.caption("ANOMALY DETAILS")
                        for i, anomaly in enumerate(alert.get('anomalies', []), 1):
                            st.markdown(f"**{i}. {anomaly.get('type', 'Unknown').replace('_', ' ').upper()}**")
                            st.write(f"   • Message: {anomaly.get('message', '')}")
                            st.write(f"   • Severity: {anomaly.get('severity', '')}")
                            
                            if 'value' in anomaly:
                                st.write(f"   • Value: {anomaly.get('value', '')}")
                            if 'threshold' in anomaly:
                                st.write(f"   • Threshold: {anomaly.get('threshold', '')}")
                            if 'ratio' in anomaly:
                                st.write(f"   • Ratio: {anomaly.get('ratio', ''):.1%}")
                            if 'z_score' in anomaly:
                                st.write(f"   • Z-Score: {anomaly.get('z_score', ''):.2f}")
                            if 'mean' in anomaly:
                                st.write(f"   • Historical Mean: {anomaly.get('mean', ''):.1f}")
                            if 'std' in anomaly:
                                st.write(f"   • Std Dev: {anomaly.get('std', ''):.1f}")
                            
                            if i < len(alert.get('anomalies', [])):
                                st.divider()
                    
                    with tab2:
                        st.caption("INVESTIGATION NOTES")
                        
                        col1, col2 = st.columns([3, 1])
                        with col1:
                            new_note = st.text_area(
                                "Add deliberation",
                                key=f"note_input_{alert_id}_{idx}",
                                placeholder="Enter your analysis, findings, or action plan...",
                                height=100
                            )
                        with col2:
                            st.write(" ")
                            st.write(" ")
                            if st.button("Add Note", key=f"add_note_{alert_id}_{idx}"):
                                if new_note.strip():
                                    note_entry = {
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'author': 'analyst',
                                        'note': new_note.strip()
                                    }
                                    if 'deliberations' not in alert:
                                        alert['deliberations'] = []
                                    alert['deliberations'].append(note_entry)
                                    st.rerun()
                        
                        st.divider()
                        st.caption("DELIBERATION HISTORY")
                        
                        if alert.get('deliberations'):
                            for note_entry in reversed(alert['deliberations']):
                                st.markdown(f"**{note_entry['timestamp']}** - *{note_entry['author']}*")
                                st.write(note_entry['note'])
                                st.divider()
                        else:
                            st.info("No deliberations recorded yet.")
                    
                    with tab3:
                        st.caption("RESOLUTION")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            acknowledged = st.checkbox(
                                "Acknowledged",
                                value=alert.get('acknowledged', False),
                                key=f"ack_{alert_id}_{idx}"
                            )
                            alert['acknowledged'] = acknowledged
                        
                        with col2:
                            resolved = st.checkbox(
                                "Resolved",
                                value=alert.get('resolved', False),
                                key=f"res_{alert_id}_{idx}"
                            )
                            alert['resolved'] = resolved
                        
                        st.caption("RESOLUTION NOTES")
                        resolution_note = st.text_area(
                            "Resolution details",
                            value=alert.get('notes', ''),
                            key=f"resolution_{alert_id}_{idx}",
                            placeholder="Describe how this alert was resolved..."
                        )
                        alert['notes'] = resolution_note
                        
                        if alert.get('resolved'):
                            st.success(f"This alert was resolved. Status: {alert.get('status', 'resolved')}")
                    
                    st.caption(f"RECOMMENDATION: {alert.get('recommendation', 'NORMAL')}")
                
                st.divider()
            
            if total_pages > 1:
                col_prev, col_pages, col_next = st.columns([1, 3, 1])
                
                with col_prev:
                    if st.button("Previous", key="prev_btn", disabled=st.session_state.alerts_page <= 1):
                        st.session_state.alerts_page -= 1
                        st.rerun()
                
                with col_pages:
                    page_numbers = []
                    start_page = max(1, st.session_state.alerts_page - 2)
                    end_page = min(total_pages, st.session_state.alerts_page + 2)
                    
                    for i in range(start_page, end_page + 1):
                        page_numbers.append(i)
                    
                    page_cols = st.columns(len(page_numbers))
                    for i, page_num in enumerate(page_numbers):
                        with page_cols[i]:
                            btn_key = f"page_{page_num}_{st.session_state.chart_key}_{idx}"
                            if st.button(
                                str(page_num),
                                key=btn_key,
                                type="primary" if page_num == st.session_state.alerts_page else "secondary"
                            ):
                                st.session_state.alerts_page = page_num
                                st.rerun()
                
                with col_next:
                    if st.button("Next", key="next_btn", disabled=st.session_state.alerts_page >= total_pages):
                        st.session_state.alerts_page += 1
                        st.rerun()
        else:
            st.info("No alerts match the selected filter")

elif st.session_state.current_tab == "Analytics":
    st.header("Transaction Analytics")
    
    if st.session_state.buffer_data:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Status Distribution")
            df = pd.DataFrame(st.session_state.buffer_data)
            
            if not df.empty:
                latest = df.iloc[-1]
                
                dist_data = pd.DataFrame({
                    'Status': ['Approved', 'Failed', 'Denied', 'Reversed'],
                    'Count': [
                        latest.get('approved', 0),
                        latest.get('failed', 0),
                        latest.get('denied', 0),
                        latest.get('reversed', 0)
                    ]
                })
                
                if dist_data['Count'].sum() > 0:
                    fig = go.Figure(data=[go.Pie(
                        labels=dist_data['Status'],
                        values=dist_data['Count'],
                        hole=0.4,
                        marker=dict(colors=['#2ecc71', '#e74c3c', '#f39c12', '#9b59b6'])
                    )])
                    fig.update_layout(
                        height=400,
                        title=f"Distribution at {latest['timestamp'].strftime('%H:%M:%S')}"
                    )
                    st.plotly_chart(fig, width='stretch', key=f"analytics_pie_{st.session_state.chart_key}")
                else:
                    st.info("No transactions in current minute")
        
        with col2:
            st.subheader("Performance Metrics")
            
            total_tx = sum(entry['total'] for entry in st.session_state.buffer_data)
            total_failed = sum(entry['failed'] for entry in st.session_state.buffer_data)
            total_denied = sum(entry['denied'] for entry in st.session_state.buffer_data)
            total_reversed = sum(entry['reversed'] for entry in st.session_state.buffer_data)
            
            success_rate = ((total_tx - total_failed - total_denied - total_reversed) / total_tx * 100) if total_tx > 0 else 0
            
            st.metric("Total Transactions", total_tx)
            st.metric("Success Rate", f"{success_rate:.1f}%")
            st.metric("Failed Rate", f"{total_failed/total_tx*100:.1f}%" if total_tx > 0 else "0%")
            st.metric("Denied Rate", f"{total_denied/total_tx*100:.1f}%" if total_tx > 0 else "0%")
            st.metric("Reversed Rate", f"{total_reversed/total_tx*100:.1f}%" if total_tx > 0 else "0%")
    else:
        st.info("No transaction data available")

elif st.session_state.current_tab == "Settings":
    st.header("System Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Current Configuration")
        st.info(f"""
        **Failed Threshold:** {threshold_failed}
        **Denied Threshold:** {threshold_denied}
        **Reversed Threshold:** {threshold_reversed}
        **Spike Threshold:** {spike_threshold} alerts/min
        **Refresh Rate:** {refresh_rate}s
        **Minutes Displayed:** {chart_minutes}
        """)
    
    with col2:
        st.subheader("System Information")
        try:
            health = requests.get(f"{API_URL}/health", timeout=2).json()
            st.success("API Status: Connected")
            st.json({
                'detector': health.get('detector_initialized', False),
                'alerts': health.get('alerts_history', 0),
                'buffer': health.get('minutes_in_buffer', 0)
            })
        except:
            st.error("API Status: Disconnected")