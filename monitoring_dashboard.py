#!/usr/bin/env python3
"""
Fixed Factor Monitoring Dashboard
Compatible with current Dash version
"""

import os
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FactorMonitoringDashboard:
    """Real-time dashboard for factor monitoring system"""
    
    def __init__(self, db_path="factor_monitoring_production.db"):
        self.db_path = db_path
        self.app = None
        self.setup_dashboard()
    
    def setup_dashboard(self):
        """Setup Dash application"""
        try:
            import dash
            from dash import dcc, html, Input, Output, callback
            import dash_bootstrap_components as dbc
            
            # Create Dash app with Bootstrap theme
            self.app = dash.Dash(
                __name__, 
                external_stylesheets=[dbc.themes.BOOTSTRAP],
                title="Factor Monitoring Dashboard"
            )
            
            # Setup layout
            self.create_layout()
            
            # Setup callbacks
            self.setup_callbacks()
            
            logger.info("✅ Dashboard setup complete")
            
        except ImportError as e:
            logger.error(f"❌ Missing required package: {e}")
            logger.info("💡 Install with: pip install dash dash-bootstrap-components")
            raise
        except Exception as e:
            logger.error(f"❌ Dashboard setup failed: {e}")
            raise
    
    def create_layout(self):
        """Create dashboard layout"""
        from dash import dcc, html
        import dash_bootstrap_components as dbc
        
        self.app.layout = dbc.Container([
            # Header
            dbc.Row([
                dbc.Col([
                    html.H1("🎯 Factor Monitoring Dashboard", className="text-center mb-4"),
                    html.H6(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                           className="text-center text-muted mb-4")
                ])
            ]),
            
            # Status Cards
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("System Status", className="card-title"),
                            html.H2(id="system-status", className="text-success"),
                            html.P("Last Analysis", className="card-text")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Portfolio Value", className="card-title"),
                            html.H2(id="portfolio-value", className="text-primary"),
                            html.P("Total Assets", className="card-text")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Active Positions", className="card-title"),
                            html.H2(id="active-positions", className="text-info"),
                            html.P("Holdings", className="card-text")
                        ])
                    ])
                ], width=3),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Today's P&L", className="card-title"),
                            html.H2(id="daily-pnl", className="text-warning"),
                            html.P("Unrealized", className="card-text")
                        ])
                    ])
                ], width=3)
            ], className="mb-4"),
            
            # Auto-refresh interval
            dcc.Interval(
                id='interval-component',
                interval=30*1000,  # Update every 30 seconds
                n_intervals=0
            ),
            
            # Charts Row 1
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="portfolio-performance-chart")
                ], width=6),
                dbc.Col([
                    dcc.Graph(id="factor-exposure-chart")
                ], width=6)
            ], className="mb-4"),
            
            # Charts Row 2
            dbc.Row([
                dbc.Col([
                    dcc.Graph(id="holdings-allocation-chart")
                ], width=6),
                dbc.Col([
                    dcc.Graph(id="sector-allocation-chart")
                ], width=6)
            ], className="mb-4"),
            
            # Recent Activity Table
            dbc.Row([
                dbc.Col([
                    html.H4("Recent Activity"),
                    html.Div(id="recent-activity-table")
                ], width=12)
            ])
            
        ], fluid=True)
    
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        from dash import Input, Output, callback
        
        @callback(
            [Output('system-status', 'children'),
             Output('portfolio-value', 'children'),
             Output('active-positions', 'children'),
             Output('daily-pnl', 'children'),
             Output('portfolio-performance-chart', 'figure'),
             Output('factor-exposure-chart', 'figure'),
             Output('holdings-allocation-chart', 'figure'),
             Output('sector-allocation-chart', 'figure'),
             Output('recent-activity-table', 'children')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_dashboard(n):
            return self.update_all_components()
    
    def get_database_data(self):
        """Get data from database"""
        try:
            if not os.path.exists(self.db_path):
                logger.warning(f"Database not found: {self.db_path}")
                return self.get_sample_data()
            
            conn = sqlite3.connect(self.db_path)
            
            # Get latest portfolio data
            portfolio_df = pd.read_sql_query("""
                SELECT * FROM portfolio_snapshots 
                ORDER BY timestamp DESC 
                LIMIT 30
            """, conn)
            
            # Get recent trades
            trades_df = pd.read_sql_query("""
                SELECT * FROM trades 
                ORDER BY timestamp DESC 
                LIMIT 50
            """, conn)
            
            # Get current positions
            positions_df = pd.read_sql_query("""
                SELECT * FROM positions 
                WHERE quantity != 0
                ORDER BY market_value DESC
            """, conn)
            
            conn.close()
            
            return {
                'portfolio': portfolio_df,
                'trades': trades_df,
                'positions': positions_df
            }
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return self.get_sample_data()
    
    def get_sample_data(self):
        """Generate sample data for demo"""
        logger.info("Using sample data for dashboard demo")
        
        # Sample portfolio performance
        dates = pd.date_range(start='2024-01-01', end=datetime.now(), freq='D')
        portfolio_data = []
        value = 1000000
        
        for date in dates:
            value += value * np.random.normal(0.0008, 0.02)  # ~20% annual return, 20% volatility
            portfolio_data.append({
                'timestamp': date,
                'total_value': value,
                'cash': value * 0.05,
                'positions_value': value * 0.95
            })
        
        portfolio_df = pd.DataFrame(portfolio_data)
        
        # Sample positions
        positions_data = [
            {'symbol': 'AAPL', 'quantity': 100, 'market_value': 18500, 'sector': 'Technology'},
            {'symbol': 'MSFT', 'quantity': 80, 'market_value': 16000, 'sector': 'Technology'},
            {'symbol': 'GOOGL', 'quantity': 50, 'market_value': 15000, 'sector': 'Technology'},
            {'symbol': 'JPM', 'quantity': 75, 'market_value': 12000, 'sector': 'Financial'},
            {'symbol': 'JNJ', 'quantity': 90, 'market_value': 11000, 'sector': 'Healthcare'},
            {'symbol': 'V', 'quantity': 45, 'market_value': 10500, 'sector': 'Financial'},
            {'symbol': 'PG', 'quantity': 60, 'market_value': 9500, 'sector': 'Consumer Goods'},
            {'symbol': 'HD', 'quantity': 30, 'market_value': 9000, 'sector': 'Consumer Discretionary'},
        ]
        positions_df = pd.DataFrame(positions_data)
        
        # Sample trades
        trades_data = [
            {'timestamp': datetime.now() - timedelta(hours=2), 'symbol': 'AAPL', 'action': 'BUY', 'quantity': 10, 'price': 185.00},
            {'timestamp': datetime.now() - timedelta(hours=5), 'symbol': 'MSFT', 'action': 'SELL', 'quantity': 5, 'price': 200.00},
            {'timestamp': datetime.now() - timedelta(days=1), 'symbol': 'GOOGL', 'action': 'BUY', 'quantity': 5, 'price': 300.00},
        ]
        trades_df = pd.DataFrame(trades_data)
        
        return {
            'portfolio': portfolio_df,
            'trades': trades_df,
            'positions': positions_df
        }
    
    def update_all_components(self):
        """Update all dashboard components"""
        try:
            data = self.get_database_data()
            
            portfolio_df = data['portfolio']
            positions_df = data['positions']
            trades_df = data['trades']
            
            # Status metrics
            if not portfolio_df.empty:
                latest_value = portfolio_df.iloc[0]['total_value']
                system_status = "🟢 Active"
                portfolio_value = f"${latest_value:,.0f}"
                
                if len(portfolio_df) > 1:
                    prev_value = portfolio_df.iloc[1]['total_value']
                    daily_change = latest_value - prev_value
                    daily_pnl = f"${daily_change:+,.0f}"
                else:
                    daily_pnl = "$0"
            else:
                system_status = "🔴 No Data"
                portfolio_value = "N/A"
                daily_pnl = "N/A"
            
            active_positions = str(len(positions_df))
            
            # Charts
            performance_chart = self.create_performance_chart(portfolio_df)
            factor_chart = self.create_factor_exposure_chart()
            holdings_chart = self.create_holdings_chart(positions_df)
            sector_chart = self.create_sector_chart(positions_df)
            
            # Recent activity
            activity_table = self.create_activity_table(trades_df)
            
            return (
                system_status, portfolio_value, active_positions, daily_pnl,
                performance_chart, factor_chart, holdings_chart, sector_chart,
                activity_table
            )
            
        except Exception as e:
            logger.error(f"Update error: {e}")
            # Return safe defaults
            return (
                "🔴 Error", "N/A", "0", "N/A",
                {}, {}, {}, {}, html.Div("Error loading data")
            )
    
    def create_performance_chart(self, df):
        """Create portfolio performance chart"""
        if df.empty:
            return {}
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['total_value'],
            mode='lines',
            name='Portfolio Value',
            line=dict(color='#1f77b4', width=2)
        ))
        
        fig.update_layout(
            title="Portfolio Performance",
            xaxis_title="Date",
            yaxis_title="Value ($)",
            height=400,
            showlegend=False
        )
        
        return fig
    
    def create_factor_exposure_chart(self):
        """Create factor exposure radar chart"""
        # Sample factor exposures
        factors = ['Value', 'Growth', 'Momentum', 'Quality', 'Low Vol', 'Size']
        exposures = [0.3, 0.7, 0.5, 0.8, 0.4, -0.2]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatterpolar(
            r=exposures,
            theta=factors,
            fill='toself',
            name='Factor Exposures'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[-1, 1]
                )),
            showlegend=False,
            title="Factor Exposures",
            height=400
        )
        
        return fig
    
    def create_holdings_chart(self, df):
        """Create holdings pie chart"""
        if df.empty:
            return {}
        
        # Top 8 holdings + others
        top_holdings = df.head(8)
        others_value = df.tail(len(df) - 8)['market_value'].sum() if len(df) > 8 else 0
        
        labels = list(top_holdings['symbol'])
        values = list(top_holdings['market_value'])
        
        if others_value > 0:
            labels.append('Others')
            values.append(others_value)
        
        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=0.3
        )])
        
        fig.update_layout(
            title="Holdings Allocation",
            height=400
        )
        
        return fig
    
    def create_sector_chart(self, df):
        """Create sector allocation chart"""
        if df.empty or 'sector' not in df.columns:
            return {}
        
        sector_allocation = df.groupby('sector')['market_value'].sum().reset_index()
        
        fig = go.Figure(data=[go.Bar(
            x=sector_allocation['sector'],
            y=sector_allocation['market_value'],
            text=sector_allocation['market_value'],
            texttemplate='$%{text:,.0f}',
            textposition='auto'
        )])
        
        fig.update_layout(
            title="Sector Allocation",
            xaxis_title="Sector",
            yaxis_title="Value ($)",
            height=400
        )
        
        return fig
    
    def create_activity_table(self, df):
        """Create recent activity table"""
        from dash import html
        
        if df.empty:
            return html.Div("No recent activity")
        
        # Format recent trades
        recent_trades = df.head(10)
        
        table_rows = []
        for _, trade in recent_trades.iterrows():
            table_rows.append(html.Tr([
                html.Td(trade['timestamp'].strftime('%H:%M:%S') if hasattr(trade['timestamp'], 'strftime') else str(trade['timestamp'])),
                html.Td(trade['symbol']),
                html.Td(trade['action']),
                html.Td(f"{trade['quantity']:,}"),
                html.Td(f"${trade['price']:.2f}")
            ]))
        
        return html.Table([
            html.Thead([
                html.Tr([
                    html.Th("Time"),
                    html.Th("Symbol"),
                    html.Th("Action"),
                    html.Th("Quantity"),
                    html.Th("Price")
                ])
            ]),
            html.Tbody(table_rows)
        ], className="table table-striped")
    
    def run(self, host='localhost', port=8050, debug=False):
        """Run the dashboard server"""
        try:
            logger.info(f"🌐 Starting dashboard server...")
            logger.info(f"📊 Dashboard will be available at: http://{host}:{port}")
            
            # Use the updated method name
            self.app.run(host=host, port=port, debug=debug)
            
        except Exception as e:
            logger.error(f"❌ Failed to start dashboard: {e}")
            raise

def main():
    """Main entry point"""
    print("🌐 Starting Factor Monitoring Dashboard...")
    
    try:
        # Import numpy for sample data
        import numpy as np
        globals()['np'] = np  # Make np available globally
        
        # Create and run dashboard
        dashboard = FactorMonitoringDashboard()
        print("📊 Dashboard available at: http://localhost:8050")
        dashboard.run(debug=True)
        
    except ImportError as e:
        print(f"❌ Missing required package: {e}")
        print("💡 Install with: pip install dash dash-bootstrap-components plotly pandas numpy")
    except KeyboardInterrupt:
        print("\n👋 Dashboard stopped by user")
    except Exception as e:
        print(f"❌ Dashboard error: {e}")

if __name__ == "__main__":
    main()
