import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import sqlite3
import logging
from dataclasses import dataclass
import json
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

@dataclass
class AttributionResult:
    """Data class for attribution analysis results"""
    period: str
    total_return: float
    benchmark_return: float
    excess_return: float
    factor_contributions: Dict[str, float]
    sector_contributions: Dict[str, float]
    selection_effect: float
    allocation_effect: float
    interaction_effect: float
    risk_adjusted_metrics: Dict[str, float]

class PerformanceAttributionSystem:
    """
    Comprehensive performance attribution system for factor portfolios
    Analyzes returns, risk, and attribution across multiple dimensions
    """
    
    def __init__(self, db_path: str = "factor_data.db", benchmark: str = "SPY"):
        self.db_path = db_path
        self.benchmark = benchmark
        self.logger = logging.getLogger('PerformanceAttribution')
        
        # Factor ETF mappings
        self.factor_etfs = {
            'Value': 'VTV',
            'Growth': 'VUG',
            'Quality': 'QUAL',
            'Momentum': 'MTUM',
            'Low_Volatility': 'USMV',
            'Size': 'VB'
        }
        
        # Sector mappings for detailed attribution
        self.sector_etfs = {
            'Technology': 'XLK',
            'Healthcare': 'XLV',
            'Financials': 'XLF',
            'Consumer_Discretionary': 'XLY',
            'Industrials': 'XLI',
            'Energy': 'XLE',
            'Utilities': 'XLU',
            'Materials': 'XLB',
            'Consumer_Staples': 'XLP',
            'Real_Estate': 'XLRE',
            'Communications': 'XLC'
        }
        
        self.initialize_attribution_tables()
    
    def initialize_attribution_tables(self):
        """Initialize tables for attribution analysis storage"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS attribution_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    period_start TEXT NOT NULL,
                    period_end TEXT NOT NULL,
                    total_return REAL,
                    benchmark_return REAL,
                    excess_return REAL,
                    factor_contributions TEXT,
                    sector_contributions TEXT,
                    selection_effect REAL,
                    allocation_effect REAL,
                    interaction_effect REAL,
                    risk_metrics TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS daily_attribution (
                    date TEXT PRIMARY KEY,
                    portfolio_return REAL,
                    benchmark_return REAL,
                    excess_return REAL,
                    factor_contributions TEXT,
                    active_weights TEXT,
                    created_date TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.commit()
            conn.close()
            
            self.logger.info("Attribution tables initialized")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize attribution tables: {e}")
    
    def get_portfolio_returns(self, start_date: str, end_date: str, 
                            weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """Get portfolio returns based on factor allocations"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Get factor returns
            query = '''
                SELECT date, symbol, daily_return
                FROM factor_returns
                WHERE date BETWEEN ? AND ?
                AND symbol IN ({})
            '''.format(','.join('?' * len(self.factor_etfs.values())))
            
            params = [start_date, end_date] + list(self.factor_etfs.values())
            returns_df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            
            if returns_df.empty:
                self.logger.warning("No return data found for specified period")
                return pd.DataFrame()
            
            # Pivot to get factors as columns
            returns_pivot = returns_df.pivot(index='date', columns='symbol', values='daily_return')
            returns_pivot.index = pd.to_datetime(returns_pivot.index)
            
            # Map symbols back to factor names
            symbol_to_factor = {v: k for k, v in self.factor_etfs.items()}
            returns_pivot.columns = [symbol_to_factor.get(col, col) for col in returns_pivot.columns]
            
            # Apply weights to calculate portfolio returns
            if weights is None:
                # Equal weight if no weights provided
                weights = {factor: 1.0/len(self.factor_etfs) for factor in self.factor_etfs.keys()}
            
            # Calculate weighted portfolio returns
            portfolio_returns = pd.Series(index=returns_pivot.index, dtype=float)
            
            for date_idx in returns_pivot.index:
                daily_return = 0.0
                for factor, weight in weights.items():
                    if factor in returns_pivot.columns:
                        factor_return = returns_pivot.loc[date_idx, factor]
                        if not pd.isna(factor_return):
                            daily_return += weight * factor_return
                
                portfolio_returns[date_idx] = daily_return
            
            # Create result DataFrame
            result_df = returns_pivot.copy()
            result_df['Portfolio'] = portfolio_returns
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Failed to get portfolio returns: {e}")
            return pd.DataFrame()
    
    def get_benchmark_returns(self, start_date: str, end_date: str) -> pd.Series:
        """Get benchmark returns"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            query = '''
                SELECT date, daily_return
                FROM factor_returns
                WHERE date BETWEEN ? AND ?
                AND symbol = ?
            '''
            
            benchmark_df = pd.read_sql_query(query, conn, params=[start_date, end_date, self.benchmark])
            conn.close()
            
            if benchmark_df.empty:
                self.logger.warning(f"No benchmark data found for {self.benchmark}")
                return pd.Series()
            
            benchmark_series = benchmark_df.set_index('date')['daily_return']
            benchmark_series.index = pd.to_datetime(benchmark_series.index)
            
            return benchmark_series
            
        except Exception as e:
            self.logger.error(f"Failed to get benchmark returns: {e}")
            return pd.Series()
    
    def calculate_brinson_attribution(self, portfolio_returns: pd.DataFrame, 
                                    benchmark_returns: pd.Series,
                                    portfolio_weights: Dict[str, float],
                                    benchmark_weights: Optional[Dict[str, float]] = None) -> Dict:
        """
        Calculate Brinson-Hood-Beebower attribution analysis
        Decomposes excess return into allocation and selection effects
        """
        try:
            if benchmark_weights is None:
                # Assume equal weight benchmark for factors
                benchmark_weights = {factor: 1.0/len(self.factor_etfs) for factor in self.factor_etfs.keys()}
            
            # Calculate period returns
            portfolio_period_return = (portfolio_returns['Portfolio'] + 1).prod() - 1
            benchmark_period_return = (benchmark_returns + 1).prod() - 1
            
            # Calculate factor period returns
            factor_period_returns = {}
            for factor in portfolio_weights.keys():
                if factor in portfolio_returns.columns:
                    factor_period_returns[factor] = (portfolio_returns[factor] + 1).prod() - 1
            
            # Calculate allocation effect: (wp - wb) * rb
            allocation_effect = 0.0
            selection_effect = 0.0
            interaction_effect = 0.0
            
            factor_contributions = {}
            
            for factor in portfolio_weights.keys():
                wp = portfolio_weights[factor]  # Portfolio weight
                wb = benchmark_weights.get(factor, 0.0)  # Benchmark weight
                rp = factor_period_returns.get(factor, 0.0)  # Portfolio factor return
                rb = benchmark_period_return  # Use benchmark return as sector benchmark
                
                # Allocation effect: (wp - wb) * rb
                factor_allocation = (wp - wb) * rb
                allocation_effect += factor_allocation
                
                # Selection effect: wb * (rp - rb)
                factor_selection = wb * (rp - rb)
                selection_effect += factor_selection
                
                # Interaction effect: (wp - wb) * (rp - rb)
                factor_interaction = (wp - wb) * (rp - rb)
                interaction_effect += factor_interaction
                
                # Total factor contribution
                factor_contributions[factor] = factor_allocation + factor_selection + factor_interaction
            
            excess_return = portfolio_period_return - benchmark_period_return
            
            attribution_result = {
                'total_return': portfolio_period_return,
                'benchmark_return': benchmark_period_return,
                'excess_return': excess_return,
                'allocation_effect': allocation_effect,
                'selection_effect': selection_effect,
                'interaction_effect': interaction_effect,
                'factor_contributions': factor_contributions,
                'reconciliation': allocation_effect + selection_effect + interaction_effect
            }
            
            self.logger.info(f"Attribution analysis completed. Excess return: {excess_return:.4f}")
            
            return attribution_result
            
        except Exception as e:
            self.logger.error(f"Attribution calculation failed: {e}")
            return {}
    
    def calculate_factor_tilts_attribution(self, returns_df: pd.DataFrame, 
                                         weights: Dict[str, float]) -> Dict:
        """
        Calculate attribution based on factor tilts vs market-cap weighted approach
        """
        try:
            # Calculate equal-weighted benchmark (neutral factor exposure)
            equal_weights = {factor: 1.0/len(weights) for factor in weights.keys()}
            
            # Calculate returns for both approaches
            tilted_returns = []
            neutral_returns = []
            
            for date_idx in returns_df.index:
                tilted_return = 0.0
                neutral_return = 0.0
                
                for factor in weights.keys():
                    if factor in returns_df.columns:
                        factor_return = returns_df.loc[date_idx, factor]
                        if not pd.isna(factor_return):
                            tilted_return += weights[factor] * factor_return
                            neutral_return += equal_weights[factor] * factor_return
                
                tilted_returns.append(tilted_return)
                neutral_returns.append(neutral_return)
            
            tilted_series = pd.Series(tilted_returns, index=returns_df.index)
            neutral_series = pd.Series(neutral_returns, index=returns_df.index)
            
            # Calculate attribution by factor tilt
            tilt_attribution = {}
            
            for factor in weights.keys():
                if factor in returns_df.columns:
                    factor_returns = returns_df[factor]
                    weight_diff = weights[factor] - equal_weights[factor]
                    
                    # Attribution = (actual_weight - neutral_weight) * factor_return
                    factor_attribution = (factor_returns * weight_diff).sum()
                    tilt_attribution[factor] = factor_attribution
            
            total_tilt_effect = sum(tilt_attribution.values())
            
            result = {
                'tilted_portfolio_return': tilted_series.sum(),
                'neutral_portfolio_return': neutral_series.sum(),
                'total_tilt_effect': total_tilt_effect,
                'factor_tilt_contributions': tilt_attribution,
                'daily_excess_returns': tilted_series - neutral_series
            }
            
            return result
            
        except Exception as e:
            self.logger.error(f"Factor tilt attribution failed: {e}")
            return {}
    
    def calculate_risk_adjusted_metrics(self, portfolio_returns: pd.Series, 
                                      benchmark_returns: pd.Series,
                                      risk_free_rate: float = 0.02) -> Dict:
        """Calculate comprehensive risk-adjusted performance metrics"""
        try:
            # Ensure series are aligned
            aligned_data = pd.DataFrame({
                'portfolio': portfolio_returns,
                'benchmark': benchmark_returns
            }).dropna()
            
            if aligned_data.empty:
                return {}
            
            portfolio_ret = aligned_data['portfolio']
            benchmark_ret = aligned_data['benchmark']
            
            # Basic return metrics
            portfolio_total_return = (portfolio_ret + 1).prod() - 1
            benchmark_total_return = (benchmark_ret + 1).prod() - 1
            excess_return = portfolio_total_return - benchmark_total_return
            
            # Risk metrics
            portfolio_vol = portfolio_ret.std() * np.sqrt(252)  # Annualized
            benchmark_vol = benchmark_ret.std() * np.sqrt(252)
            
            # Tracking error
            tracking_error = (portfolio_ret - benchmark_ret).std() * np.sqrt(252)
            
            # Information ratio
            avg_excess_return = (portfolio_ret - benchmark_ret).mean() * 252
            information_ratio = avg_excess_return / tracking_error if tracking_error != 0 else 0
            
            # Sharpe ratios
            daily_rf = risk_free_rate / 252
            portfolio_sharpe = ((portfolio_ret - daily_rf).mean() * 252) / portfolio_vol if portfolio_vol != 0 else 0
            benchmark_sharpe = ((benchmark_ret - daily_rf).mean() * 252) / benchmark_vol if benchmark_vol != 0 else 0
