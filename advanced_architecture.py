#!/usr/bin/env python3
"""
【高度なアーキテクチャ】
Strategy パターン + Snapshot機能 + 正規化 + レート制限管理

このシステムは「今日の気分で計算ロジックを切り替える」ことで、
コード1行の変更もなく、複数の分析ツールを統一的に運用できます。
"""

import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Protocol, List, Optional, Dict
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
import json
import logging
from threading import Lock
import time

# ========== ログシステム（デバッグ・運用効率化） ==========

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# ========== 【1】API レート制限の共通管理 ==========

class RateLimiter:
    """
    複数のスクリーナーが同時に走った時に
    J-Quants API の制限に引っかからないようにする
    
    制限: 1分あたり30リクエスト（安全値: 25）
    """
    
    def __init__(self, max_requests: int = 25, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times = []
        self.lock = Lock()
    
    def wait_if_needed(self):
        """必要に応じて待機（複数スクリーナー間で共有）"""
        with self.lock:
            now = time.time()
            
            # ウィンドウ外の古いリクエスト時刻を削除
            self.request_times = [
                t for t in self.request_times
                if now - t < self.window_seconds
            ]
            
            # 制限に達していたら待機
            if len(self.request_times) >= self.max_requests:
                sleep_time = self.window_seconds - (now - self.request_times[0])
                if sleep_time > 0:
                    logger.warning(f"🔄 API 制限に到達。{sleep_time:.1f}秒待機中...")
                    time.sleep(sleep_time)
            
            # リクエスト記録
            self.request_times.append(now)

# グローバルなレート制限管理（全スクリーナー共通）
GLOBAL_RATE_LIMITER = RateLimiter()

# ========== 【2】Strategy パターン（計算ロジック注入） ==========

class ScoringStrategy(Protocol):
    """
    スコア計算のインターフェース
    
    異なる計算ロジックを「プラグイン」できる設計
    Screener本体は計算の中身を知らない = 疎結合
    """
    
    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        スコアを計算して返す
        
        Args:
            df: 企業データ（必要な列が全て揃っている）
        
        Returns:
            スコアの Series（各企業のスコア）
        """
        ...
    
    def get_name(self) -> str:
        """戦略の名前を返す"""
        ...

class DividendStrategy:
    """配当株スクリーニング用戦略"""
    
    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        配当利回り重視のスコア計算
        """
        logger.info("📊 DividendStrategy を適用中...")
        
        # 正規化（後述）を使って 0-100 に統一
        normalized_yield = self._normalize(df['dividend_yield'], 0, 10)
        normalized_payout = self._normalize(df['payout_ratio'], 30, 80)
        normalized_roe = self._normalize(df['roe'], 5, 20)
        normalized_doe = self._normalize(1 / (df['doe'] + 0.1), 0, 1)  # DOE は低い方が良い
        
        score = (
            normalized_yield * 0.40 +
            normalized_roe * 0.15 +
            (100 - normalized_payout) * 0.10 +  # 配当性向は低い方が良い
            normalized_doe * 0.10 +
            self._compute_fcf_coverage(df) * 0.25
        )
        
        logger.debug(f"スコア計算完了: {len(df)} 件")
        return score
    
    def get_name(self) -> str:
        return "DividendStrategy"
    
    @staticmethod
    def _normalize(series: pd.Series, min_val: float = None, max_val: float = None) -> pd.Series:
        """
        Min-Max正規化（0-100にスケーリング）
        外れ値の影響を減らすため、パーセンタイル値を使用
        """
        if min_val is None:
            min_val = series.quantile(0.05)  # 5パーセンタイル
        if max_val is None:
            max_val = series.quantile(0.95)  # 95パーセンタイル
        
        normalized = ((series - min_val) / (max_val - min_val)) * 100
        normalized = normalized.clip(0, 100)  # 0-100の範囲に制限
        
        return normalized
    
    @staticmethod
    def _compute_fcf_coverage(df: pd.DataFrame) -> pd.Series:
        """FCF で配当をカバーできているか"""
        fcf = df['fcf']
        dividend = df['dividend_per_share']
        
        # FCF / 配当 >= 1.0 なら満点
        coverage = (fcf / (dividend * 100 + 0.1)).clip(0, 1) * 100
        return coverage

class GrowthStrategy:
    """成長株スクリーニング用戦略"""
    
    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        EPS成長率重視のスコア計算
        """
        logger.info("📈 GrowthStrategy を適用中...")
        
        normalized_eps_growth = self._normalize(df['eps_growth'], 0, 50)
        normalized_revenue_growth = self._normalize(df['revenue_growth'], 0, 30)
        normalized_fcf_growth = self._normalize(df['fcf_growth'], -20, 50)
        normalized_roe = self._normalize(df['roe'], 5, 25)
        
        score = (
            normalized_eps_growth * 0.40 +
            normalized_revenue_growth * 0.20 +
            normalized_fcf_growth * 0.20 +
            normalized_roe * 0.20
        )
        
        logger.debug(f"スコア計算完了: {len(df)} 件")
        return score
    
    def get_name(self) -> str:
        return "GrowthStrategy"
    
    @staticmethod
    def _normalize(series: pd.Series, min_val: float = None, max_val: float = None) -> pd.Series:
        if min_val is None:
            min_val = series.quantile(0.05)
        if max_val is None:
            max_val = series.quantile(0.95)
        
        normalized = ((series - min_val) / (max_val - min_val)) * 100
        return normalized.clip(0, 100)

class ValueStrategy:
    """割安株スクリーニング用戦略"""
    
    def compute(self, df: pd.DataFrame) -> pd.Series:
        """
        PER/PBR 重視のスコア計算
        """
        logger.info("💎 ValueStrategy を適用中...")
        
        # PER は低い方が良い → 逆順で正規化
        normalized_per = 100 - self._normalize(df['per'], 10, 40)
        normalized_pbr = 100 - self._normalize(df['pbr'], 0.5, 3.0)
        normalized_roe = self._normalize(df['roe'], 5, 20)
        normalized_doe = 100 - self._normalize(df['doe'], 0.5, 2.0)
        
        score = (
            normalized_per * 0.35 +
            normalized_pbr * 0.25 +
            normalized_roe * 0.20 +
            normalized_doe * 0.20
        )
        
        logger.debug(f"スコア計算完了: {len(df)} 件")
        return score
    
    def get_name(self) -> str:
        return "ValueStrategy"
    
    @staticmethod
    def _normalize(series: pd.Series, min_val: float = None, max_val: float = None) -> pd.Series:
        if min_val is None:
            min_val = series.quantile(0.05)
        if max_val is None:
            max_val = series.quantile(0.95)
        
        normalized = ((series - min_val) / (max_val - min_val)) * 100
        return normalized.clip(0, 100)

# ========== 【3】Snapshot機能（時系列比較） ==========

@dataclass
class ScreenerSnapshot:
    """
    特定時点での スクリーニング結果を保存
    
    「3ヶ月前のスコアと現在のスコアの差分」を簡単に出す
    """
    timestamp: datetime
    strategy_name: str
    results: pd.DataFrame  # code, name, score, ... を含む
    
    def to_dict(self) -> dict:
        """JSON保存用に辞書化"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'strategy_name': self.strategy_name,
            'results': self.results.to_dict('records')
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ScreenerSnapshot':
        """JSON から復元"""
        return cls(
            timestamp=datetime.fromisoformat(data['timestamp']),
            strategy_name=data['strategy_name'],
            results=pd.DataFrame(data['results'])
        )

class SnapshotManager:
    """
    複数のスナップショットを管理
    「スコアの変動」を追跡可能にする
    """
    
    def __init__(self, storage_path: str = './snapshots'):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)
        self.snapshots: List[ScreenerSnapshot] = []
        self._load_snapshots()
    
    def save_snapshot(self, snapshot: ScreenerSnapshot):
        """スナップショットを保存"""
        self.snapshots.append(snapshot)
        
        filename = f"{snapshot.strategy_name}_{snapshot.timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(self.storage_path, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(snapshot.to_dict(), f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 スナップショット保存: {filepath}")
    
    def _load_snapshots(self):
        """保存済みのスナップショットを読み込み"""
        for filename in os.listdir(self.storage_path):
            if filename.endswith('.json'):
                filepath = os.path.join(self.storage_path, filename)
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.snapshots.append(ScreenerSnapshot.from_dict(data))
                except Exception as e:
                    logger.warning(f"スナップショット読み込み失敗: {filepath} - {e}")
    
    def get_score_changes(self, 
                         strategy_name: str,
                         code: str,
                         days_ago: int = 30) -> Optional[Dict]:
        """
        特定の銘柄のスコア変動を取得
        
        例: 「FPG のスコアが 30日前は 70点だったのに、今は 85点」
        """
        # 対象期間のスナップショットを取得
        recent_snapshots = [
            s for s in self.snapshots
            if s.strategy_name == strategy_name and
            (datetime.now() - s.timestamp).days <= days_ago
        ]
        
        if len(recent_snapshots) < 2:
            logger.warning(f"比較用のスナップショットが不足: {code}")
            return None
        
        # 最古と最新を取得
        oldest = sorted(recent_snapshots, key=lambda s: s.timestamp)[0]
        newest = recent_snapshots[-1]
        
        # 銘柄のスコアを取得
        old_score = oldest.results[oldest.results['code'] == code]['score'].values
        new_score = newest.results[newest.results['code'] == code]['score'].values
        
        if len(old_score) == 0 or len(new_score) == 0:
            return None
        
        old_score = old_score[0]
        new_score = new_score[0]
        
        return {
            'code': code,
            'old_score': float(old_score),
            'new_score': float(new_score),
            'change': float(new_score - old_score),
            'change_pct': float((new_score - old_score) / old_score * 100),
            'days': (datetime.now() - oldest.timestamp).days
        }
    
    def detect_score_jumps(self, 
                          strategy_name: str,
                          threshold: int = 20) -> List[Dict]:
        """
        スコアが大きく変わった銘柄を検出
        
        例: 「過去30日でスコアが20点以上上昇した銘柄」
        """
        recent_snapshots = [
            s for s in self.snapshots
            if s.strategy_name == strategy_name
        ]
        
        if len(recent_snapshots) < 2:
            return []
        
        newest = recent_snapshots[-1]
        older = recent_snapshots[-2]  # 1つ前
        
        # マージして変動を計算
        merged = newest.results.merge(
            older.results,
            on='code',
            suffixes=('_new', '_old')
        )
        
        merged['score_change'] = merged['score_new'] - merged['score_old']
        
        # 変動が大きい順にソート
        jumps = merged[abs(merged['score_change']) >= threshold].copy()
        jumps = jumps.sort_values('score_change', ascending=False)
        
        return jumps[['code', 'name_new', 'score_old', 'score_new', 'score_change']].to_dict('records')

# ========== 【4】基底クラス（すべてのスクリーナーの親） ==========

class BaseScreener(ABC):
    """
    すべてのスクリーナーの親クラス
    
    計算ロジックは Strategy で注入される（疎結合）
    データ取得・前処理・出力は共通化
    """
    
    def __init__(self, 
                 api_key: str,
                 strategy: ScoringStrategy,
                 snapshot_manager: Optional[SnapshotManager] = None):
        self.api_key = api_key
        self.strategy = strategy
        self.snapshot_manager = snapshot_manager or SnapshotManager()
        self.headers = {'x-api-key': api_key}
        self.base_url = 'https://api.jquants.com/v2'
        logger.info(f"📌 Screener 初期化: Strategy = {strategy.get_name()}")
    
    def screen(self, codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        スクリーニング実行（共通フロー）
        """
        logger.info(f"🔍 スクリーニング開始: {self.strategy.get_name()}")
        
        try:
            # 1. データ取得
            df_data = self._fetch_data(codes)
            
            if df_data is None or len(df_data) == 0:
                logger.error("データ取得失敗")
                return pd.DataFrame()
            
            # 2. 計算（Strategy に任せる）
            df_data['score'] = self.strategy.compute(df_data)
            
            # 3. ソート
            results = df_data.sort_values('score', ascending=False).reset_index(drop=True)
            results['rank'] = range(1, len(results) + 1)
            
            # 4. スナップショット保存
            snapshot = ScreenerSnapshot(
                timestamp=datetime.now(),
                strategy_name=self.strategy.get_name(),
                results=results
            )
            self.snapshot_manager.save_snapshot(snapshot)
            
            logger.info(f"✅ スクリーニング完了: {len(results)} 件")
            
            return results
        
        except Exception as e:
            logger.error(f"❌ スクリーニング失敗: {e}")
            return pd.DataFrame()
    
    def _fetch_data(self, codes: Optional[List[str]]) -> Optional[pd.DataFrame]:
        """データ取得（サブクラスで実装）"""
        raise NotImplementedError
    
    def detect_watchlist_changes(self, days: int = 30, threshold: int = 20) -> List[Dict]:
        """ウォッチリスト機能: スコアが大きく変わった銘柄を検出"""
        logger.info(f"👀 ウォッチリスト更新: {days}日間で{threshold}点以上変動した銘柄を検出")
        return self.snapshot_manager.detect_score_jumps(
            self.strategy.get_name(),
            threshold
        )

# ========== 【実装例】DividendScreener ==========

class DividendScreener(BaseScreener):
    """
    実装例: 配当株スクリーニング
    
    Strategy を指定するだけで、成長株にも割安株にも変身！
    """
    
    def _fetch_data(self, codes: Optional[List[str]]) -> Optional[pd.DataFrame]:
        """J-Quants からデータを取得"""
        try:
            url = f'{self.base_url}/equities'
            GLOBAL_RATE_LIMITER.wait_if_needed()
            
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            if not data.get('equities'):
                return None
            
            df = pd.DataFrame(data['equities']).head(500)
            logger.info(f"📊 {len(df)} 件の企業データを取得")
            
            return df
        
        except Exception as e:
            logger.error(f"データ取得エラー: {e}")
            return None

# ========== 【使用例】 ==========

def demo():
    """
    実装例のデモンストレーション
    
    同じ Screener で、計算ロジックだけ切り替える！
    """
    
    api_key = os.getenv('JQUANTS_API_KEY')
    if not api_key:
        print("⚠️ JQUANTS_API_KEY が設定されていません")
        return
    
    # Snapshot マネージャーを作成（複数のスクリーナー間で共有）
    snapshot_mgr = SnapshotManager()
    
    print("\n" + "=" * 70)
    print("【デモ】Strategy パターンの実装")
    print("=" * 70)
    
    # 例1: 配当株スクリーニング
    print("\n1️⃣ 配当株スクリーニング")
    print("-" * 70)
    screener_dividend = DividendScreener(
        api_key=api_key,
        strategy=DividendStrategy(),  # ← Strategy を指定
        snapshot_manager=snapshot_mgr
    )
    # results_dividend = screener_dividend.screen()
    
    # 例2: 同じ Screener で、Strategy だけ変更 → 成長株に！
    print("\n2️⃣ 成長株スクリーニング（同じ Screener を再利用）")
    print("-" * 70)
    screener_growth = DividendScreener(
        api_key=api_key,
        strategy=GrowthStrategy(),  # ← Strategy を変更するだけ！
        snapshot_manager=snapshot_mgr
    )
    # results_growth = screener_growth.screen()
    
    # 例3: 割安株スクリーニング
    print("\n3️⃣ 割安株スクリーニング（同じ Screener を再利用）")
    print("-" * 70)
    screener_value = DividendScreener(
        api_key=api_key,
        strategy=ValueStrategy(),  # ← Strategy を変更するだけ！
        snapshot_manager=snapshot_mgr
    )
    # results_value = screener_value.screen()
    
    print("\n" + "=" * 70)
    print("【メリット】")
    print("=" * 70)
    print("""
✅ コード1行も消さずに、計算ロジックを切り替え可能
✅ 新しい Strategy を追加しても、Screener 本体は変更不要
✅ テストも簡単（Strategy だけをユニットテストできる）
✅ 複数のスクリーニングを同時実行してもコード管理が簡単
✅ API 制限も一元管理

つまり：
  朝は配当株スクリーニング
  昼は成長株スクリーニング
  夜は割安株スクリーニング
  
  と、「計算ロジックだけ差し替え」で対応可能！ 🎛️
    """)
    
    # 例4: スコアの変動をウォッチ
    print("\n4️⃣ ウォッチリスト機能（スコア変動の追跡）")
    print("-" * 70)
    print("※ 過去のスナップショットが保存されていれば、以下が実行できます")
    print("""
    # 30日以内にスコアが20点以上上昇した銘柄を検出
    watchlist = screener_dividend.detect_watchlist_changes(
        days=30,
        threshold=20
    )
    
    for item in watchlist:
        print(f"🚀 {item['code']} のスコアが {item['score_change']:.0f}点上昇！")
    """)

if __name__ == '__main__':
    demo()
