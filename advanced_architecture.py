#!/usr/bin/env python3
"""
【本番版】Stock Analysis Platform v3.0
J-Quants API から実データを取得して爆発初動株を検出
"""

import os
import json
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class JQuantsDataFetcher:
    """J-Quants API からデータを取得"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.jquants.com/v2'
        self.headers = {'x-api-key': api_key}
        self.request_count = 0
        self.window_start = time.time()
    
    def wait_rate_limit(self):
        """レート制限管理（1分あたり25リクエスト）"""
        now = time.time()
        if now - self.window_start > 60:
            self.request_count = 0
            self.window_start = now
        
        if self.request_count >= 25:
            sleep_time = 60 - (now - self.window_start)
            if sleep_time > 0:
                logger.warning(f"🔄 レート制限に到達。{sleep_time:.1f}秒待機中...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.window_start = time.time()
        
        self.request_count += 1
    
    def fetch_daily_bars(self, code, days=100):
        """日足データを取得"""
        try:
            self.wait_rate_limit()
            
            url = f'{self.base_url}/equities/bars/daily'
            params = {
                'code': code,
                'from': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'to': datetime.now().strftime('%Y-%m-%d')
            }
            
            resp = requests.get(url, params=params, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            if not data.get('data'):
                return None
            
            df = pd.DataFrame(data['data'])
            logger.debug(f"📊 {code}: {len(df)} 件取得")
            
            return df
        
        except Exception as e:
            logger.warning(f"⚠️ {code} データ取得失敗: {e}")
            return None

class ExplosionStockDetector:
    """爆発初動株検出エンジン"""
    
    def __init__(self):
        self.thresholds = {
            'volume_ratio': 2.0,
            'body_strength': 3.0,
            'price_threshold': 500,
            'trading_value': 1e9
        }
    
    def detect(self, code, name, df):
        """爆発初動株を検出"""
        
        try:
            if df is None or len(df) < 21:
                return None
            
            latest = df.iloc[-1]
            prev_20 = df.iloc[-21:-1]
            
            score = 0
            details = []
            
            # 1. 出来高倍率（40点）
            avg_volume = prev_20['AdjVo'].mean()
            volume_ratio = latest['AdjVo'] / avg_volume if avg_volume > 0 else 0
            if volume_ratio >= self.thresholds['volume_ratio']:
                volume_score = min(40, (volume_ratio - 1) * 20)
                score += volume_score
                details.append(f"出来高倍率: {volume_ratio:.2f}倍 ({volume_score:.0f}点)")
            
            # 2. 高値ブレークアウト（25点）
            high_20 = prev_20['AdjH'].max()
            if latest['AdjH'] > high_20:
                breakout_pct = (latest['AdjH'] - high_20) / high_20 * 100
                breakout_score = min(25, breakout_pct * 2)
                score += breakout_score
                details.append(f"20日高値ブレイク: +{breakout_pct:.2f}% ({breakout_score:.0f}点)")
            
            # 3. ローソク足本体の強さ（15点）
            if latest['AdjO'] > 0:
                body_pct = abs(latest['AdjC'] - latest['AdjO']) / latest['AdjO'] * 100
                if body_pct >= self.thresholds['body_strength']:
                    body_score = min(15, body_pct * 5)
                    score += body_score
                    details.append(f"ローソク足本体: {body_pct:.2f}% ({body_score:.0f}点)")
            
            # 4. 出来高金額（10点）
            trading_value = latest['AdjVo'] * latest['AdjC']
            if trading_value >= self.thresholds['trading_value']:
                value_score = min(10, (trading_value / self.thresholds['trading_value']) * 5)
                score += value_score
                details.append(f"出来高金額: ¥{trading_value/1e9:.1f}B ({value_score:.0f}点)")
            
            # 5. トレンド位置（10点）
            if latest['AdjC'] > prev_20['AdjC'].mean():
                trend_score = 10
                score += trend_score
                details.append(f"トレンド: 上昇トレンド (10点)")
            
            if score >= 50:
                return {
                    'code': code,
                    'name': name,
                    'price': latest['AdjC'],
                    'volume': latest['AdjVo'],
                    'score': score,
                    'details': ' | '.join(details)
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"⚠️ {code} 検出処理エラー: {e}")
            return None

def main():
    """メイン処理"""
    
    try:
        # 1. 設定ファイルを読み込み
        logger.info("📖 config_phase1.json を読み込み中...")
        with open('config_phase1.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info(f"✅ 設定読み込み完了: {config['system']['name']}")
        
        # 2. API キーを取得
        api_key = os.getenv('JQUANTS_API_KEY')
        if not api_key:
            logger.error("❌ JQUANTS_API_KEY が設定されていません")
            return False
        
        # 3. 企業リストを取得
        companies = config['companies']['data'][:15]
        logger.info(f"📊 企業数: {len(companies)}")
        
        # 4. データ取得と検出
        fetcher = JQuantsDataFetcher(api_key)
        detector = ExplosionStockDetector()
        
        results = []
        
        logger.info("🔍 爆発初動株を検出中...")
        for company in companies:
            code = company['code']
            name = company['name']
            
            df = fetcher.fetch_daily_bars(code)
            result = detector.detect(code, name, df)
            
            if result:
                results.append(result)
                logger.info(f"🎯 {code} {name}: スコア {result['score']:.0f}")
            
            time.sleep(0.1)
        
        # 5. 結果を DataFrame に変換
        if results:
            df_results = pd.DataFrame(results)
            df_results = df_results.sort_values('score', ascending=False).reset_index(drop=True)
            df_results['順位'] = range(1, len(df_results) + 1)
        else:
            df_results = pd.DataFrame(columns=['順位', 'code', 'name', 'price', 'volume', 'score', 'details'])
        
        logger.info(f"✅ 検出完了: {len(df_results)} 件")
        
        # 6. ディレクトリを作成
        os.makedirs('results', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        os.makedirs('snapshots', exist_ok=True)
        
        # 7. Excel に出力
        output_file = 'results/phase1_results.xlsx'
        columns = ['順位', 'code', 'name', 'score', 'price', 'volume', 'details']
        df_output = df_results[columns]
        df_output.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '詳細']
        df_output.to_excel(output_file, index=False, sheet_name='爆発初動株')
        
        logger.info(f"✅ Excel 出力完了: {output_file}")
        
        # 8. JSON スナップショット保存
        snapshot_file = f"snapshots/phase1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        df_results.to_json(snapshot_file, orient='records', force_ascii=False)
        
        logger.info(f"✅ スナップショット保存: {snapshot_file}")
        
        # 9. ログ出力
        logger.info("=" * 70)
        logger.info("Phase 1 Screening 完了")
        logger.info("=" * 70)
        logger.info(f"企業データ抽出: ✅ 完了")
        logger.info(f"爆発初動検出: ✅ 完了 ({len(df_results)} 件)")
        logger.info(f"結果保存: ✅ 完了")
        logger.info(f"出力ファイル: {output_file}")
        
        print("\n【爆発初動株ランキング】")
        print(df_output.to_string(index=False))
        
        return True
    
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
