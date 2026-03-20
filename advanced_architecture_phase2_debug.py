#!/usr/bin/env python3
"""
【デバッグ版】Stock Analysis Platform v3.0 - Phase 2
J-Quants API V2 のレスポンスを詳細にログ出力
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
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

class JQuantsDataFetcher:
    """J-Quants API V2 からデータを取得（デバッグ版）"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.jquants.com/v2'
        self.headers = {'x-api-key': api_key}
        self.request_count = 0
        self.window_start = time.time()
    
    def fetch_daily_bars(self, code, days=7300):  # 20年 ≈ 7300日
        """日足データを取得（20年分一括）（デバッグ版）"""
        try:
            url = f'{self.base_url}/equities/bars/daily'
            params = {
                'code': code,
                'from': '2004-01-01',  # 過去20年分
                'to': datetime.now().strftime('%Y-%m-%d')
            }
            
            logger.debug(f"📡 {code}: リクエスト送信 → {url}?{params}")
            
            resp = requests.get(url, params=params, headers=self.headers, timeout=15)
            
            # レスポンス情報をログ出力
            logger.debug(f"📊 {code}: ステータス = {resp.status_code}")
            logger.debug(f"🔑 {code}: レート制限 = {resp.headers.get('X-RateLimit-Remaining', 'N/A')}/{resp.headers.get('X-RateLimit-Limit', 'N/A')}")
            
            resp.raise_for_status()
            
            data = resp.json()
            logger.debug(f"📦 {code}: レスポンス キー = {list(data.keys())}")
            
            if data.get('data'):
                logger.info(f"✅ {code}: {len(data['data'])} 件取得")
                df = pd.DataFrame(data['data'])
                logger.debug(f"📋 {code}: カラム = {df.columns.tolist()}")
                return df
            else:
                logger.warning(f"⚠️ {code}: data キーが空または存在しない")
                logger.debug(f"📦 {code}: レスポンス全体 = {json.dumps(data, indent=2)[:500]}")
                return None
        
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ {code} HTTP エラー: {e.response.status_code} - {e.response.text[:200]}")
            return None
        except Exception as e:
            logger.error(f"❌ {code} データ取得失敗: {e}")
            return None

def main():
    """メイン処理（最初の 5 社のみテスト）"""
    
    try:
        # 1. 設定ファイルを読み込み
        logger.info("📖 config_phase2.json を読み込み中...")
        with open('config_phase2.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # 2. API キーを取得
        api_key = os.getenv('JQUANTS_API_KEY')
        if not api_key:
            logger.error("❌ JQUANTS_API_KEY が設定されていません")
            return False
        
        # 3. 企業リストを取得（最初の 5 社のみテスト）
        companies_list = config['companies']['data'][:5]
        logger.info(f"📊 テスト企業数: {len(companies_list)}")
        
        # 4. データ取得テスト
        fetcher = JQuantsDataFetcher(api_key)
        
        logger.info("=" * 70)
        logger.info("🔍 J-Quants API V2 デバッグテスト開始")
        logger.info("=" * 70)
        
        for company in companies_list:
            code = company['code']
            name = company['name']
            
            logger.info(f"\n🎯 テスト: {code} {name}")
            
            df = fetcher.fetch_daily_bars(code)
            
            if df is not None:
                logger.info(f"✅ 成功: {len(df)} 行取得")
                logger.info(f"   最新日付: {df.iloc[-1].get('Date', 'N/A')}")
                logger.info(f"   最新終値: {df.iloc[-1].get('AdjC', 'N/A')}")
            else:
                logger.warning(f"❌ 失敗: データなし")
            
            time.sleep(0.5)
        
        logger.info("\n" + "=" * 70)
        logger.info("✅ デバッグテスト完了")
        logger.info("=" * 70)
        
        return True
    
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
