#!/usr/bin/env python3
"""
EDINET DB API v1 デバッグ版
レスポンスを詳細ログ出力
"""

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class EDINETDataFetcher:
    """EDINET DB API v1 デバッグ版"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://edinetdb.jp/v1'
        self.headers = {'X-API-Key': api_key}
    
    def search_company(self, code):
        """証券コードから EDINET コードを検索"""
        try:
            url = f'{self.base_url}/search'
            params = {'q': code}
            
            logger.debug(f"🔍 {code}: 企業検索リクエスト")
            logger.debug(f"   URL: {url}")
            logger.debug(f"   パラメータ: {params}")
            
            resp = requests.get(url, params=params, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            logger.debug(f"📦 {code}: レスポンスキー = {list(data.keys())}")
            logger.debug(f"📦 {code}: レスポンス = {json.dumps(data, indent=2, ensure_ascii=False)[:500]}")
            
            if data.get('data'):
                edinet_code = data['data'][0].get('edinet_code')
                logger.info(f"✅ {code}: EDINET コード = {edinet_code}")
                return edinet_code
            else:
                logger.warning(f"⚠️ {code}: 検索結果なし")
                return None
        
        except Exception as e:
            logger.error(f"❌ {code} 検索失敗: {e}")
            return None
    
    def get_company_ratios(self, edinet_code):
        """EDINET コードから財務指標を取得"""
        try:
            url = f'{self.base_url}/companies/{edinet_code}/ratios'
            
            logger.debug(f"📊 {edinet_code}: 財務指標リクエスト")
            logger.debug(f"   URL: {url}")
            
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            logger.debug(f"📦 {edinet_code}: レスポンスキー = {list(data.keys())}")
            
            if data.get('data'):
                logger.debug(f"📦 {edinet_code}: data 件数 = {len(data['data'])}")
                if len(data['data']) > 0:
                    latest = data['data'][0]
                    logger.debug(f"📦 {edinet_code}: 最新データキー = {list(latest.keys())}")
                    logger.debug(f"📦 {edinet_code}: 最新データ = {json.dumps(latest, indent=2, ensure_ascii=False)}")
                    
                    # 実際のフィールド名を出力
                    logger.info(f"✅ {edinet_code}: 取得フィールド")
                    logger.info(f"   roe: {latest.get('roe')}")
                    logger.info(f"   de_ratio: {latest.get('de_ratio')}")
                    logger.info(f"   dividend_yield: {latest.get('dividend_yield')}")
                    logger.info(f"   per: {latest.get('per')}")
                    logger.info(f"   bps: {latest.get('bps')}")
                    
                    return latest
            else:
                logger.warning(f"⚠️ {edinet_code}: data キーが空")
                logger.debug(f"📦 {edinet_code}: フルレスポンス = {json.dumps(data, indent=2, ensure_ascii=False)[:500]}")
                return None
        
        except Exception as e:
            logger.error(f"❌ {edinet_code} API 失敗: {e}")
            return None

def main():
    """デバッグ用メイン処理"""
    
    api_key = os.getenv('EDINETDB_API_KEY')
    if not api_key:
        logger.error("❌ EDINETDB_API_KEY が設定されていません")
        return False
    
    fetcher = EDINETDataFetcher(api_key)
    
    # テスト企業（最初の 3 社）
    test_codes = ['6501', '7751', '9001']
    
    logger.info("=" * 70)
    logger.info("🔍 EDINET DB API v1 デバッグテスト開始")
    logger.info("=" * 70)
    
    for code in test_codes:
        logger.info(f"\n🎯 {code} テスト開始")
        
        # Step 1: 企業検索
        edinet_code = fetcher.search_company(code)
        
        if edinet_code:
            # Step 2: 財務指標取得
            ratios = fetcher.get_company_ratios(edinet_code)
            
            if ratios:
                logger.info(f"✅ {code}: 成功")
            else:
                logger.warning(f"⚠️ {code}: 財務指標取得失敗")
        else:
            logger.warning(f"⚠️ {code}: EDINET コード検索失敗")
    
    logger.info("\n" + "=" * 70)
    logger.info("✅ デバッグテスト完了")
    logger.info("=" * 70)
    
    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
