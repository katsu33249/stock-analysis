#!/usr/bin/env python3
"""
EDINET DB API v2 からデータ取得（キャッシュ機能付き）
18時間経過したデータのみ再取得して API 制限を回避
"""

import os
import requests
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class EDINETDBCachedFetcher:
    """EDINET DB API v2 からデータを取得（キャッシュ機能付き）"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://edinetdb.jp/api/v2'
        self.headers = {'Authorization': f'Bearer {api_key}'}
        self.cache_file = 'edinet_cache.csv'
        self.cache_df = self._load_cache()
    
    def _load_cache(self):
        """キャッシュ CSV を読み込み"""
        if os.path.exists(self.cache_file):
            try:
                df = pd.read_csv(self.cache_file)
                logger.info(f"📂 キャッシュ読み込み: {len(df)} 件")
                return df
            except Exception as e:
                logger.warning(f"⚠️ キャッシュ読み込み失敗: {e}")
                return pd.DataFrame(columns=['code', 'name', 'roe', 'pbr', 'per', 'dividend_yield', 'eps_growth', 'doe', 'last_updated'])
        else:
            logger.info("📂 キャッシュなし（初回実行）")
            return pd.DataFrame(columns=['code', 'name', 'roe', 'pbr', 'per', 'dividend_yield', 'eps_growth', 'doe', 'last_updated'])
    
    def _is_cache_expired(self, last_updated_str):
        """キャッシュが18時間経過したか判定"""
        try:
            last_updated = datetime.fromisoformat(last_updated_str)
            elapsed = datetime.now() - last_updated
            return elapsed > timedelta(hours=18)
        except:
            return True
    
    def _needs_update(self, code):
        """企業コードが更新対象かチェック"""
        cached = self.cache_df[self.cache_df['code'] == code]
        
        if len(cached) == 0:
            return True  # キャッシュなし → 取得必要
        
        last_updated = cached.iloc[0]['last_updated']
        return self._is_cache_expired(last_updated)
    
    def get_company_financials(self, code):
        """企業の財務情報を取得"""
        try:
            url = f'{self.base_url}/companies/{code}'
            
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            
            # レスポンスから財務指標を抽出
            if data.get('success'):
                company = data.get('data', {})
                return {
                    'roe': float(company.get('roe', 10.0)),
                    'pbr': float(company.get('pbr', 1.0)),
                    'per': float(company.get('per', 15.0)),
                    'dividend_yield': float(company.get('dividend_yield', 2.0)),
                    'eps_growth': float(company.get('eps_growth', 5.0)),
                    'doe': float(company.get('doe', 1.0))
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"⚠️ {code} API 呼び出し失敗: {e}")
            return None
    
    def enrich_company_data(self, companies):
        """企業データを充実させる（キャッシュ優先、必要時のみ API 呼び出し）"""
        logger.info("📊 企業データを充実させる中...")
        
        api_calls = 0
        cache_hits = 0
        enriched_companies = []
        new_rows = []
        
        for idx, company in enumerate(companies):
            code = company['code']
            name = company['name']
            
            if (idx + 1) % 10 == 0:
                logger.info(f"進捗: {idx + 1}/{len(companies)} (API呼び出し: {api_calls}, キャッシュ: {cache_hits})")
            
            # キャッシュをチェック
            if not self._needs_update(code):
                # キャッシュが有効 → キャッシュから読み込み
                cached = self.cache_df[self.cache_df['code'] == code].iloc[0]
                company['metadata'].update({
                    'roe': float(cached['roe']),
                    'pbr': float(cached['pbr']),
                    'per': float(cached['per']),
                    'dividend_yield': float(cached['dividend_yield']),
                    'eps_growth': float(cached['eps_growth']),
                    'doe': float(cached['doe']),
                    'edinet_updated': cached['last_updated']
                })
                cache_hits += 1
                logger.debug(f"✅ {code}: キャッシュから読み込み")
            else:
                # API から新規取得
                financials = self.get_company_financials(code)
                
                if financials:
                    company['metadata'].update(financials)
                    company['metadata']['edinet_updated'] = datetime.now().isoformat()
                    
                    # キャッシュに追加
                    new_rows.append({
                        'code': code,
                        'name': name,
                        'roe': financials['roe'],
                        'pbr': financials['pbr'],
                        'per': financials['per'],
                        'dividend_yield': financials['dividend_yield'],
                        'eps_growth': financials['eps_growth'],
                        'doe': financials['doe'],
                        'last_updated': datetime.now().isoformat()
                    })
                    
                    api_calls += 1
                    logger.info(f"📡 {code} {name}: ROE={financials['roe']:.1f}%, PBR={financials['pbr']:.2f}")
                else:
                    logger.warning(f"⚠️ {code}: API 取得失敗、デフォルト値を使用")
                    company['metadata']['edinet_updated'] = datetime.now().isoformat()
                
                time.sleep(0.2)  # API レート制限対応
            
            enriched_companies.append(company)
        
        # キャッシュを更新
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # 古いデータを削除して新しいデータを追加
            self.cache_df = self.cache_df[~self.cache_df['code'].isin([row['code'] for row in new_rows])]
            self.cache_df = pd.concat([self.cache_df, new_df], ignore_index=True)
        
        # CSV に保存
        self.cache_df.to_csv(self.cache_file, index=False, encoding='utf-8')
        logger.info(f"💾 キャッシュを保存: {self.cache_file}")
        
        logger.info("=" * 70)
        logger.info(f"📊 API 呼び出し: {api_calls} 件")
        logger.info(f"✅ キャッシュ使用: {cache_hits} 件")
        logger.info(f"📂 キャッシュ総数: {len(self.cache_df)} 件")
        logger.info("=" * 70)
        
        return enriched_companies

def main():
    """メイン処理"""
    
    # 1. API キーを取得
    api_key = os.getenv('EDINETDB_API_KEY')
    if not api_key:
        logger.error("❌ EDINETDB_API_KEY が設定されていません")
        return False
    
    # 2. config_phase2.json を読み込み
    logger.info("📖 config_phase2.json を読み込み中...")
    try:
        with open('config_phase2.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error("❌ config_phase2.json が見つかりません")
        return False
    
    companies = config['companies']['data']
    logger.info(f"📊 企業数: {len(companies)}")
    
    # 3. キャッシュ機能付きで EDINET DB データを取得・統合
    fetcher = EDINETDBCachedFetcher(api_key)
    enriched_companies = fetcher.enrich_company_data(companies)
    
    # 4. 更新した config を保存
    config['companies']['data'] = enriched_companies
    config['system']['last_edinet_update'] = datetime.now().isoformat()
    
    with open('config_phase2.json', 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    logger.info("=" * 70)
    logger.info("✅ config_phase2.json に EDINET DB データを統合完了")
    logger.info("=" * 70)
    
    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
