#!/usr/bin/env python3
"""
EDINET DB API v1 からデータ取得（キャッシュ機能付き）
18時間経過したデータのみ再取得して API 制限を回避
証券コード(4桁) → EDINET コード(E+5桁) 変換対応
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
    """EDINET DB API v1 からデータを取得（キャッシュ機能付き）"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://edinetdb.jp/v1'
        self.headers = {'X-API-Key': api_key}
        self.cache_file = 'edinet_cache.csv'
        self.cache_df = self._load_cache()
        self.api_call_count = 0
    
    def _load_cache(self):
        """キャッシュ CSV を読み込み"""
        if os.path.exists(self.cache_file):
            try:
                df = pd.read_csv(self.cache_file)
                logger.info(f"📂 キャッシュ読み込み: {len(df)} 件")
                return df
            except Exception as e:
                logger.warning(f"⚠️ キャッシュ読み込み失敗: {e}")
                return pd.DataFrame(columns=['sec_code', 'edinet_code', 'roe', 'pbr', 'per', 'dividend_yield', 'eps_growth', 'doe', 'last_updated'])
        else:
            logger.info("📂 キャッシュなし（初回実行）")
            return pd.DataFrame(columns=['sec_code', 'edinet_code', 'roe', 'pbr', 'per', 'dividend_yield', 'eps_growth', 'doe', 'last_updated'])
    
    def _is_cache_expired(self, last_updated_str):
        """キャッシュが18時間経過したか判定"""
        try:
            last_updated = datetime.fromisoformat(last_updated_str)
            elapsed = datetime.now() - last_updated
            return elapsed > timedelta(hours=18)
        except:
            return True
    
    def _needs_update(self, sec_code):
        """企業コードが更新対象かチェック"""
        cached = self.cache_df[self.cache_df['sec_code'] == sec_code]
        
        if len(cached) == 0:
            return True  # キャッシュなし → 取得必要
        
        last_updated = cached.iloc[0]['last_updated']
        return self._is_cache_expired(last_updated)
    
    def search_company(self, sec_code):
        """証券コードから EDINET コードを検索"""
        try:
            url = f'{self.base_url}/search'
            params = {'q': sec_code, 'limit': 1}
            
            resp = requests.get(url, params=params, headers={}, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            if data.get('data') and len(data['data']) > 0:
                return data['data'][0]['edinet_code']
            
            return None
        
        except Exception as e:
            logger.warning(f"⚠️ {sec_code} 検索失敗: {e}")
            return None
    
    def get_company_ratios(self, edinet_code):
        """EDINET コードから財務指標を取得"""
        try:
            url = f'{self.base_url}/companies/{edinet_code}/ratios'
            
            resp = requests.get(url, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            self.api_call_count += 1
            
            data = resp.json()
            if data.get('data') and len(data['data']) > 0:
                latest = data['data'][0]  # 最新年度
                
                return {
                    'roe': float(latest.get('roe', 10.0)) if latest.get('roe') else 10.0,
                    'pbr': float(latest.get('bps', 1.0)) if latest.get('bps') else 1.0,
                    'per': float(latest.get('per', 15.0)) if latest.get('per') else 15.0,
                    'dividend_yield': float(latest.get('dividend_yield', 2.0)) if latest.get('dividend_yield') else 2.0,
                    'eps_growth': float(latest.get('eps_growth', 5.0)) if latest.get('eps_growth') else 5.0,
                    'doe': float(latest.get('financial_leverage', 1.0)) if latest.get('financial_leverage') else 1.0  # 財務レバレッジ
                }
            
            return None
        
        except Exception as e:
            logger.warning(f"⚠️ {edinet_code} 財務指標取得失敗: {e}")
            return None
    
    def enrich_company_data(self, companies):
        """企業データを充実させる（キャッシュ優先、必要時のみ API 呼び出し）"""
        logger.info("📊 企業データを充実させる中...")
        
        cache_hits = 0
        enriched_companies = []
        new_rows = []
        
        for idx, company in enumerate(companies):
            sec_code = company['code']
            name = company['name']
            
            if (idx + 1) % 10 == 0:
                logger.info(f"進捗: {idx + 1}/{len(companies)} (API呼び出し: {self.api_call_count}, キャッシュ: {cache_hits})")
            
            # キャッシュをチェック
            if not self._needs_update(sec_code):
                # キャッシュが有効 → キャッシュから読み込み
                cached = self.cache_df[self.cache_df['sec_code'] == sec_code].iloc[0]
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
                logger.debug(f"✅ {sec_code}: キャッシュから読み込み")
            else:
                # EDINET コードを検索
                edinet_code = self.search_company(sec_code)
                
                if edinet_code:
                    # 財務指標を取得
                    ratios = self.get_company_ratios(edinet_code)
                    
                    if ratios:
                        company['metadata'].update(ratios)
                        company['metadata']['edinet_updated'] = datetime.now().isoformat()
                        
                        # キャッシュに追加
                        new_rows.append({
                            'sec_code': sec_code,
                            'edinet_code': edinet_code,
                            'roe': ratios['roe'],
                            'pbr': ratios['pbr'],
                            'per': ratios['per'],
                            'dividend_yield': ratios['dividend_yield'],
                            'eps_growth': ratios['eps_growth'],
                            'doe': ratios['doe'],
                            'last_updated': datetime.now().isoformat()
                        })
                        
                        logger.info(f"📡 {sec_code} {name}: ROE={ratios['roe']:.1f}%, 配当利回り={ratios['dividend_yield']:.1f}%, D/E={ratios['doe']:.2f}")
                    else:
                        logger.warning(f"⚠️ {sec_code}: 財務指標取得失敗、デフォルト値を使用")
                        company['metadata']['edinet_updated'] = datetime.now().isoformat()
                else:
                    logger.warning(f"⚠️ {sec_code}: EDINET コード検索失敗、デフォルト値を使用")
                    company['metadata']['edinet_updated'] = datetime.now().isoformat()
                
                time.sleep(0.1)  # API レート制限対応
            
            enriched_companies.append(company)
        
        # キャッシュを更新
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            # 古いデータを削除して新しいデータを追加
            self.cache_df = self.cache_df[~self.cache_df['sec_code'].isin([row['sec_code'] for row in new_rows])]
            self.cache_df = pd.concat([self.cache_df, new_df], ignore_index=True)
        
        # CSV に保存
        self.cache_df.to_csv(self.cache_file, index=False, encoding='utf-8')
        logger.info(f"💾 キャッシュを保存: {self.cache_file}")
        
        logger.info("=" * 70)
        logger.info(f"📊 API 呼び出し: {self.api_call_count} 件")
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
    logger.info("✅ config_phase2.json に EDINET DB v1 データを統合完了")
    logger.info("=" * 70)
    
    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
