#!/usr/bin/env python3
"""
J-Quants データ管理・キャッシュシステム
20年分データをParquetで管理・差分更新
"""

import os
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class JQuantsDataManager:
    """J-Quants データキャッシュ管理"""
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://api.jquants.com/v2'
        self.headers = {'x-api-key': api_key}
        self.cache_dir = 'jquants_cache'
        self.cache_file = f'{self.cache_dir}/all_stocks_data.parquet'
        
        # キャッシュディレクトリ作成
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # 全社データをロード
        self.df_all = self._load_cache()
    
    def _load_cache(self):
        """Parquet キャッシュを読み込み"""
        if os.path.exists(self.cache_file):
            try:
                df = pd.read_parquet(self.cache_file)
                logger.info(f"📂 キャッシュ読み込み: {len(df)} 件（{df['Code'].nunique()} 社）")
                logger.info(f"   日付範囲: {df['Date'].min()} ～ {df['Date'].max()}")
                return df
            except Exception as e:
                logger.warning(f"⚠️ キャッシュ読み込み失敗: {e}")
                return pd.DataFrame()
        else:
            logger.info("📂 キャッシュなし（初回実行）")
            return pd.DataFrame()
    
    def _save_cache(self):
        """Parquet キャッシュを保存"""
        try:
            self.df_all.to_parquet(self.cache_file, index=False, compression='snappy')
            logger.info(f"💾 キャッシュ保存: {len(self.df_all)} 件")
        except Exception as e:
            logger.error(f"❌ キャッシュ保存失敗: {e}")
    
    def _get_last_cached_date(self, code):
        """企業の最後のキャッシュ日付を取得"""
        if len(self.df_all) == 0:
            return None
        
        code_data = self.df_all[self.df_all['Code'] == code]
        if len(code_data) == 0:
            return None
        
        return code_data['Date'].max()
    
    def fetch_company_data(self, code, fetch_full=False):
        """企業データを取得（差分更新対応）"""
        try:
            if fetch_full:
                # 初回：20年分全データ
                from_date = '2004-01-01'
                logger.info(f"📡 {code}: 全データ取得（2004年～現在）")
            else:
                # 2回目以降：前日以降のみ
                last_date = self._get_last_cached_date(code)
                if last_date is None:
                    from_date = '2004-01-01'
                    logger.info(f"📡 {code}: 新規取得（キャッシュなし）")
                else:
                    # 前営業日の次の日から取得
                    from_date = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"📡 {code}: 差分取得（{from_date} ～）")
            
            url = f'{self.base_url}/equities/bars/daily'
            params = {
                'code': code,
                'from': from_date,
                'to': datetime.now().strftime('%Y-%m-%d')
            }
            
            resp = requests.get(url, params=params, headers=self.headers, timeout=15)
            resp.raise_for_status()
            
            data = resp.json()
            if not data.get('data'):
                logger.warning(f"⚠️ {code}: レスポンスが空")
                return None
            
            df_new = pd.DataFrame(data['data'])
            logger.info(f"✅ {code}: {len(df_new)} 件取得")
            
            return df_new
        
        except Exception as e:
            logger.error(f"❌ {code} API 取得失敗: {e}")
            return None
    
    def update_company_data(self, code, fetch_full=False):
        """企業データをキャッシュに追加・更新"""
        df_new = self.fetch_company_data(code, fetch_full)
        
        if df_new is None:
            return False
        
        # 既存データと新データをマージ
        if len(self.df_all) > 0:
            # 重複を削除（新データで上書き）
            self.df_all = self.df_all[self.df_all['Code'] != code]
        
        # 新データを追加
        self.df_all = pd.concat([self.df_all, df_new], ignore_index=True)
        
        return True
    
    def enrich_all_companies(self, companies, fetch_full=False):
        """全企業のデータを取得・更新"""
        logger.info("=" * 70)
        logger.info("🔍 J-Quants データキャッシュ更新開始")
        logger.info("=" * 70)
        
        success_count = 0
        
        for idx, company in enumerate(companies):
            code = company['code']
            name = company['name']
            
            if (idx + 1) % 10 == 0:
                logger.info(f"進捗: {idx + 1}/{len(companies)}")
            
            if self.update_company_data(code, fetch_full):
                success_count += 1
            
            time.sleep(0.2)  # API レート制限対応
        
        # キャッシュを保存
        self._save_cache()
        
        logger.info("=" * 70)
        logger.info(f"✅ キャッシュ更新完了: {success_count}/{len(companies)} 社")
        logger.info(f"📊 キャッシュ総件数: {len(self.df_all)} 件")
        logger.info(f"🗓️ 日付範囲: {self.df_all['Date'].min()} ～ {self.df_all['Date'].max()}")
        logger.info("=" * 70)
        
        return True
    
    def get_company_data(self, code, days=100):
        """キャッシュから企業データを取得"""
        if len(self.df_all) == 0:
            logger.warning(f"⚠️ {code}: キャッシュが空")
            return None
        
        df = self.df_all[self.df_all['Code'] == code].copy()
        
        if len(df) == 0:
            logger.warning(f"⚠️ {code}: キャッシュにデータなし")
            return None
        
        # 最新 N 日分を返す
        df = df.sort_values('Date').tail(days)
        logger.debug(f"✅ {code}: キャッシュから {len(df)} 件取得")
        
        return df

def main():
    """メイン処理"""
    
    # 1. API キーを取得
    api_key = os.getenv('JQUANTS_API_KEY')
    if not api_key:
        logger.error("❌ JQUANTS_API_KEY が設定されていません")
        return False
    
    # 2. config_phase2.json を読み込み
    logger.info("📖 config_phase2.json を読み込み中...")
    with open('config_phase2.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    companies = config['companies']['data']
    logger.info(f"📊 企業数: {len(companies)}")
    
    # 3. データマネージャーを初期化
    manager = JQuantsDataManager(api_key)
    
    # 4. キャッシュ判定
    if len(manager.df_all) == 0:
        logger.info("🆕 初回実行 → 20年分全データを取得します")
        fetch_full = True
    else:
        logger.info("📂 キャッシュあり → 差分更新を実行します")
        fetch_full = False
    
    # 5. 全企業のデータを更新
    manager.enrich_all_companies(companies, fetch_full=fetch_full)
    
    logger.info("\n✅ J-Quants データキャッシュ管理が完了しました")
    logger.info(f"次から advanced_architecture_phase2.py を実行してください")
    
    return True

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
