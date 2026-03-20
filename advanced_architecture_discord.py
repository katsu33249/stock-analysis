#!/usr/bin/env python3
"""
【修正版】Stock Analysis Platform v3.0 - Phase 1
キャッシュ自動生成機能付き
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ExplosionStockDetector をインポート（Phase 2 から）
import sys
sys.path.insert(0, os.path.dirname(__file__))

from advanced_architecture_phase2 import ExplosionStockDetector

def send_to_discord(webhook_url, excel_file, df_results):
    """Discord に爆発初動株をテーブル形式で送信"""
    if not webhook_url or len(df_results) == 0:
        return
    
    try:
        import requests
        
        # テーブルヘッダー
        message = "🎯 **Phase 1 爆発初動株スクリーニング結果**\n\n"
        message += "```\n"
        
        # テーブル形式
        df_display = df_results[['順位', '企業コード', '企業名', 'スコア', '株価']].head(10).copy()
        message += df_display.to_string(index=False)
        message += "\n```\n"
        
        # ファイルアップロード
        with open(excel_file, 'rb') as f:
            files = {'file': f}
            data = {
                'content': message,
                'username': '爆発初動スクリーナー'
            }
            requests.post(webhook_url, data=data, files=files, timeout=10)
        
        logger.info(f"✅ Discord 送信完了")
    
    except Exception as e:
        logger.error(f"❌ Discord 送信失敗: {e}")

def main():
    """Phase 1 メイン処理"""
    
    try:
        # 1. 設定ファイルを読み込み
        logger.info("📖 config_phase1.json を読み込み中...")
        with open('config_phase1.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info("✅ 設定読み込み完了: Stock Analysis Platform v3.0 - Phase 1")
        
        companies_list = config['companies']['data']
        logger.info(f"📊 企業数: {len(companies_list)}")
        
        # 2. EDINET データを読み込み
        logger.info("📋 EDINET キャッシュを確認中...")
        if os.path.exists('edinet_cache.csv'):
            edinet_df = pd.read_csv('edinet_cache.csv')
            logger.info(f"✅ EDINET キャッシュ: {len(edinet_df)} 件")
        else:
            logger.warning("⚠️ EDINET キャッシュなし")
            edinet_df = pd.DataFrame()
        
        # 企業リストに EDINET データを統合
        for company in companies_list:
            code = company['code']
            edinet_data = edinet_df[edinet_df['sec_code'] == code]
            
            if len(edinet_data) > 0:
                row = edinet_data.iloc[0]
                company['metadata'] = {
                    'dividend_yield': row['dividend_yield'],
                    'eps_growth': row['eps_growth'],
                    'per': row['per'],
                    'pbr': row['pbr'],
                    'roe': row['roe'],
                    'doe': row['doe']
                }
            else:
                company['metadata'] = {
                    'dividend_yield': 0,
                    'eps_growth': 0,
                    'per': 0,
                    'pbr': 0,
                    'roe': 10,
                    'doe': 1
                }
        
        # 3. API キーを取得
        api_key = os.getenv('JQUANTS_API_KEY')
        if not api_key:
            logger.error("❌ JQUANTS_API_KEY が設定されていません")
            return False
        
        # 4. キャッシュマネージャーから検出
        # ※ キャッシュがなければ自動生成
        from jquants_data_manager import JQuantsDataManager
        
        manager = JQuantsDataManager(api_key)
        
        # キャッシュがなければ自動生成
        if len(manager.df_all) == 0:
            logger.warning("⚠️ キャッシュが空 → 自動生成を開始します")
            manager.enrich_all_companies(companies_list, fetch_full=True)
        
        detector = ExplosionStockDetector()
        
        results = []
        
        logger.info("🔍 爆発初動株を検出中...")
        for idx, company in enumerate(companies_list):
            code = company['code']
            name = company['name']
            metadata = company.get('metadata', {})
            
            if (idx + 1) % 10 == 0:
                logger.info(f"進捗: {idx + 1}/{len(companies_list)}")
            
            df = manager.get_company_data(code, days=100)
            result = detector.detect(code, name, df)
            
            if result:
                # metadata から財務情報を追加
                result['dividend_yield'] = metadata.get('dividend_yield', 0)
                result['eps_growth'] = metadata.get('eps_growth', 0)
                result['per'] = metadata.get('per', 0)
                result['pbr'] = metadata.get('pbr', 0)
                result['roe'] = metadata.get('roe', 0)
                result['doe'] = metadata.get('doe', 0)
                
                results.append(result)
                logger.info(f"🎯 {code} {name}: スコア {result['score']:.0f}")
            
            time.sleep(0.1)
        
        # 5. 全社のスコアを DataFrame に変換
        all_company_scores = []
        for company in companies_list:
            code = company['code']
            name = company['name']
            metadata = company.get('metadata', {})
            
            df = manager.get_company_data(code, days=100)
            result = detector.detect(code, name, df)
            
            if result:
                score = result['score']
            else:
                score = 0
            
            all_company_scores.append({
                'code': code,
                'name': name,
                'score': score,
                'price': result['price'] if result else 0,
                'volume': result['volume'] if result else 0,
                'dividend_yield': metadata.get('dividend_yield', 0),
                'eps_growth': metadata.get('eps_growth', 0),
                'per': metadata.get('per', 0),
                'pbr': metadata.get('pbr', 0),
                'roe': metadata.get('roe', 0),
                'doe': metadata.get('doe', 0)
            })
        
        # 6. スコアでソート
        df_all = pd.DataFrame(all_company_scores)
        df_all = df_all.sort_values('score', ascending=False).reset_index(drop=True)
        df_all['順位'] = range(1, len(df_all) + 1)
        
        # 段階別に分類
        df_monitoring = df_all[(df_all['score'] >= 40) & (df_all['score'] < 55)]  # 監視
        df_entry = df_all[(df_all['score'] >= 55) & (df_all['score'] < 70)]  # 押し目/初動狙い
        df_trend = df_all[(df_all['score'] >= 70) & (df_all['score'] < 85)]  # 順張りエントリー
        df_strong = df_all[df_all['score'] >= 85]  # 主力資金OK
        
        logger.info(f"✅ 段階別分類完了:")
        logger.info(f"  📊 全社: {len(df_all)} 社")
        logger.info(f"  👀 監視（40～55点）: {len(df_monitoring)} 社")
        logger.info(f"  🎯 押し目/初動狙い（55～70点）: {len(df_entry)} 社")
        logger.info(f"  🚀 順張りエントリー（70～85点）: {len(df_trend)} 社")
        logger.info(f"  💪 主力資金OK（85点以上）: {len(df_strong)} 社")
        
        # 7. ディレクトリを作成
        os.makedirs('results', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        os.makedirs('snapshots', exist_ok=True)
        
        # 8. Excel に出力（複数シート：段階別）
        output_file = 'results/phase1_results.xlsx'
        
        columns_all = ['順位', 'code', 'name', 'score', 'price', 'volume', 'dividend_yield', 'eps_growth', 'per', 'pbr', 'roe', 'doe']
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: 全社スコア
            df_output_all = df_all[columns_all].copy()
            df_output_all.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
            df_output_all.to_excel(writer, sheet_name='全社スコア', index=False)
            
            # Sheet 2: 監視（40～55点）
            df_output_monitoring = df_monitoring[columns_all].copy()
            df_output_monitoring.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
            df_output_monitoring.to_excel(writer, sheet_name='監視_40～55', index=False)
            
            # Sheet 3: 押し目/初動狙い（55～70点）
            df_output_entry = df_entry[columns_all].copy()
            df_output_entry.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
            df_output_entry.to_excel(writer, sheet_name='押し目_55～70', index=False)
            
            # Sheet 4: 順張りエントリー（70～85点）
            df_output_trend = df_trend[columns_all].copy()
            df_output_trend.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
            df_output_trend.to_excel(writer, sheet_name='順張り_70～85', index=False)
            
            # Sheet 5: 主力資金OK（85点以上）
            df_output_strong = df_strong[columns_all].copy()
            df_output_strong.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
            df_output_strong.to_excel(writer, sheet_name='主力_85+', index=False)
        
        logger.info(f"✅ Excel 出力完了: {output_file}")
        
        # 9. JSON スナップショット保存
        snapshot_file = f"snapshots/phase1_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        df_all.to_json(snapshot_file, orient='records', force_ascii=False)
        
        logger.info(f"✅ スナップショット保存: {snapshot_file}")
        
        # 10. Discord に送信
        webhook_url = os.getenv('DISCORD_WEBHOOK')
        send_to_discord(webhook_url, output_file, df_strong)
        
        # 11. ログ出力
        logger.info("=" * 70)
        logger.info("Phase 1 Screening 完了")
        logger.info("=" * 70)
        logger.info(f"企業データ抽出: ✅ 完了 ({len(companies_list)} 社)")
        logger.info(f"全社スコア計算: ✅ 完了 ({len(df_all)} 社)")
        logger.info(f"段階別分類: ✅ 完了")
        logger.info(f"  👀 監視（40～55点）: {len(df_monitoring)} 社")
        logger.info(f"  🎯 押し目/初動狙い（55～70点）: {len(df_entry)} 社")
        logger.info(f"  🚀 順張りエントリー（70～85点）: {len(df_trend)} 社")
        logger.info(f"  💪 主力資金OK（85点以上）: {len(df_strong)} 社")
        logger.info(f"結果保存: ✅ 完了 (段階別 5シート)")
        logger.info(f"Discord 送信: ✅ 完了")
        
        return True
    
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
