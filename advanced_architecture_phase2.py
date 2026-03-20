#!/usr/bin/env python3
"""
【Phase 2】Stock Analysis Platform v3.0
100社対象 爆発初動株スクリーニング + 財務分析
実行時間：毎日 23:00
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

def send_to_discord(webhook_url, excel_file, df_output):
    """Discord に爆発初動ランキング（テーブル）と Excel ファイルを送信"""
    try:
        if not webhook_url:
            logger.warning("⚠️ Discord 送信をスキップ")
            return
        
        # 1. テーブル形式でランキングを作成
        table_content = "```\n"
        table_content += "順位 | 企業コード | 企業名        | スコア | 株価\n"
        table_content += "-" * 60 + "\n"
        
        if len(df_output) > 0:
            for idx, row in df_output.head(20).iterrows():
                rank = int(row['順位'])
                code = row['企業コード']
                name = row['企業名'][:8].ljust(8)
                score = int(row['スコア'])
                price = int(row['株価'])
                table_content += f"{rank:2d}   | {code}    | {name} | {score:3d}  | ¥{price}\n"
        else:
            table_content += "爆発初動株が検出されませんでした\n"
        
        table_content += "```"
        
        # Discord に テーブル メッセージ送信
        data = {
            'content': f"🚀 **Phase 2 爆発初動株ランキング (Top 20)**\n\n{table_content}",
            'username': 'Stock Analysis Bot'
        }
        requests.post(webhook_url, json=data, timeout=30)
        logger.info("✅ Discord にランキングを送信")
        
        # 2. Excel ファイルを送信
        if os.path.exists(excel_file):
            with open(excel_file, 'rb') as f:
                files = {'file': f}
                data = {
                    'content': '📊 詳細は添付の Excel ファイルをご覧ください'
                }
                requests.post(webhook_url, files=files, data=data, timeout=30)
            logger.info("✅ Discord に Excel ファイルを送信")
    
    except Exception as e:
        logger.warning(f"⚠️ Discord 送信エラー: {e}")

def main():
    """メイン処理"""
    
    try:
        # 1. 設定ファイルを読み込み
        logger.info("📖 config_phase2.json を読み込み中...")
        with open('config_phase2.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info(f"✅ 設定読み込み完了: {config['system']['name']}")
        
        # 2. API キーを取得
        api_key = os.getenv('JQUANTS_API_KEY')
        if not api_key:
            logger.error("❌ JQUANTS_API_KEY が設定されていません")
            return False
        
        # 3. 企業リストを取得
        companies_list = config['companies']['data']
        logger.info(f"📊 企業数: {len(companies_list)}")
        
        # 4. データ取得と検出
        fetcher = JQuantsDataFetcher(api_key)
        detector = ExplosionStockDetector()
        
        results = []
        
        logger.info("🔍 爆発初動株を検出中...")
        for idx, company in enumerate(companies_list):
            code = company['code']
            name = company['name']
            metadata = company.get('metadata', {})
            
            if (idx + 1) % 10 == 0:
                logger.info(f"進捗: {idx + 1}/{len(companies_list)}")
            
            df = fetcher.fetch_daily_bars(code)
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
        
        # 5. 爆発初動株を抽出
        explosion_stocks = [r for r in results if r['score'] >= 50]
        
        # 6. 全社のスコアを DataFrame に変換
        all_company_scores = []
        for company in companies_list:
            code = company['code']
            name = company['name']
            metadata = company.get('metadata', {})
            
            # その企業の検出結果を探す
            result = next((r for r in results if r['code'] == code), None)
            
            if result:
                all_company_scores.append({
                    'code': code,
                    'name': name,
                    'score': result['score'],
                    'price': result['price'],
                    'volume': result['volume'],
                    'dividend_yield': metadata.get('dividend_yield', 0),
                    'eps_growth': metadata.get('eps_growth', 0),
                    'per': metadata.get('per', 0),
                    'pbr': metadata.get('pbr', 0),
                    'roe': metadata.get('roe', 0),
                    'doe': metadata.get('doe', 0)
                })
            else:
                # スコア計算されなかった企業は 0 スコア
                all_company_scores.append({
                    'code': code,
                    'name': name,
                    'score': 0,
                    'price': 0,
                    'volume': 0,
                    'dividend_yield': metadata.get('dividend_yield', 0),
                    'eps_growth': metadata.get('eps_growth', 0),
                    'per': metadata.get('per', 0),
                    'pbr': metadata.get('pbr', 0),
                    'roe': metadata.get('roe', 0),
                    'doe': metadata.get('doe', 0)
                })
        
        # スコアでソート
        df_all = pd.DataFrame(all_company_scores)
        df_all = df_all.sort_values('score', ascending=False).reset_index(drop=True)
        df_all['順位'] = range(1, len(df_all) + 1)
        
        # 爆発初動株のみ
        df_results = pd.DataFrame(explosion_stocks)
        if len(df_results) > 0:
            df_results = df_results.sort_values('score', ascending=False).reset_index(drop=True)
            df_results['順位'] = range(1, len(df_results) + 1)
        else:
            df_results = pd.DataFrame(columns=['順位', 'code', 'name', 'score', 'dividend_yield', 'eps_growth', 'per', 'pbr', 'roe', 'doe'])
        
        logger.info(f"✅ 爆発初動株: {len(df_results)} 件")
        logger.info(f"📊 全社スコア: {len(df_all)} 件")
        
        # 6. ディレクトリを作成
        os.makedirs('results', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        os.makedirs('snapshots', exist_ok=True)
        
        # 7. Excel に出力（2シート）
        output_file = 'results/phase2_results.xlsx'
        
        # Sheet 1: 全社スコア
        columns_all = ['順位', 'code', 'name', 'score', 'price', 'volume', 'dividend_yield', 'eps_growth', 'per', 'pbr', 'roe', 'doe']
        df_output_all = df_all[columns_all]
        df_output_all.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
        
        # Sheet 2: 爆発初動株のみ
        columns_explosion = ['順位', 'code', 'name', 'score', 'price', 'volume', 'dividend_yield', 'eps_growth', 'per', 'pbr', 'roe', 'doe']
        df_output_explosion = df_results[columns_explosion]
        df_output_explosion.columns = ['順位', '企業コード', '企業名', 'スコア', '株価', '出来高', '配当利回り', 'EPS成長率', 'PER', 'PBR', 'ROE', 'DOE']
        
        # Excel ファイルに複数シートで保存
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_output_all.to_excel(writer, sheet_name='全社スコア', index=False)
            df_output_explosion.to_excel(writer, sheet_name='爆発初動株', index=False)
        
        logger.info(f"✅ Excel 出力完了: {output_file}")
        
        # 8. JSON スナップショット保存
        snapshot_file = f"snapshots/phase2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        df_all.to_json(snapshot_file, orient='records', force_ascii=False)
        
        logger.info(f"✅ スナップショット保存: {snapshot_file}")
        
        # 9. Discord に送信
        webhook_url = os.getenv('DISCORD_WEBHOOK')
        send_to_discord(webhook_url, output_file, df_output_explosion)
        
        # 10. ログ出力
        logger.info("=" * 70)
        logger.info("Phase 2 Screening 完了")
        logger.info("=" * 70)
        logger.info(f"企業データ抽出: ✅ 完了 ({len(companies_list)} 社)")
        logger.info(f"全社スコア計算: ✅ 完了 ({len(df_all)} 社)")
        logger.info(f"爆発初動検出: ✅ 完了 ({len(df_results)} 件)")
        logger.info(f"結果保存: ✅ 完了 (全社スコア + 爆発初動株)")
        logger.info(f"Discord 送信: ✅ 完了")
        
        print("\n【爆発初動株ランキング (Top 20)】")
        print(df_output_explosion.head(20).to_string(index=False))
        
        return True
    
    except Exception as e:
        logger.error(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)
