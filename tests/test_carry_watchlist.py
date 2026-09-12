from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from grid_optimizer import carry_watchlist as monitor


class CarryWatchlistTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'watchlist.json'
        self.state = Path(self.temp.name) / 'state.json'
        self.now = datetime(2026, 9, 12, 15, 50, tzinfo=timezone.utc)
        self.contracts = [{'symbol': 'BTCUSDT', 'market': 'usdm', 'perpetual': True}, {'symbol': 'BTCUSD_PERP', 'market': 'coinm', 'perpetual': True}, {'symbol': 'BTCUSD_261225', 'market': 'coinm', 'perpetual': False}]
        p = patch.object(monitor, 'fetch_contracts', return_value=self.contracts)
        p.start(); self.addCleanup(p.stop)
        p = patch.object(monitor, 'bark_configured', return_value=True)
        p.start(); self.addCleanup(p.stop)
        p = patch.object(monitor, '_send_bark', return_value={'sent': True, 'error': None})
        self.send = p.start(); self.addCleanup(p.stop)

    def rule(self, **kwargs):
        raw = {'symbol': 'BTCUSDT', 'market': 'usdm', 'condition': 'funding_below', 'threshold': 0, 'channel': 'bark', 'frequency': 'daily', **kwargs}
        return monitor.save_rule(raw, self.path)

    def check(self, funding=-0.001, change=51, minutes=0, stale=False, missing=False):
        now = self.now + timedelta(minutes=minutes)
        stamp = (now - timedelta(hours=1) if stale else now).timestamp() * 1000
        snapshot = {'premium': {r['symbol']: {'lastFundingRate': funding, 'time': stamp} for r in self.contracts}, 'ticker': {r['symbol']: {'priceChangePercent': change, 'lastPrice': 100, 'closeTime': stamp} for r in self.contracts}, 'errors': []}
        if missing: snapshot['premium'] = {}
        with patch.object(monitor, '_market_snapshot', return_value=snapshot):
            return monitor.check_watchlist(watchlist_path=self.path, state_path=self.state, now=now)

    def test_daily_uses_beijing_date_and_caps_failures(self):
        self.rule()
        self.send.return_value = {'sent': False, 'error': '失败'}
        self.check(minutes=0)
        self.check(minutes=5)
        self.assertEqual(self.send.call_count, 1)
        self.check(minutes=15)  # New Beijing day, same UTC day.
        self.assertEqual(self.send.call_count, 2)

    def test_repeat_interval_while_condition_remains_true(self):
        self.rule(frequency='repeat', interval_minutes=15)
        for minutes in [0, 5, 10, 15, 20, 30]: self.check(minutes=minutes)
        self.assertEqual(self.send.call_count, 3)

    def test_edge_rearms_only_after_condition_clears(self):
        self.rule(frequency='edge')
        for minutes, rate in enumerate([-.001, -.002, .001, -.001]): self.check(funding=rate, minutes=minutes*5)
        self.assertEqual(self.send.call_count, 2)

    def test_negative_point_five_percent_is_not_negative_point_five_fraction(self):
        self.rule(threshold=-0.5)
        self.check(funding=-0.004)
        self.check(funding=-0.005, minutes=5)  # Strictly below, equality is excluded.
        self.send.assert_not_called()
        self.check(funding=-0.006, minutes=10)
        self.send.assert_called_once()

    def test_cross_needs_positive_baseline_and_handles_zero(self):
        self.rule(condition='funding_cross_negative', frequency='edge')
        for i, value in enumerate([-.001, .001, 0, -.001, -.002]): self.check(funding=value, minutes=i*5)
        self.assertEqual(self.send.call_count, 1)

    def test_consecutive_samples_reset_on_missing_and_stale_data(self):
        self.rule(confirmations=2)
        self.check()
        self.check(minutes=5, missing=True)
        self.check(minutes=10)
        self.check(minutes=15, stale=True)
        self.check(minutes=20)
        self.send.assert_not_called()
        self.check(minutes=25)
        self.send.assert_called_once()

    def test_price_rule_works_for_delivery_and_web_channel_does_not_send(self):
        self.rule(symbol='BTCUSD_261225', market='coinm', condition='change_above', threshold=50, channel='web')
        self.check(change=49)
        self.check(change=51, minutes=5)
        self.send.assert_not_called()
        events = json.loads(self.state.read_text())['events']
        self.assertEqual(len(events), 1)
        self.assertIn('51%', events[0]['body'])
        with self.assertRaises(ValueError): self.rule(symbol='BTCUSD_261225', market='coinm')

    def test_pause_delete_and_edit_reset_baseline(self):
        rule = self.rule(condition='funding_cross_negative')
        self.check(funding=.001)
        monitor.change_rule(rule['id'], 'toggle', self.path)
        self.check(funding=-.001, minutes=5)
        monitor.change_rule(rule['id'], 'toggle', self.path)
        self.check(funding=-.001, minutes=10)
        self.send.assert_not_called()
        monitor.change_rule(rule['id'], 'delete', self.path)
        self.assertEqual(monitor.load_rules(self.path), [])

    def test_existing_observations_migrate_without_losing_selected_symbols(self):
        monitor._save_json(self.path, {'symbols': ['BTCUSDT']})
        monitor._save_json(self.state, {'symbols': {'BTCUSDT': {'last_rate': .001}}})
        self.check(funding=-.001)
        self.send.assert_called_once()
        self.rule(condition='change_above', threshold=50)
        self.assertEqual(len(monitor.load_rules(self.path)), 2)
        self.assertEqual(monitor.load_watchlist(self.path), ['BTCUSDT'])

    def test_invalid_threshold_and_settings_rejected(self):
        for invalid in [{'threshold': float('nan')}, {'interval_minutes': 0}, {'confirmations': 13}, {'channel': 'email'}, {'market': 'bad', 'symbol': 'MISSING'}, {'condition': 'price_below', 'threshold': -1}]:
            with self.assertRaises(ValueError): self.rule(**invalid)

    def test_bark_url_is_not_echoed_and_file_is_private(self):
        with patch.object(monitor, 'DEFAULT_BARK_PATH', Path(self.temp.name) / 'bark.json'):
            monitor.save_bark_endpoint('https://api.day.app/abcdefgh12345678')
            self.assertEqual(monitor.DEFAULT_BARK_PATH.stat().st_mode & 0o777, 0o600)
            self.assertNotIn('abcdefgh12345678', json.dumps(monitor.watchlist_payload(self.path, self.state)))
            with self.assertRaises(ValueError): monitor.save_bark_endpoint('http://127.0.0.1/key')
