# -*- coding: utf-8 -*-
import asyncio
import logging
import platform
import sys
import sqlite3
import time
import statistics
import os
from collections import deque, defaultdict
from contextlib import closing
from dataclasses import dataclass, field
from decimal import Decimal, getcontext, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable

# --- Проверка зависимостей ---
try:
    import ccxt.async_support as ccxt_async
    from ccxt.base.errors import (
        ExchangeError, PermissionDenied, InsufficientFunds, NetworkError,
        OrderNotFound, OperationRejected, InvalidOrder, ExchangeNotAvailable,
        BadSymbol, RateLimitExceeded, AuthenticationError
    )
except ImportError:
    print("ОШИБКА: Библиотека CCXT не найдена. Пожалуйста, установите ее: pip install ccxt")
    sys.exit(1)


# --- УПРАВЛЕНИЕ API-КЛЮЧАМИ ---
API_KEY = "7QDQrlViBW5rugiRjJC9UqGbwwQGWmMlxHGZOhWciWLVZvWiDuRZLP6kI1EOAsnr"
API_SECRET = "a1Pe212ErRkYlVB8ATlsvibNzOTbBdNpnfAebmjeh8yh43DIdv5VLh1jz9TO9cly"


class Config:
    DRY_RUN = False
    EXCHANGE_ID = 'binance'
    QUOTE_CURRENCY = 'USDT'
    DATABASE_FILE = Path("binance_trades_v21_final.db")
    
    MARGIN_LEVERAGE = Decimal("2.0")
    CAPITAL_PER_TRADE_USD = Decimal("6.0")
    TARGET_TRADE_AMOUNT_USD = CAPITAL_PER_TRADE_USD * MARGIN_LEVERAGE
    
    MAX_OPEN_POSITIONS = 1
    
    STDEV_THRESHOLD_OPEN_LONG = Decimal("3.5")
    STDEV_THRESHOLD_OPEN_SHORT = Decimal("-3.5")
    Z_SCORE_STOP_LOSS_FACTOR = Decimal("-0.8")
    TRAILING_TAKE_PROFIT_ACTIVATION = Decimal("0.5")
    TRAILING_TAKE_PROFIT_DISTANCE = Decimal("0.3")

    SPREAD_HISTORY_SIZE = 100
    MIN_DAILY_VOLUME_USD = Decimal("500000.0")
    MAX_POSITION_HOLD_TIME_S = 12 * 3600
    ORDER_EXECUTION_TIMEOUT_S = 60
    MIN_FILL_RATIO = 0.95
    COMMISSION_RATE = Decimal("0.001")
    TRADE_CYCLE_SLEEP_S = 5
    API_RETRY_COUNT = 3
    API_RETRY_DELAY_S = 3
    POST_OPERATION_SLEEP_S = 3.5
    COLLATERAL_BUFFER_FACTOR = Decimal("1.02")

# --- Настройка среды и логирования ---
if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
getcontext().prec = 28
logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[logging.FileHandler('arbitrage_bot_final.log', encoding='utf-8', mode='a'), logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MarginTrader")

class PartialFillException(Exception): pass
class CleanupException(Exception): pass

@dataclass
class Opportunity: symbol: str; spot_price: Decimal; z_score: Decimal; direction: str
@dataclass
class OpenPosition:
    id: int; symbol: str; direction: str; amount_base: Decimal; open_price: Decimal
    z_score_entry: Decimal; created_at: int = field(default_factory=lambda: int(time.time()))

class DatabaseManager:
    def __init__(self, db_file: Path):
        self.db_file = db_file; self.logger = logging.getLogger(self.__class__.__name__)
    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_file, timeout=10); conn.row_factory = sqlite3.Row; return conn
    def setup_database(self) -> None:
        query = """CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY, symbol TEXT NOT NULL UNIQUE, direction TEXT NOT NULL,
            amount_base TEXT NOT NULL, open_price TEXT NOT NULL,
            z_score_entry TEXT NOT NULL, created_at INTEGER NOT NULL);"""
        try:
            with closing(self._get_connection()) as conn: conn.execute(query); conn.commit()
            self.logger.info("База данных успешно инициализирована.")
        except sqlite3.Error as e: self.logger.critical(f"Критическая ошибка при инициализации БД: {e}"); raise
    def add_position(self, pos: OpenPosition) -> Optional[int]:
        query = "INSERT INTO positions (symbol, direction, amount_base, open_price, z_score_entry, created_at) VALUES (?, ?, ?, ?, ?, ?)"
        try:
            with closing(self._get_connection()) as conn:
                cursor = conn.cursor()
                cursor.execute(query, (pos.symbol, pos.direction, str(pos.amount_base), str(pos.open_price), str(pos.z_score_entry), pos.created_at))
                conn.commit(); return cursor.lastrowid
        except sqlite3.IntegrityError: self.logger.error(f"Позиция {pos.symbol} уже существует в БД."); return None
        except sqlite3.Error as e: self.logger.error(f"Ошибка записи в БД: {e}"); return None
    def get_open_positions(self) -> List[OpenPosition]:
        try:
            with closing(self._get_connection()) as conn:
                rows = conn.execute("SELECT * FROM positions").fetchall()
                return [OpenPosition(id=row['id'], symbol=row['symbol'], direction=row['direction'], amount_base=Decimal(row['amount_base']), open_price=Decimal(row['open_price']), z_score_entry=Decimal(row['z_score_entry']), created_at=row['created_at']) for row in rows]
        except sqlite3.Error as e: self.logger.error(f"Ошибка чтения из БД: {e}"); return []
    def delete_position(self, pos_id: int) -> None:
        try:
            with closing(self._get_connection()) as conn: conn.execute("DELETE FROM positions WHERE id = ?", (pos_id,)); conn.commit()
        except sqlite3.Error as e: self.logger.error(f"Ошибка удаления из БД: {e}")

class SpreadHistory:
    def __init__(self, history_size: int):
        self.spreads: Dict[str, deque] = defaultdict(lambda: deque(maxlen=history_size))
    def add_spread(self, symbol: str, ticker: dict):
        try:
            ask = Decimal(str(ticker.get('ask', 0))); bid = Decimal(str(ticker.get('bid', 0)))
            if ask > 0 and bid > 0: self.spreads[symbol].append(ask - bid)
        except (KeyError, InvalidOperation, TypeError, ValueError): pass
    def get_stats(self, symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        history = self.spreads[symbol]
        if len(history) < 20: return None, None
        try:
            history_float = [float(s) for s in history]
            return Decimal(str(statistics.mean(history_float))), Decimal(str(statistics.stdev(history_float)))
        except statistics.StatisticsError: return None, None

class TradeExecutor:
    def __init__(self, exchange: ccxt_async.Exchange, config: Config, db_manager: DatabaseManager):
        self.exchange = exchange; self.config = config; self.db = db_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.exchange.markets:
            self.logger.critical("КРИТИЧЕСКАЯ ОШИБКА: TradeExecutor был создан без предварительно загруженных рынков.")
            raise ValueError("Объект exchange должен иметь загруженные рынки перед передачей в TradeExecutor.")

    async def _retry_with_backoff(self, coro_func: Callable, *args, **kwargs) -> Any:
        delay = self.config.API_RETRY_DELAY_S
        for attempt in range(self.config.API_RETRY_COUNT):
            try: return await coro_func(*args, **kwargs)
            except InsufficientFunds as e: self.logger.error(f"Ошибка недостатка средств: {e}. Операция не будет повторена."); raise
            except (NetworkError, ExchangeNotAvailable, RateLimitExceeded) as e:
                if attempt == self.config.API_RETRY_COUNT - 1: raise
                self.logger.warning(f"Сетевая ошибка или превышение лимитов (попытка {attempt + 1}/{self.config.API_RETRY_COUNT}): {e}. Повтор через {delay} с."); await asyncio.sleep(delay); delay *= 2
            except AuthenticationError as e: self.logger.critical(f"Критическая ошибка аутентификации: {e}."); raise
            except ExchangeError as e:
                if "code\":-3050" in str(e):
                    self.logger.error(f"Достигнут лимит изолированных пар. Операция не будет повторена: {e}")
                    raise
                if isinstance(e, (PermissionDenied, OperationRejected, InvalidOrder, BadSymbol)): raise
                if attempt == self.config.API_RETRY_COUNT - 1: raise
                self.logger.warning(f"Ошибка биржи (попытка {attempt + 1}/{self.config.API_RETRY_COUNT}): {e}. Повтор через {delay} с."); await asyncio.sleep(delay); delay *= 2
        raise ExchangeError("Не удалось выполнить API вызов после нескольких попыток")

    async def _get_spot_balance(self, asset: str) -> Decimal:
        try:
            balances = await self._retry_with_backoff(self.exchange.fetch_balance)
            return Decimal(str(balances.get('free', {}).get(asset, '0')))
        except Exception as e: self.logger.error(f"Не удалось получить спотовый баланс для {asset}: {e}"); return Decimal('0')

    async def _get_cross_margin_balance(self, asset: str) -> Decimal:
        try:
            cross_balance_data = await self._retry_with_backoff(self.exchange.sapi_get_margin_account)
            for asset_info in cross_balance_data.get('userAssets', []):
                if asset_info.get('asset') == asset: return Decimal(asset_info.get('free', '0'))
            return Decimal('0')
        except Exception as e: self.logger.error(f"Не удалось получить кросс-маржинальный баланс для {asset}: {e}"); return Decimal('0')

    async def _get_isolated_asset_balance(self, symbol: str, asset_code: str) -> Decimal:
        api_symbol = symbol.replace('/', '')
        try:
            balance_data = await self._retry_with_backoff(self.exchange.sapi_get_margin_isolated_account, {'symbols': api_symbol})
            if 'assets' in balance_data and balance_data['assets']:
                pair_assets = balance_data['assets'][0]
                asset_details = pair_assets['baseAsset'] if pair_assets['baseAsset']['asset'] == asset_code else pair_assets['quoteAsset']
                if asset_details['asset'] == asset_code: return Decimal(asset_details.get('free', '0'))
            return Decimal('0')
        except Exception as e: self.logger.error(f"Не удалось получить баланс {asset_code} для {symbol}: {e}", exc_info=True); return Decimal('0')

    async def _transfer_from_cross_to_spot(self, asset: str, amount: Decimal):
        self.logger.info(f"Перевод {amount:.8f} {asset} с КРОСС-МАРЖИ на СПОТ...")
        params = {'type': 'MARGIN_MAIN', 'asset': asset, 'amount': float(amount)}
        await self._retry_with_backoff(self.exchange.sapi_post_asset_transfer, params)
        self.logger.info("Перевод с кросс-маржи на спот успешно выполнен."); await asyncio.sleep(self.config.POST_OPERATION_SLEEP_S)

    async def _transfer_collateral(self, symbol: str, asset: str, amount: Decimal, direction: int):
        api_symbol = symbol.replace('/', '')
        from_account, to_account, type_str = (None, None, "")

        if direction == 1:
            from_account, to_account, type_str = ('spot', 'isolated', "спот -> изоляция")
        elif direction == 2:
            from_account, to_account, type_str = ('isolated', 'spot', "изоляция -> спот")
        else:
            raise ValueError(f"Неподдерживаемое направление перевода: {direction}")

        self.logger.info(f"Перевод {float(amount)} {asset} ({type_str}) для {api_symbol}...")
        if self.config.DRY_RUN:
            self.logger.info(f"[DRY RUN] Перевод выполнен."); return
        try:
            await self._retry_with_backoff(self.exchange.transfer, asset, float(amount), from_account, to_account, params={'symbol': api_symbol})
            self.logger.info("Обеспечение успешно переведено."); await asyncio.sleep(self.config.POST_OPERATION_SLEEP_S)
        except Exception as e:
            self.logger.error(f"Критическая ошибка при переводе обеспечения для {symbol}: {e}", exc_info=True)
            raise

    async def _prepare_collateral(self, symbol: str) -> bool:
        required_collateral = (self.config.TARGET_TRADE_AMOUNT_USD / self.config.MARGIN_LEVERAGE) * self.config.COLLATERAL_BUFFER_FACTOR
        self.logger.info(f"Для сделки на ${self.config.TARGET_TRADE_AMOUNT_USD} с плечом x{self.config.MARGIN_LEVERAGE} требуется обеспечение: ~${required_collateral:.2f}")

        quote_currency = self.config.QUOTE_CURRENCY
        spot_balance = await self._get_spot_balance(quote_currency)
        
        if spot_balance >= required_collateral:
            self.logger.info(f"На спотовом счете достаточно средств для обеспечения ({spot_balance:.2f} {quote_currency}).")
            return True

        self.logger.warning(f"Недостаточно средств на споте ({spot_balance:.2f} {quote_currency}). Проверяю кросс-маржу...")
        cross_balance = await self._get_cross_margin_balance(quote_currency)
        total_available = spot_balance + cross_balance

        if total_available >= required_collateral:
            amount_to_transfer = required_collateral - spot_balance
            self.logger.info(f"На кросс-марже достаточно средств. Требуется перевести на спот: {amount_to_transfer:.2f} {quote_currency}.")
            try:
                if amount_to_transfer > 0:
                    await self._transfer_from_cross_to_spot(quote_currency, amount_to_transfer)
                return True
            except Exception as e:
                self.logger.error(f"Не удалось перевести средства с кросс-маржи на спот: {e}", exc_info=True)
                return False
        else:
            self.logger.error(f"Недостаточно средств для открытия позиции {symbol}. Требуется обеспечения: {required_collateral:.2f} {quote_currency}. Всего доступно (спот + кросс): {total_available:.2f} {quote_currency}.")
            return False

    async def _wait_for_order_fill(self, order_id: str, symbol: str, expected_amount: float) -> dict:
        api_symbol = symbol.replace('/', ''); params = {'symbol': api_symbol, 'orderId': order_id, 'isIsolated': 'TRUE'}; start_time = time.time()
        while time.time() - start_time < self.config.ORDER_EXECUTION_TIMEOUT_S:
            try:
                fetched_order = await self._retry_with_backoff(self.exchange.sapi_get_margin_order, params)
                if fetched_order.get('status', '').upper() == 'FILLED':
                    filled_amount = float(fetched_order.get('executedQty', 0.0))
                    avg_price = float(fetched_order.get('cummulativeQuoteQty', 0.0)) / filled_amount if filled_amount > 0 else 0
                    
                    self.logger.info(f"Ордер {order_id} ({symbol}) успешно исполнен.")
                    self.logger.info(f"ДЕТАЛИ ОРДЕРА: Исполнено={filled_amount}, Цена={avg_price:.6f}, Общая стоимость={float(fetched_order.get('cummulativeQuoteQty')):.4f} USDT")
                    
                    if filled_amount / expected_amount < self.config.MIN_FILL_RATIO: raise PartialFillException(f"Ордер {order_id} исполнен лишь на {filled_amount}/{expected_amount}.")
                    
                    return {'id': str(fetched_order['orderId']), 'symbol': symbol, 'average': avg_price, 'filled': filled_amount, 'status': 'closed'}
                await asyncio.sleep(2)
            except OrderNotFound: await asyncio.sleep(3)
            except ExchangeError as e: self.logger.error(f"Ошибка при ожидании ордера {order_id}: {e}"); raise
        try:
            await self._retry_with_backoff(self.exchange.sapi_delete_margin_order, {'symbol': api_symbol, 'orderId': order_id, 'isIsolated': 'TRUE'})
            self.logger.warning(f"Ордер {order_id} не исполнился и был отменен.")
        except OrderNotFound: pass
        except ExchangeError as e: self.logger.error(f"Не удалось отменить зависший ордер {order_id}: {e}")
        raise OperationRejected(f"Ордер {order_id} не исполнился за {self.config.ORDER_EXECUTION_TIMEOUT_S} секунд.")

    async def _execute_margin_order(self, symbol: str, side: str, amount: float, side_effect_type: str) -> dict:
        if amount <= 0: raise ValueError("Количество для ордера должно быть положительным.")
        final_amount_str = self.exchange.amount_to_precision(symbol, amount)
        final_amount_float = float(final_amount_str)
        if final_amount_float <= 0: raise InvalidOrder(f"Ошибка: после форматирования объем для {symbol} равен нулю. Начальный: {amount}.")
        self.logger.info(f"Размещение ордера: {side.upper()} {final_amount_float} {symbol} (ISOLATED MARGIN, sideEffectType={side_effect_type})")
        if self.config.DRY_RUN:
            ticker = await self.exchange.fetch_ticker(symbol)
            est_price = ticker['ask'] if side.upper() == 'BUY' else ticker['bid']
            return {'id': f'dry_{int(time.time())}', 'symbol': symbol, 'side': side, 'average': est_price, 'filled': final_amount_float, 'status': 'closed'}
        params = {'symbol': symbol.replace('/', ''), 'side': side.upper(), 'type': 'MARKET', 'quantity': final_amount_float, 'isIsolated': 'TRUE', 'sideEffectType': side_effect_type, 'newOrderRespType': 'RESULT'}
        order_response = await self._retry_with_backoff(self.exchange.sapi_post_margin_order, params)
        return await self._wait_for_order_fill(str(order_response['orderId']), symbol, final_amount_float)

    async def open_long_position(self, opportunity: Opportunity):
        collateral_transferred = False
        try:
            if not await self._prepare_collateral(opportunity.symbol): return

            collateral_to_transfer = (self.config.TARGET_TRADE_AMOUNT_USD / self.config.MARGIN_LEVERAGE) * self.config.COLLATERAL_BUFFER_FACTOR
            amount_str = self.exchange.price_to_precision(opportunity.symbol, float(collateral_to_transfer))
            
            await self._transfer_collateral(opportunity.symbol, self.config.QUOTE_CURRENCY, Decimal(amount_str), 1)
            collateral_transferred = True
            
            amount_to_trade = float(self.config.TARGET_TRADE_AMOUNT_USD / opportunity.spot_price)
            buy_order = await self._execute_margin_order(opportunity.symbol, 'buy', amount_to_trade, 'MARGIN_BUY')
            position = OpenPosition(id=0, symbol=opportunity.symbol, direction='LONG', amount_base=Decimal(str(buy_order['filled'])), open_price=Decimal(str(buy_order['average'])), z_score_entry=opportunity.z_score)
            db_id = self.db.add_position(position)
            if db_id: self.logger.info(f"✅ Позиция LONG {opportunity.symbol} успешно открыта (ID: {db_id})")
            else: raise Exception("Не удалось сохранить позицию в БД, требуется откат.")
        except Exception as e:
            self.logger.error(f"Ошибка при открытии LONG {opportunity.symbol}: {e}. Запускаем откат.", exc_info=True)
            if collateral_transferred: await self._rollback_position(opportunity.symbol)
            raise CleanupException(f"Позиция LONG для {opportunity.symbol} не была открыта из-за ошибки.")

    async def open_short_position(self, opportunity: Opportunity):
        collateral_transferred = False
        try:
            if not await self._prepare_collateral(opportunity.symbol): return

            collateral_to_transfer = (self.config.TARGET_TRADE_AMOUNT_USD / self.config.MARGIN_LEVERAGE) * self.config.COLLATERAL_BUFFER_FACTOR
            amount_str = self.exchange.price_to_precision(opportunity.symbol, float(collateral_to_transfer))
            
            await self._transfer_collateral(opportunity.symbol, self.config.QUOTE_CURRENCY, Decimal(amount_str), 1)
            collateral_transferred = True
            
            amount_to_trade = float(self.config.TARGET_TRADE_AMOUNT_USD / opportunity.spot_price)
            sell_order = await self._execute_margin_order(opportunity.symbol, 'sell', amount_to_trade, 'AUTO_BORROW_REPAY')
            position = OpenPosition(id=0, symbol=opportunity.symbol, direction='SHORT', amount_base=Decimal(str(sell_order['filled'])), open_price=Decimal(str(sell_order['average'])), z_score_entry=opportunity.z_score)
            db_id = self.db.add_position(position)
            if db_id: self.logger.info(f"✅ Позиция SHORT {opportunity.symbol} успешно открыта (ID: {db_id})")
            else: raise Exception("Не удалось сохранить позицию в БД, требуется откат.")
        except Exception as e:
            self.logger.error(f"Ошибка при открытии SHORT {opportunity.symbol}: {e}. Запускаем откат.", exc_info=True)
            if collateral_transferred: await self._rollback_position(opportunity.symbol)
            raise CleanupException(f"Позиция SHORT для {opportunity.symbol} не была открыта из-за ошибки.")

    async def close_position(self, position: OpenPosition, reason: str):
        symbol = position.symbol
        self.logger.info(f"Начинаем закрытие {position.direction} позиции {symbol} (ID: {position.id}) по причине: {reason}.")
        try:
            market_data = self.exchange.markets[symbol]
            base_asset = market_data['base']
            amount_to_close_raw = await self._get_isolated_asset_balance(symbol, base_asset)
            if amount_to_close_raw <= 0:
                self.logger.warning(f"Нет {base_asset} для закрытия позиции {symbol}. Пропускаем ордер, переходим к возврату обеспечения.")
            else:
                amount_to_close_str = self.exchange.amount_to_precision(symbol, float(amount_to_close_raw))
                amount_to_close = float(amount_to_close_str)
                
                min_order_amount = float(market_data.get('limits', {}).get('amount', {}).get('min', 0))
                if amount_to_close < min_order_amount:
                    self.logger.warning(f"Количество для продажи {amount_to_close} {base_asset} меньше минимального {min_order_amount}. Ордер не будет выставлен.")
                else:
                    side_to_close = 'sell' if position.direction == 'LONG' else 'buy'
                    close_order_result = await self._execute_margin_order(symbol, side_to_close, amount_to_close, 'AUTO_REPAY')
                    self.logger.info(f"Ордер на закрытие позиции по {symbol} успешно отправлен.")
                    
                    try:
                        open_price = position.open_price
                        close_price = Decimal(str(close_order_result['average']))
                        
                        amount_opened = position.amount_base
                        amount_closed = Decimal(str(close_order_result['filled']))
                        pnl_calc_amount = min(amount_opened, amount_closed)

                        open_cost = open_price * pnl_calc_amount
                        close_value = close_price * pnl_calc_amount

                        if position.direction == 'LONG':
                            gross_pnl = close_value - open_cost
                        else:
                            gross_pnl = open_cost - close_value

                        total_fee = (open_cost + close_value) * self.config.COMMISSION_RATE
                        net_pnl = gross_pnl - total_fee
                        self.logger.info(f"💰 Чистая прибыль/убыток по сделке {position.symbol} (ID: {position.id}): {net_pnl:.4f} {self.config.QUOTE_CURRENCY}")
                    except Exception as pnl_e:
                        self.logger.error(f"Не удалось рассчитать PnL для позиции {position.id}: {pnl_e}")

            await self._return_all_collateral_to_spot(symbol)
            if position.id > 0: self.db.delete_position(position.id)
            
        except Exception as e:
            self.logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при закрытии {symbol}. Требуется ручная проверка! Ошибка: {e}", exc_info=True)

    async def _return_all_collateral_to_spot(self, symbol: str):
        self.logger.info(f"Начинаем процедуру очистки для {symbol}...")
        if self.config.DRY_RUN:
            await self._deactivate_isolated_pair(symbol)
            return
            
        api_symbol = symbol.replace('/', '')
        try:
            balance_data = await self._retry_with_backoff(self.exchange.sapi_get_margin_isolated_account, {'symbols': api_symbol})
            if not ('assets' in balance_data and balance_data['assets']):
                self.logger.info(f"Нет данных по изолированному счету {api_symbol}, возможно он уже пуст.")
            else:
                isolated_assets = balance_data['assets'][0]
                for asset_info in [isolated_assets['baseAsset'], isolated_assets['quoteAsset']]:
                    free_amount = Decimal(asset_info.get('free', '0'))
                    asset_code = asset_info['asset']
                    if free_amount > 0:
                        try:
                            amount_str = self.exchange.currency_to_precision(asset_code, float(free_amount))
                            amount_to_transfer = Decimal(amount_str)
                            
                            if amount_to_transfer > 0:
                                await self._transfer_collateral(symbol, asset_code, amount_to_transfer, 2)
                                await asyncio.sleep(1.5)
                            else:
                                self.logger.warning(f"Остаток {free_amount} {asset_code} ('пыль') слишком мал для перевода. Пропускаем.")
                        except InvalidOrder as e:
                            self.logger.warning(f"Не удалось подготовить к переводу остаток {free_amount} {asset_code} ('пыль'): {e}")
                        except Exception as e:
                            self.logger.error(f"Не удалось вернуть {asset_code} с {symbol}: {e}", exc_info=True)
            
        except Exception as e:
            self.logger.error(f"Не удалось получить баланс для возврата обеспечения с {symbol}: {e}", exc_info=True)
        finally:
            await self._deactivate_isolated_pair(symbol)

    async def _deactivate_isolated_pair(self, symbol: str):
        api_symbol = symbol.replace('/', '')
        self.logger.info(f"Попытка деактивации изолированной пары {api_symbol}...")
        if self.config.DRY_RUN:
            self.logger.info(f"[DRY RUN] Изолированная пара {api_symbol} деактивирована.")
            return

        try:
            await self._retry_with_backoff(self.exchange.sapi_delete_margin_isolated_account, {'symbol': api_symbol})
            self.logger.info(f"✅ Изолированная пара {api_symbol} успешно деактивирована.")
        except ExchangeError as e:
            if 'does not exist' in str(e):
                 self.logger.info(f"Пара {api_symbol} уже неактивна.")
            elif 'have debt' in str(e):
                 self.logger.warning(f"Не удалось деактивировать {api_symbol}: не погашен долг.")
            elif 'have assets' in str(e):
                 self.logger.warning(f"Не удалось деактивировать {api_symbol}: на счете остались активы (пыль).")
            else:
                 self.logger.warning(f"Не удалось деактивировать пару {api_symbol}. Ошибка биржи: {e}")
        except Exception as e:
            self.logger.error(f"Непредвиденная ошибка при деактивации пары {api_symbol}: {e}", exc_info=True)

    async def _rollback_position(self, symbol: str):
        self.logger.warning(f"ЗАПУСК ОТКАТА для {symbol}: ликвидация остатков и возврат обеспечения.")
        if self.config.DRY_RUN: return
        try:
            fake_position = OpenPosition(id=-1, symbol=symbol, direction='UNKNOWN', amount_base=Decimal(0), open_price=Decimal(0), z_score_entry=Decimal(0))
            await self.close_position(fake_position, "Откат после неудачного открытия")
            self.logger.info(f"✅ Откат для {symbol} успешно выполнен.")
        except Exception as e: self.logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА ОТКАТА для {symbol}! Требуется ручное вмешательство. Ошибка: {e}", exc_info=True)

class OpportunityFinder:
    def __init__(self, exchange: ccxt_async.Exchange, config: Config, db: DatabaseManager, history: SpreadHistory, executor: TradeExecutor):
        self.exchange = exchange; self.config = config; self.db = db; self.history = history; self.executor = executor
        self.logger = logging.getLogger(self.__class__.__name__); self.all_symbols: List[str] = []
    
    async def initialize_symbols(self):
        self.logger.info("Инициализация торговых пар...")
        try:
            all_markets = self.exchange.markets
            
            isolated_pairs_raw = await self.executor._retry_with_backoff(self.exchange.sapi_get_margin_isolated_allpairs)
            isolated_symbols = {item['symbol'] for item in isolated_pairs_raw}
            self.logger.info(f"Найдено {len(isolated_symbols)} пар, поддерживающих изолированную маржу.")

            suitable_symbols = []
            for symbol, market in all_markets.items():
                api_symbol = symbol.replace('/', '')
                if (market and market.get('spot') and market.get('active') and
                    market.get('quote') == self.config.QUOTE_CURRENCY and
                    api_symbol in isolated_symbols):
                    suitable_symbols.append(symbol)

            if not suitable_symbols:
                self.logger.error("Не найдено активных спотовых пар, которые также поддерживают ИЗОЛИРОВАННУЮ маржу.");
                return False
            
            self.logger.info(f"Найдено {len(suitable_symbols)} потенциальных пар. Фильтрация по объему...")
            
            all_tickers = await self.executor._retry_with_backoff(self.exchange.fetch_tickers)
            self.all_symbols = [s for s in suitable_symbols if s in all_tickers and all_tickers[s].get('quoteVolume') and Decimal(str(all_tickers[s]['quoteVolume'])) > self.config.MIN_DAILY_VOLUME_USD]
            
            self.logger.info(f"Найдено {len(self.all_symbols)} подходящих символов после всех фильтраций.");
            return len(self.all_symbols) > 0
        except Exception as e:
            self.logger.critical(f"Критическая ошибка при инициализации символов: {e}", exc_info=True)
            return False

    async def find_best_opportunity(self) -> Optional[Opportunity]:
        try:
            tickers = await self.executor._retry_with_backoff(self.exchange.fetch_tickers, self.all_symbols)
            open_symbols = {p.symbol for p in self.db.get_open_positions()}; opportunities = []
            for symbol, ticker in tickers.items():
                if not (ticker and ticker.get('ask') and ticker.get('bid')) or symbol in open_symbols: continue
                self.history.add_spread(symbol, ticker); mean, stdev = self.history.get_stats(symbol)
                if mean and stdev and stdev > 0:
                    current_spread = self.history.spreads[symbol][-1]; z_score = (current_spread - mean) / stdev
                    if z_score > self.config.STDEV_THRESHOLD_OPEN_LONG: opportunities.append(Opportunity(symbol=symbol, spot_price=Decimal(str(ticker['ask'])), z_score=z_score, direction='LONG'))
                    elif z_score < self.config.STDEV_THRESHOLD_OPEN_SHORT: opportunities.append(Opportunity(symbol=symbol, spot_price=Decimal(str(ticker['bid'])), z_score=z_score, direction='SHORT'))
            if opportunities:
                best_opportunity = max(opportunities, key=lambda x: abs(x.z_score))
                self.logger.info(f"💎 Найдена лучшая возможность: {best_opportunity.direction} {best_opportunity.symbol} (Z-Score: {best_opportunity.z_score:.2f})")
                return best_opportunity
        except Exception as e: self.logger.error(f"Ошибка поиска возможностей: {e}")
        return None

class PositionManager:
    def __init__(self, exchange: ccxt_async.Exchange, config: Config, db: DatabaseManager, history: SpreadHistory, executor: TradeExecutor):
        self.exchange = exchange; self.config = config; self.db = db; self.history = history; self.executor = executor
        self.logger = logging.getLogger(self.__class__.__name__)
        self.trailing_high_water_mark: Dict[str, Optional[Decimal]] = {}

    async def manage_open_positions(self):
        open_positions = self.db.get_open_positions()
        if not open_positions: return
        try:
            symbols_to_check = [p.symbol for p in open_positions]
            tickers = await self.executor._retry_with_backoff(self.exchange.fetch_tickers, symbols_to_check)
        except Exception as e: self.logger.error(f"Не удалось получить тикеры для проверки позиций: {e}"); return
        
        for position in open_positions:
            if position.symbol not in self.trailing_high_water_mark:
                self.trailing_high_water_mark[position.symbol] = None

            if reason := self._check_position_for_closure(position, tickers.get(position.symbol)):
                await self.executor.close_position(position, reason)
                if position.symbol in self.trailing_high_water_mark:
                    del self.trailing_high_water_mark[position.symbol]

    def _check_position_for_closure(self, position: OpenPosition, ticker: Optional[dict]) -> Optional[str]:
        try:
            if not ticker: return None
            self.history.add_spread(position.symbol, ticker)
            mean, stdev = self.history.get_stats(position.symbol)
            if not (mean and stdev and stdev > 0): return None
            
            z_score = (self.history.spreads[position.symbol][-1] - mean) / stdev
            stop_loss_z = position.z_score_entry * self.config.Z_SCORE_STOP_LOSS_FACTOR
            
            self.logger.info(f"Позиция {position.direction} {position.symbol}: Z-Score = {z_score:.2f} (Вход: {position.z_score_entry:.2f}, SL < {stop_loss_z:.2f})")

            high_water_mark = self.trailing_high_water_mark.get(position.symbol)

            if position.direction == 'LONG' and z_score < stop_loss_z:
                return f"Стоп-лосс (Z-Score={z_score:.2f} < {stop_loss_z:.2f})"
            if position.direction == 'SHORT' and z_score > stop_loss_z:
                return f"Стоп-лосс (Z-Score={z_score:.2f} > {stop_loss_z:.2f})"

            if time.time() - position.created_at > self.config.MAX_POSITION_HOLD_TIME_S:
                return "Максимальное время удержания"

            if position.direction == 'LONG':
                if high_water_mark is None and z_score < -self.config.TRAILING_TAKE_PROFIT_ACTIVATION:
                    self.trailing_high_water_mark[position.symbol] = z_score
                    self.logger.info(f"Для {position.symbol} активирован трейлинг-стоп на уровне Z-Score={z_score:.2f}")
                elif high_water_mark is not None:
                    if z_score < high_water_mark:
                        self.trailing_high_water_mark[position.symbol] = z_score
                    elif z_score > high_water_mark + self.config.TRAILING_TAKE_PROFIT_DISTANCE:
                        return f"Трейлинг тейк-профит (Откат от пика {high_water_mark:.2f})"
            
            elif position.direction == 'SHORT':
                if high_water_mark is None and z_score > self.config.TRAILING_TAKE_PROFIT_ACTIVATION:
                    self.trailing_high_water_mark[position.symbol] = z_score
                    self.logger.info(f"Для {position.symbol} активирован трейлинг-стоп на уровне Z-Score={z_score:.2f}")
                elif high_water_mark is not None:
                    if z_score > high_water_mark:
                        self.trailing_high_water_mark[position.symbol] = z_score
                    elif z_score < high_water_mark - self.config.TRAILING_TAKE_PROFIT_DISTANCE:
                        return f"Трейлинг тейк-профит (Откат от пика {high_water_mark:.2f})"

        except Exception as e:
            self.logger.error(f"Ошибка проверки позиции {position.symbol}: {e}", exc_info=True)
        return None

class ArbitrageMaster:
    def __init__(self, exchange, config, db, executor, finder, manager):
        self.exchange = exchange; self.config = config; self.db = db; self.executor = executor
        self.finder = finder; self.manager = manager
        self.logger = logging.getLogger(self.__class__.__name__); self._is_running = True

    @classmethod
    async def create(cls):
        logger.info("Инициализация бота..."); config = Config()
        exchange = getattr(ccxt_async, config.EXCHANGE_ID)({'apiKey': API_KEY, 'secret': API_SECRET, 'options': {'adjustForTimeDifference': True}, 'enableRateLimit': True})
        try:
            logger.info("Загрузка рынков и валют..."); await exchange.load_markets(True); logger.info("Рынки и валюты загружены.")
            
            await cls.cleanup_all_isolated_pairs(exchange)

            logger.info("Проверка разрешений API ключа..."); await exchange.fetch_balance(); logger.info("✅ Права на чтение баланса присутствуют.")
            db = DatabaseManager(config.DATABASE_FILE); db.setup_database()
            executor = TradeExecutor(exchange, config, db); history = SpreadHistory(config.SPREAD_HISTORY_SIZE)
            finder = OpportunityFinder(exchange, config, db, history, executor)
            manager = PositionManager(exchange, config, db, history, executor)
            if not await finder.initialize_symbols():
                raise RuntimeError("Не удалось инициализировать торговые пары. Бот не может продолжать работу.")
            logger.info("Компоненты бота успешно инициализированы.")
            return cls(exchange, config, db, executor, finder, manager)
        except Exception as e:
            logger.critical(f"Критическая ошибка инициализации: {e}", exc_info=True)
            if exchange:
                await exchange.close()
            raise
    
    @staticmethod
    async def cleanup_all_isolated_pairs(exchange: ccxt_async.Exchange):
        logger.info("--- НАЧАЛО ПРОЦЕДУРЫ АВТОМАТИЧЕСКОЙ ОЧИСТКИ ---")
        try:
            logger.info("Получение списка всех активных изолированных пар...")
            isolated_accounts_info = await exchange.sapi_get_margin_isolated_account()
            
            if not isolated_accounts_info or 'assets' not in isolated_accounts_info:
                logger.info("Не найдено активных изолированных пар для очистки.")
                return

            active_pairs = isolated_accounts_info['assets']
            logger.info(f"Найдено {len(active_pairs)} активных пар. Начинаю очистку...")

            for pair_info in active_pairs:
                api_symbol = pair_info['symbol']
                symbol = f"{pair_info['baseAsset']['asset']}/{pair_info['quoteAsset']['asset']}"
                logger.info(f"--- Обработка пары: {symbol} ---")

                for asset_info in [pair_info['baseAsset'], pair_info['quoteAsset']]:
                    asset_code = asset_info['asset']
                    free_amount = Decimal(asset_info['free'])
                    if free_amount > 0:
                        try:
                            max_transferable_info = await exchange.sapi_get_margin_max_transferable({'asset': asset_code, 'isolatedSymbol': api_symbol})
                            max_transferable = Decimal(max_transferable_info['amount'])
                            if max_transferable > 0:
                                logger.info(f"Попытка перевода {max_transferable} {asset_code} на спот...")
                                await exchange.transfer(asset_code, float(max_transferable), 'isolated', 'spot', params={'symbol': api_symbol})
                                logger.info(f"Успешно переведено {max_transferable} {asset_code}.")
                                await asyncio.sleep(1.5)
                        except Exception as e:
                            logger.error(f"Ошибка при переводе {asset_code} для {symbol}: {e}")
                
                try:
                    await exchange.sapi_delete_margin_isolated_account({'symbol': api_symbol})
                    logger.info(f"✅ Пара {symbol} успешно деактивирована.")
                except Exception as e:
                    logger.warning(f"Не удалось деактивировать {symbol}: {e}")
                
                await asyncio.sleep(2.0)
        except Exception as e:
            logger.error(f"Произошла ошибка во время автоматической очистки: {e}")
        finally:
            logger.info("--- ЗАВЕРШЕНИЕ ПРОЦЕДУРЫ АВТОМАТИЧЕСКОЙ ОЧИСТКИ ---")

    async def run_cycle(self):
        try:
            await self.manager.manage_open_positions()
            if len(self.db.get_open_positions()) < self.config.MAX_OPEN_POSITIONS:
                if opportunity := await self.finder.find_best_opportunity():
                    if opportunity.direction == 'LONG': await self.executor.open_long_position(opportunity)
                    elif opportunity.direction == 'SHORT': await self.executor.open_short_position(opportunity)
            else: self.logger.debug(f"Достигнут лимит открытых позиций ({self.config.MAX_OPEN_POSITIONS}). Поиск отложен.")
        except CleanupException as e: self.logger.warning(str(e))
        except Exception as e: self.logger.error(f"Неперехваченная ошибка в цикле: {e}", exc_info=True)

    async def start(self):
        self.logger.info("🚀 Бот успешно запущен. Начинаю торговый цикл...")
        while self._is_running:
            try: await self.run_cycle(); await asyncio.sleep(self.config.TRADE_CYCLE_SLEEP_S)
            except asyncio.CancelledError: break

    async def shutdown(self):
        self._is_running = False; self.logger.info("Начинаю процедуру завершения работы...")
        if self.exchange:
            try: await self.exchange.close(); self.logger.info("Соединение с биржей закрыто.")
            except Exception: pass

async def main():
    bot = None
    try:
        bot = await ArbitrageMaster.create(); await bot.start()
    except (KeyboardInterrupt, asyncio.CancelledError): logger.info("Бот останавливается...")
    except Exception as e: logger.critical(f"Не удалось запустить бота из-за критической ошибки во время создания: {e}")
    finally:
        if bot: await bot.shutdown()
        logger.info("Работа бота завершена.")

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: logger.info("Программа прервана.")