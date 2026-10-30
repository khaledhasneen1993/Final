"""Round trips reconstructed from actual fills, including all scale-in fees."""
def round_trips(fills):
    opened, closed = {}, []
    for fill in fills:
        symbol = fill['symbol']
        if not fill.get('reduceOnly'):
            if symbol not in opened:
                opened[symbol] = {'symbol': symbol, 'side': fill['side'],
                    'entry_time': fill.get('timestamp'), 'fills': [], 'amount': 0.,
                    'cost': 0., 'fees_usdt': 0., 'entry_reason': fill.get('entry_reason'),
                    'entry_signal': fill.get('entry_signal')}
            p = opened[symbol]
            p['fills'].append(fill)
            p['amount'] += fill['amount']
            p['cost'] += fill['amount'] * fill['price']
            p['fees_usdt'] += fill.get('fee_usdt', 0)
            p['average_entry'] = p['cost'] / p['amount']
            p['adds'] = len(p['fills']) - 1
        else:
            p = opened.pop(symbol, None)
            if p is None:
                continue  # Never invent an entry missing from an imported ledger.
            p.update(exit_time=fill.get('timestamp'), exit_price=fill['price'],
                     exit_reason=fill.get('exit_reason'), gross_pnl_usdt=fill.get('pnl_usdt', 0))
            p['fees_usdt'] += fill.get('fee_usdt', 0)
            p['net_pnl_usdt'] = p['gross_pnl_usdt'] - p['fees_usdt']
            p['fills'].append(fill)
            closed.append(p)
    return list(opened.values()), closed
