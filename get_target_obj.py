
import json

CAPITAL = 100000  # Capital for trade calculations

# Round to nearest base value (like 0.05 or 0.1)
def mround(value, base):
    return round(base * round(float(value) / base), 2)

# Risk/Reward calculation with labels
def calculate_and_save(open_price, yesterday_high, yesterday_low):
    open_price = float(open_price)
    yesterday_high = float(yesterday_high)
    yesterday_low = float(yesterday_low)

    risk_per_trade = CAPITAL * 0.01      # 1% of capital
    target_profit = CAPITAL * 0.015      # 1.5% of capital
    range_value = yesterday_high - yesterday_low

    buy_entry = mround((open_price + (range_value * 0.55)), 0.05)
    sell_entry = mround((open_price - (range_value * 0.55)), 0.05)

    buy_stoploss = mround(buy_entry - (buy_entry * 0.0135), 0.05)
    sell_stoploss = mround(sell_entry + (sell_entry * 0.0135), 0.05)

    risk_buy = buy_entry - buy_stoploss
    risk_sell = sell_stoploss - sell_entry

    shares_num = mround(risk_per_trade / risk_buy, 1) if risk_buy else 0

    buy_stopgain = mround(buy_entry + (target_profit / shares_num), 0.05) if shares_num else 0
    sell_stopgain = mround(sell_entry - (target_profit / shares_num), 0.05) if shares_num else 0

    # ₹ risk/reward values
    buy_total_risk = round(risk_buy * shares_num, 2)
    buy_total_reward = round((buy_stopgain - buy_entry) * shares_num, 2)

    sell_total_risk = round(risk_sell * shares_num, 2)
    sell_total_reward = round((sell_entry - sell_stopgain) * shares_num, 2)

    # Reward-to-risk ratios
    buy_risk_reward = round((buy_stopgain - buy_entry) / risk_buy, 2) if risk_buy else None
    sell_risk_reward = round((sell_entry - sell_stopgain) / risk_sell, 2) if risk_sell else None

    return {
        "Call_Option (Buy)": {
            "Entry": buy_entry,
            "Target (Profit)": buy_stopgain,
            "Stop Loss (Exit)": buy_stoploss,
            "Risk/Reward Ratio": buy_risk_reward,
            "Risk": buy_total_risk,
            "Reward": buy_total_reward
        },
        "Put_Option (Sell)": {
            "Entry": sell_entry,
            "Target (Profit)": sell_stopgain,
            "Stop Loss (Exit)": sell_stoploss,
            "Risk/Reward Ratio": sell_risk_reward,
            "Risk": sell_total_risk,
            "Reward": sell_total_reward
        },
        "Shares_count": int(shares_num),
        "capital_amount":CAPITAL
    }

# # Example usage
# data = calculate_and_save(
#     open_price=3518.80,       # Today's close = Tomorrow's open
#     yesterday_high=3558.00,   # Today's high
#     yesterday_low=3494.00     # Today's low
# )
