import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
import plotly.graph_objects as go
import plotly.subplots as sp
import joblib

class StockDataFetcher:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host='localhost', user='root', password='Kamble@123', database='live_hist')
            self.cursor = self.connection.cursor()
        except mysql.connector.Error as e:
            print(f"Database connection error: {e}")
    
    def fetch_stock_data_from_db(self, symbol):
        query = f"SELECT * FROM {symbol.lower()} ORDER BY date"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        
        if not rows:
            print(f"No data found for {symbol} in the database.")
            return None
        
        # Convert rows to pandas DataFrame
        df = pd.DataFrame(rows, columns=['symbol', 'date', 'open', 'high', 'low', 'close', 'volume'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        return df

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

class StockAnalysis:
    def __init__(self, data):
        self.data = data

    def preprocess_data(self):
        df = self.data.copy()

        for col in df.select_dtypes(include=[np.number]).columns:
            q1, q3 = df[col].quantile([0.25, 0.75])
            iqr = q3 - q1
            upper_limit, lower_limit = q3 + 1.5 * iqr, q1 - 1.5 * iqr
            df[col] = df[col].clip(lower_limit, upper_limit)

        df.fillna(method='ffill', inplace=True)
        df.fillna(method='bfill', inplace=True)
        df['SMA_10'] = df['close'].rolling(window=10).mean()
        df['SMA_50'] = df['close'].rolling(window=50).mean()
        df['SMA_200'] = df['close'].rolling(window=200).mean()
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()
        
        # Normalize the data
        scaler = MinMaxScaler()
        df[['open', 'high', 'low', 'close', 'volume']] = scaler.fit_transform(df[['open', 'high', 'low', 'close', 'volume']])
        
        return df, scaler

class Indicators:
    @staticmethod
    def calculate_rsi(data, period=14):
        delta = data['close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(period).mean()
        avg_loss = loss.rolling(period).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        data['RSI'] = rsi
        return data
    
    @staticmethod
    def calculate_macd(data):
        short_ema = data['close'].ewm(span=12, adjust=False).mean()
        long_ema = data['close'].ewm(span=26, adjust=False).mean()
        data['MACD'] = short_ema - long_ema
        data['Signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
        return data
    
    @staticmethod
    def recommend_stock_action(data):
        data = Indicators.calculate_macd(data)
        data = Indicators.calculate_rsi(data)

        data['SMA_200'] = data['close'].rolling(window=200).mean()

        latest_rsi = data['RSI'].iloc[-1]
        latest_macd = data['MACD'].iloc[-1]
        latest_signal = data['Signal_Line'].iloc[-1]
        latest_sma50 = data['SMA_50'].iloc[-1]
        latest_sma200 = data['SMA_200'].iloc[-1]
        latest_ema50 = data['EMA_50'].iloc[-1]
        latest_ema200 = data['EMA_200'].iloc[-1]

        buy_signals, sell_signals = [], []

        if latest_sma50 > latest_sma200:
            buy_signals.append("SMA_50 above SMA_200 (Golden Cross)")
        elif latest_sma50 < latest_sma200:
            sell_signals.append("SMA_50 below SMA_200 (Death Cross)")

        if latest_ema50 > latest_ema200:
            buy_signals.append("EMA_50 above EMA_200 (Golden Cross)")
        elif latest_ema50 < latest_ema200:
            sell_signals.append("EMA_50 below EMA_200 (Death Cross)")

        if latest_rsi < 30:
            buy_signals.append("RSI below 30 (Oversold)")
        elif latest_rsi > 70:
            sell_signals.append("RSI above 70 (Overbought)")

        if latest_macd > latest_signal:
            buy_signals.append("MACD above Signal Line")
        elif latest_macd < latest_signal:
            sell_signals.append("MACD below Signal Line")

        if len(buy_signals) > len(sell_signals):
            return f"**Recommendation: BUY** 📈\nReasons: {', '.join(buy_signals)}"
        elif len(sell_signals) > len(buy_signals):
            return f"**Recommendation: SELL** 📉\nReasons: {', '.join(sell_signals)}"
        else:
            return "**Recommendation: HOLD** 🤔\nMarket is neutral or mixed signals."

class StockModelTrainer:
    def train_ml_model(self, df):
        X, y = df[['open', 'high', 'low', 'volume']], df['close']
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        joblib.dump(model, "stock_prediction_ML_model.pkl")
        return model
    
    def train_dl_model(self, df):
        X = df[['open', 'high', 'low', 'volume']].values.reshape(df.shape[0], 1, 4)
        y = df['close'].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = Sequential([
            LSTM(50, activation='relu', return_sequences=True, input_shape=(1, 4)),
            LSTM(50, return_sequences=False),
            Dense(25),
            Dense(1)
        ])
        model.compile(optimizer='adam', loss='mean_squared_error')
        model.fit(X_train, y_train, epochs=10, batch_size=32, verbose=1)
        model.save('stock_prediction_DL_model.h5')
        return model

class Predictions:
    @staticmethod
    def predict_current_price(data, ml_model, dl_model, scaler):
        latest_data = data[['open', 'high', 'low', 'volume']].iloc[-1].values.reshape(1, -1)

        # ML Prediction
        ml_prediction = ml_model.predict(latest_data)[0]

        # DL Prediction
        latest_data_dl = np.reshape(latest_data, (latest_data.shape[0], 1, latest_data.shape[1]))
        dl_prediction = dl_model.predict(latest_data_dl)[0][0]

        # Reverse Scaling
        original_min = scaler.data_min_[3]  # 'close' column min
        original_max = scaler.data_max_[3]  # 'close' column max

        ml_actual_price = ml_prediction * (original_max - original_min) + original_min
        dl_actual_price = dl_prediction * (original_max - original_min) + original_min

        return ml_actual_price, dl_actual_price

class StockVisualization:
    def generate_charts(self, data, symbol):
        fig = sp.make_subplots(rows=2, cols=2,
                              subplot_titles=(f'{symbol} Stock Price with SMA',
                                              f'{symbol} Stock Price with EMA',
                                              'Relative Strength Index',
                                              'MACD Indicator'),
                              vertical_spacing=0.2)

        fig.add_trace(go.Scatter(x=data.index, y=data['close'], mode='lines', name='Close Price', line=dict(color='blue')), row=1, col=1)
        fig.add_trace(go.Scatter(x=data.index, y=data['SMA_50'], mode='lines', name='50-Day SMA', line=dict(color='red')), row=1, col=1)
        fig.add_trace(go.Scatter(x=data.index, y=data['SMA_200'], mode='lines', name='200-Day SMA', line=dict(color='green')), row=1, col=1)

        fig.add_trace(go.Scatter(x=data.index, y=data['EMA_50'], mode='lines', name='50-Day EMA', line=dict(color='purple')), row=1, col=2)
        fig.add_trace(go.Scatter(x=data.index, y=data['EMA_200'], mode='lines', name='200-Day EMA', line=dict(color='orange')), row=1, col=2)

        fig.add_trace(go.Scatter(x=data.index, y=data['RSI'], mode='lines', name='RSI', line=dict(color='brown')), row=2, col=1)
        fig.add_trace(go.Scatter(x=data.index, y=[70]*len(data), mode='lines', name='Overbought (70)', line=dict(color='red', dash='dash')), row=2, col=1)
        fig.add_trace(go.Scatter(x=data.index, y=[30]*len(data), mode='lines', name='Oversold (30)', line=dict(color='green', dash='dash')), row=2, col=1)

        if 'MACD' in data.columns and 'Signal_Line' in data.columns:
            fig.add_trace(go.Scatter(x=data.index, y=data['MACD'], mode='lines', name='MACD', line=dict(color='blue')), row=2, col=2)
            fig.add_trace(go.Scatter(x=data.index, y=data['Signal_Line'], mode='lines', name='Signal Line', line=dict(color='red', dash='dash')), row=2, col=2)

        fig.update_layout(title=f'{symbol} Stock Analysis',
                          xaxis_title='Date',
                          yaxis_title='Price',
                          height=1200, width=1500,
                          showlegend=True)
        fig.show()

def main():
    symbols = ['GOOG', 'AMZN','AAPL']
    trainer = StockModelTrainer()
    visualizer = StockVisualization()
    fetcher = StockDataFetcher()
    for symbol in symbols:
        data = fetcher.fetch_stock_data_from_db(symbol)
        if data is not None:
            analysis = StockAnalysis(data)
            df, scaler = analysis.preprocess_data()
            ml_model = trainer.train_ml_model(df)
            dl_model = trainer.train_dl_model(df)
            df = Indicators.calculate_rsi(df)
            df = Indicators.calculate_macd(df)
            visualizer.generate_charts(df, symbol)
            ml_actual_price, dl_actual_price = Predictions.predict_current_price(df, ml_model, dl_model, scaler)
            recommend = Indicators.recommend_stock_action(df)
            print(f"Symbol: {symbol}")
            print(f"ML Actual Price: {ml_actual_price}")
            print(f"DL Actual Price: {dl_actual_price}")
            print(f"Recommendation: {recommend}")
            print()

if __name__ == "__main__":
    main()
