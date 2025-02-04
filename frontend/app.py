from flask import Flask, render_template, request
import mysql.connector
import pandas as pd
import numpy as np
import joblib
from tensorflow.keras.models import load_model
from app3 import StockDataFetcher, StockAnalysis, StockModelTrainer, Predictions, Indicators, StockVisualization

app = Flask(__name__)

# Load your models
ml_model = joblib.load(r'C:\Users\prani\frontend\models\stock_prediction_ML_model.pkl')
dl_model = load_model(r'C:\Users\prani\frontend\models\stock_prediction_DL_model.h5')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    symbol = request.form['symbol']
    fetcher = StockDataFetcher()
    data = fetcher.fetch_stock_data_from_db(symbol)

    if data is not None:
        analysis = StockAnalysis(data)
        df, scaler = analysis.preprocess_data()
        df = Indicators.calculate_rsi(df)
        df = Indicators.calculate_macd(df)

        # Make predictions
        ml_actual_price, dl_actual_price = Predictions.predict_current_price(df, ml_model, dl_model, scaler)
        recommend = Indicators.recommend_stock_action(df)

        # Generate charts
        visualizer = StockVisualization()
        visualizer.generate_charts(df, symbol)

        return render_template('results.html', symbol=symbol, ml_price=ml_actual_price, dl_price=dl_actual_price, recommendation=recommend)
    else:
        return render_template('results.html', symbol=symbol, ml_price=None, dl_price=None, recommendation="No data found.")

if __name__ == "__main__":
    app.run(debug=True)