# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Step 5: Demand Forecasting with MLflow
# MAGIC ## Train, track, register and serve a sales forecasting model

# COMMAND ----------
# Install required libraries
%pip install prophet scikit-learn --quiet

# COMMAND ----------
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
from pyspark.sql import functions as F

catalog = "retail_intelligence"
mlflow.set_experiment("/Shared/retail_intelligence/demand_forecasting")

# COMMAND ----------
# MAGIC %md ## 1. Load Gold Sales Data for Training

# COMMAND ----------
# Load aggregated daily sales from Gold layer
df_gold = spark.table(f"{catalog}.gold.gold_daily_sales").toPandas()
df_gold["order_date"] = pd.to_datetime(df_gold["order_date"])
df_gold = df_gold.sort_values("order_date")

print(f"Training data: {df_gold.shape[0]} days of sales data")
display(df_gold.head())

# COMMAND ----------
# MAGIC %md ## 2. Prepare Prophet Format (ds, y)

# COMMAND ----------
# Prophet expects columns: ds (date) and y (target)
df_prophet = df_gold[["order_date","total_revenue"]].rename(
    columns={"order_date": "ds", "total_revenue": "y"}
)

# Train/test split — last 30 days as test
split_date = df_prophet["ds"].max() - pd.Timedelta(days=30)
train = df_prophet[df_prophet["ds"] <= split_date]
test  = df_prophet[df_prophet["ds"] >  split_date]

print(f"Train: {len(train)} rows | Test: {len(test)} rows")

# COMMAND ----------
# MAGIC %md ## 3. Train Prophet Model + Log with MLflow

# COMMAND ----------
with mlflow.start_run(run_name="prophet_demand_forecast_v1") as run:
    # Hyperparameters
    changepoint_prior = 0.1
    seasonality_prior = 10.0
    
    mlflow.log_params({
        "model_type":           "Prophet",
        "changepoint_prior":    changepoint_prior,
        "seasonality_prior":    seasonality_prior,
        "training_rows":        len(train),
        "forecast_horizon_days": 90,
    })

    # Train
    model = Prophet(
        changepoint_prior_scale=changepoint_prior,
        seasonality_prior_scale=seasonality_prior,
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=False,
    )
    model.add_country_holidays(country_name="US")
    model.fit(train)
    
    # Evaluate on test set
    future_test = model.make_future_dataframe(periods=len(test))
    forecast    = model.predict(future_test)
    test_pred   = forecast[forecast["ds"].isin(test["ds"])]["yhat"].values
    test_actual = test["y"].values
    
    mae  = mean_absolute_error(test_actual, test_pred)
    rmse = np.sqrt(mean_squared_error(test_actual, test_pred))
    mape = np.mean(np.abs((test_actual - test_pred) / test_actual)) * 100
    
    mlflow.log_metrics({"mae": mae, "rmse": rmse, "mape": mape})
    print(f"📊 MAE: ${mae:,.0f} | RMSE: ${rmse:,.0f} | MAPE: {mape:.1f}%")
    
    # Log model
    mlflow.sklearn.log_model(
        model,
        artifact_path="prophet_model",
        registered_model_name="retail_demand_forecast",
        input_example=train.tail(5),
    )
    
    run_id = run.info.run_id
    print(f"✅ Model logged — Run ID: {run_id}")

# COMMAND ----------
# MAGIC %md ## 4. Generate 90-Day Forecast

# COMMAND ----------
future_90 = model.make_future_dataframe(periods=90)
forecast_90 = model.predict(future_90)

# Keep only future predictions
last_train_date = train["ds"].max()
future_only = forecast_90[forecast_90["ds"] > last_train_date][
    ["ds","yhat","yhat_lower","yhat_upper"]
].rename(columns={"ds":"forecast_date","yhat":"predicted_revenue",
                   "yhat_lower":"lower_bound","yhat_upper":"upper_bound"})

print(f"📅 90-day forecast generated: {len(future_only)} rows")
display(future_only.head(10))

# COMMAND ----------
# MAGIC %md ## 5. Write Forecast to Gold Layer

# COMMAND ----------
spark.createDataFrame(future_only).write.mode("overwrite").saveAsTable(
    f"{catalog}.gold.gold_forecast_90day"
)
print(f"✅ Forecast saved to {catalog}.gold.gold_forecast_90day")

# COMMAND ----------
# MAGIC %md ## 6. Register Model to Production

# COMMAND ----------
from mlflow.tracking import MlflowClient
client = MlflowClient()

# Transition to Production stage
latest = client.get_latest_versions("retail_demand_forecast", stages=["None"])
if latest:
    client.transition_model_version_stage(
        name="retail_demand_forecast",
        version=latest[0].version,
        stage="Production",
        archive_existing_versions=True,
    )
    print(f"✅ Model v{latest[0].version} promoted to Production")
