import pandas as pd
import os

# Папка с объединёнными файлами
input_dir = "combined_output"

# Загружаем данные
kill_df = pd.read_csv(os.path.join(input_dir, "combined_kill_dump.csv"))
regional_df = pd.read_csv(os.path.join(input_dir, "combined_regional_stats.csv"))
mining_df = pd.read_csv(os.path.join(input_dir, "combined_mining_by_region.csv"))
moon_df = pd.read_csv(os.path.join(input_dir, "combined_moon_materials_by_region.csv"))

# --- Сводные таблицы по каждому источнику ---

# Kill dump: уничтожения и ISK-потери
kill_summary = (
    kill_df.groupby(["region", "month"])
    .agg(total_kills=("killCount", "sum"),
         total_isk_lost=("iskLost", "sum"))
    .reset_index()
)

# Regional stats: производство и торговля
regional_summary = (
    regional_df.groupby(["region", "month"])
    .agg(total_production=("productionValue", "sum"),
         total_trade=("tradeValue", "sum"))
    .reset_index()
)

# Mining: добыча руды
mining_summary = (
    mining_df.groupby(["region", "month"])
    .agg(total_mining=("quantity", "sum"))
    .reset_index()
)

# Moon materials: добыча луны
moon_summary = (
    moon_df.groupby(["region", "month"])
    .agg(total_moon_mining=("quantity", "sum"))
    .reset_index()
)

# --- Объединение всех сводных таблиц ---
summary = (
    kill_summary
    .merge(regional_summary, on=["region", "month"], how="outer")
    .merge(mining_summary, on=["region", "month"], how="outer")
    .merge(moon_summary, on=["region", "month"], how="outer")
)

# Заполняем пропуски нулями (если в каком-то месяце нет данных)
summary = summary.fillna(0)

# Сохраняем итоговую таблицу
output_path = os.path.join(input_dir, "summary_by_region_month.csv")
summary.to_csv(output_path, index=False)

print("Сводная таблица сохранена:", output_path)
print(summary.head())
