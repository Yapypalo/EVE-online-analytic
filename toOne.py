import os
import pandas as pd

# Папки с данными (июль 202507 отсутствует)
month_dirs = [
    'EVEOnline_MER_202501', 'EVEOnline_MER_202502', 'EVEOnline_MER_202503',
    'EVEOnline_MER_202504', 'EVEOnline_MER_202505', 'EVEOnline_MER_202506',
    'EVEOnline_MER_202508'
]

# Целевые файлы, которые хотим объединить
target_files = [
    'mining_by_region.csv',
    'moon_materials_by_region.csv',
    'regional_stats.csv',
    'kill_dump.csv'
]

# Словарь для хранения датафреймов
combined_data = {file: [] for file in target_files}

# Проход по папкам и сбор данных
for month_dir in month_dirs:
    month = month_dir.split('_')[-1]  # YYYYMM
    for file in target_files:
        file_path = os.path.join(month_dir, file)
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                df['month'] = month
                combined_data[file].append(df)
            except Exception as e:
                print(f"Ошибка при чтении {file_path}: {e}")

# Объединение по типу файла
final_combined = {}
for file, dfs in combined_data.items():
    if dfs:
        final_combined[file] = pd.concat(dfs, ignore_index=True)
        print(f"\n===== {file} ({len(final_combined[file])} строк) =====")
        print(final_combined[file].head())
    else:
        print(f"\nНет данных для {file}")

# Теперь у тебя есть:
# final_combined['mining_by_region.csv']
# final_combined['moon_materials_by_region.csv']
# final_combined['regional_stats.csv']
# final_combined['kill_dump.csv']
