import os
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class EveDataConsolidatorFinal:
    """Окончательная версия консолидатора данных EVE Online"""
    
    def __init__(self):
        self.archives_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\архивы")
        self.output_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.consolidated_data = []
        self.log_file = self.output_dir / "consolidation_final_log.txt"
        
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write("Лог консолидации данных EVE Online (финальная версия)\n")
            f.write("=" * 60 + "\n")
    
    def log_message(self, message):
        print(message)
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(message + "\n")
    
    def parse_date_from_folder(self, folder_name):
        """Извлекает дату из имени папки"""
        try:
            parts = folder_name.split('_')
            if len(parts) < 3:
                return None
                
            month_year = parts[-1]
            month_str = month_year[:3]
            year_str = month_year[3:]
            
            month_map = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            }
            
            if month_str in month_map and year_str.isdigit():
                return f"{year_str}-{month_map[month_str]}-01"
            return None
        except Exception as e:
            self.log_message(f"Ошибка при разборе даты из '{folder_name}': {e}")
            return None
    
    def find_file(self, folder_path, possible_names):
        """Находит файл по списку возможных имён"""
        for name in possible_names:
            file_path = folder_path / name
            if file_path.exists():
                return file_path
        return None
    
    def extract_production_data_fixed(self, folder_path, target_date):
        """ИСПРАВЛЕННАЯ функция извлечения данных о производстве"""
        result = {}
        
        # Возможные имена файлов
        possible_files = [
            "ProducedDestroyedMined.csv",
            "produced_destroyed_mined.csv",
        ]
        
        file_path = self.find_file(folder_path, possible_files)
        
        if not file_path:
            self.log_message(f"    Файл с производством не найден")
            return result
        
        try:
            df = pd.read_csv(file_path)
            self.log_message(f"    Файл найден: {file_path.name}, строк: {len(df)}")
            
            # ВАЖНО: Выводим ВСЕ столбцы для отладки
            all_columns = list(df.columns)
            self.log_message(f"    Все столбцы в файле: {all_columns}")
            
            # Определяем столбец с датой
            date_column = None
            if 'history_date' in df.columns:
                date_column = 'history_date'
            elif 'date' in df.columns:
                date_column = 'date'
            
            if not date_column:
                self.log_message(f"    ОШИБКА: Нет столбца с датой!")
                return result
            
            # Преобразуем дату
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            # Фильтруем по целевому месяцу
            target_year_month = target_date.strftime('%Y-%m')
            df['year_month'] = df[date_column].dt.strftime('%Y-%m')
            month_data = df[df['year_month'] == target_year_month]
            
            self.log_message(f"    Найдено {len(month_data)} записей за {target_year_month}")
            
            if len(month_data) == 0:
                self.log_message(f"    ВНИМАНИЕ: Нет данных за {target_year_month}!")
                return result
            
            # ВАЖНО: Проверяем первые несколько строк для отладки
            self.log_message(f"    Первые 3 строки за {target_year_month}:")
            for i, (_, row) in enumerate(month_data.head(3).iterrows()):
                self.log_message(f"      Строка {i+1}: {row[date_column].date()}, производство={row.get('production_isk', row.get('produced', 'N/A'))}")
            
            # Ищем данные о ПРОИЗВОДСТВЕ
            if 'production_isk' in month_data.columns:
                result['production_isk'] = float(month_data['production_isk'].sum())
                self.log_message(f"    Найден production_isk: {result['production_isk']:,.2f}")
            elif 'produced' in month_data.columns:
                result['production_isk'] = float(month_data['produced'].sum())
                self.log_message(f"    Найден produced: {result['production_isk']:,.2f}")
            else:
                self.log_message(f"    ВНИМАНИЕ: Столбец производства не найден!")
                # Показываем доступные столбцы
                self.log_message(f"    Доступные столбцы: {list(month_data.columns)}")
            
            # Ищем данные об УНИЧТОЖЕНИИ
            if 'destruction_isk' in month_data.columns:
                result['destruction_isk'] = float(month_data['destruction_isk'].sum())
                self.log_message(f"    Найден destruction_isk: {result['destruction_isk']:,.2f}")
            elif 'destroyed' in month_data.columns:
                result['destruction_isk'] = float(month_data['destroyed'].sum())
                self.log_message(f"    Найден destroyed: {result['destruction_isk']:,.2f}")
            
            # Ищем данные о ДОБЫЧЕ
            if 'mining_isk' in month_data.columns:
                result['mining_isk'] = float(month_data['mining_isk'].sum())
                self.log_message(f"    Найден mining_isk: {result['mining_isk']:,.2f}")
            elif 'mining.value' in month_data.columns:
                result['mining_isk'] = float(month_data['mining.value'].sum())
                self.log_message(f"    Найден mining.value: {result['mining_isk']:,.2f}")
            elif 'mining' in month_data.columns:
                result['mining_isk'] = float(month_data['mining'].sum())
                self.log_message(f"    Найден mining: {result['mining_isk']:,.2f}")
            
        except Exception as e:
            self.log_message(f"    ОШИБКА при чтении файла {file_path.name}: {e}")
            import traceback
            self.log_message(f"    Трассировка: {traceback.format_exc()}")
        
        return result
    
    def extract_trade_data_fixed(self, folder_path):
        """Извлечение торговых данных"""
        result = {}
        
        possible_files = [
            "RegionalStats.csv",
            "regional_stats.csv",
        ]
        
        file_path = self.find_file(folder_path, possible_files)
        
        if not file_path:
            return result
        
        try:
            df = pd.read_csv(file_path)
            
            # Ищем столбцы с торговлей
            if 'trade_value' in df.columns:
                result['trade_value'] = float(df['trade_value'].sum())
            elif 'trade.value' in df.columns:
                result['trade_value'] = float(df['trade.value'].sum())
            elif 'trade' in df.columns:
                result['trade_value'] = float(df['trade'].sum())
            
            if 'exports' in df.columns:
                result['total_exports'] = float(df['exports'].sum())
            elif 'export' in df.columns:
                result['total_exports'] = float(df['export'].sum())
            
            if 'imports' in df.columns:
                result['total_imports'] = float(df['imports'].sum())
            elif 'import' in df.columns:
                result['total_imports'] = float(df['import'].sum())
            
        except Exception as e:
            self.log_message(f"    Ошибка при чтении торговых данных: {e}")
        
        return result
    
    def extract_kill_data_fixed(self, folder_path):
        """Извлечение данных о потерях"""
        result = {'total_isk_destroyed': 0.0}
        
        possible_files = [
            "kill_dump.csv",
            "Killdump.csv",
            "kills.csv",
            "Kills.csv"
        ]
        
        file_path = self.find_file(folder_path, possible_files)
        
        if not file_path:
            return result
        
        try:
            # Пробуем разные разделители
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                sample = f.read(5000)
            
            sep = ';' if ';' in sample else ','
            
            df = pd.read_csv(file_path, sep=sep, low_memory=False, on_bad_lines='skip')
            
            # Ищем столбец с потерями
            for col in df.columns:
                col_lower = col.lower()
                if 'isk' in col_lower and ('destroyed' in col_lower or 'lost' in col_lower):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    total = float(df[col].sum())
                    result['total_isk_destroyed'] = total
                    break
            
        except Exception as e:
            self.log_message(f"    Ошибка при чтении данных о потерях: {e}")
        
        return result
    
    def extract_money_data_fixed(self, folder_path):
        """Извлечение данных о денежной массе"""
        result = {}
        
        possible_files = [
            "MoneySupply.csv",
            "money_supply.csv",
        ]
        
        file_path = self.find_file(folder_path, possible_files)
        
        if not file_path:
            return result
        
        try:
            df = pd.read_csv(file_path)
            
            # Ищем столбец скорости обращения
            for col in df.columns:
                col_lower = col.lower()
                if 'velocity' in col_lower:
                    result['isk_velocity'] = float(df[col].mean())
                    break
            
            # Ищем общую денежную массу
            if 'total_isk' in df.columns:
                result['total_isk'] = float(df['total_isk'].mean())
            elif 'total' in df.columns and 'isk' in str(df.columns).lower():
                # Может быть, столбец называется просто 'total'
                result['total_isk'] = float(df['total'].mean())
            
        except Exception as e:
            self.log_message(f"    Ошибка при чтении денежных данных: {e}")
        
        return result
    
    def process_month_fixed(self, folder_path, date_str):
        """Обработка данных за один месяц (исправленная)"""
        month_data = {"history_date": date_str}
        target_date = pd.to_datetime(date_str)
        
        self.log_message(f"\n{'='*50}")
        self.log_message(f"ОБРАБОТКА: {folder_path.name} ({date_str})")
        self.log_message(f"{'='*50}")
        
        # 1. Производство, уничтожение, добыча
        prod_data = self.extract_production_data_fixed(folder_path, target_date)
        month_data.update(prod_data)
        
        # 2. Торговля
        trade_data = self.extract_trade_data_fixed(folder_path)
        month_data.update(trade_data)
        
        # 3. Потери
        kill_data = self.extract_kill_data_fixed(folder_path)
        month_data.update(kill_data)
        
        # 4. Денежная масса
        money_data = self.extract_money_data_fixed(folder_path)
        month_data.update(money_data)
        
        # Проверяем, что данные извлечены
        extracted_count = len(month_data) - 1  # минус history_date
        self.log_message(f"    Извлечено показателей: {extracted_count}")
        
        # Логируем ключевые показатели
        key_metrics = ['production_isk', 'destruction_isk', 'mining_isk', 'trade_value', 'total_isk_destroyed']
        for metric in key_metrics:
            if metric in month_data:
                value = month_data[metric]
                if metric == 'isk_velocity':
                    self.log_message(f"    {metric}: {value:.4f}")
                else:
                    self.log_message(f"    {metric}: {value:,.2f}")
        
        if extracted_count == 0:
            self.log_message(f"    ВНИМАНИЕ: Не удалось извлечь ни одного показателя!")
        
        return month_data
    
    def consolidate_all_months_fixed(self):
        """Консолидация данных за все месяцы (исправленная)"""
        self.log_message("Начинаю консолидацию данных (исправленная версия)...")
        
        # Получаем все папки с отчётами
        mer_folders = sorted([f for f in os.listdir(self.archives_dir) 
                            if f.startswith("EVEOnline_MER_")])
        
        self.log_message(f"Найдено папок: {len(mer_folders)}")
        
        processed_count = 0
        for folder_name in mer_folders:
            date_str = self.parse_date_from_folder(folder_name)
            if not date_str:
                self.log_message(f"Пропускаю папку: {folder_name} (не удалось определить дату)")
                continue
            
            folder_path = self.archives_dir / folder_name
            if not folder_path.exists():
                self.log_message(f"Пропускаю: {folder_name} (папка не существует)")
                continue
            
            # Обрабатываем месяц
            month_data = self.process_month_fixed(folder_path, date_str)
            
            if month_data and len(month_data) > 1:  # Есть хотя бы один показатель кроме даты
                self.consolidated_data.append(month_data)
                processed_count += 1
            else:
                self.log_message(f"Пропускаю: {folder_name} (нет данных)")
        
        if not self.consolidated_data:
            self.log_message("Не удалось получить данные ни за один месяц!")
            return None
        
        # Создаём DataFrame
        df = pd.DataFrame(self.consolidated_data)
        df["history_date"] = pd.to_datetime(df["history_date"])
        df = df.sort_values("history_date").reset_index(drop=True)
        
        self.log_message(f"\nКонсолидация завершена!")
        self.log_message(f"Успешно обработано месяцев: {processed_count}")
        self.log_message(f"Период: {df['history_date'].min().date()} - {df['history_date'].max().date()}")
        
        return df
    
    def add_war_indicator(self, df, percentile=75):
        """Добавление индикатора военных периодов"""
        if "total_isk_destroyed" not in df.columns:
            self.log_message("Невозможно добавить индикатор войн: нет данных о потерях")
            return df
        
        threshold = df["total_isk_destroyed"].quantile(percentile / 100)
        df["is_war_period"] = (df["total_isk_destroyed"] >= threshold).astype(int)
        
        war_months = df["is_war_period"].sum()
        
        self.log_message(f"\nСтатистика военных периодов:")
        self.log_message(f"  Порог: {threshold:,.2f} ISK")
        self.log_message(f"  Военных месяцев: {war_months} ({war_months/len(df)*100:.1f}%)")
        self.log_message(f"  Мирных месяцев: {len(df)-war_months} ({(len(df)-war_months)/len(df)*100:.1f}%)")
        
        return df
    
    def save_results_fixed(self, df):
        """Сохранение результатов"""
        if df is None or len(df) == 0:
            self.log_message("Невозможно сохранить: датасет пуст")
            return
        
        # Сохраняем основной датасет
        main_path = self.output_dir / "eve_consolidated_data_final.csv"
        df.to_csv(main_path, index=False)
        self.log_message(f"\nОсновной датасет сохранён: {main_path}")
        
        # Сохраняем подробную статистику
        self.save_detailed_statistics(df)
        
        return main_path
    
    def save_detailed_statistics(self, df):
        """Сохранение подробной статистики"""
        stats_path = self.output_dir / "dataset_statistics_final.txt"
        
        with open(stats_path, 'w', encoding='utf-8') as f:
            f.write("СТАТИСТИКА ФИНАЛЬНОГО ДАТАСЕТА\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Общая информация:\n")
            f.write(f"  Месяцев: {len(df)}\n")
            f.write(f"  Период: {df['history_date'].min().date()} - {df['history_date'].max().date()}\n\n")
            
            f.write("Покрытие данных по показателям:\n")
            for col in sorted(df.columns):
                if col != 'history_date' and col != 'is_war_period':
                    non_null = df[col].notna().sum()
                    percentage = non_null / len(df) * 100
                    
                    if non_null > 0:
                        if col in ['production_isk', 'destruction_isk', 'mining_isk', 'trade_value', 'total_isk_destroyed']:
                            avg = df[col].mean() / 1e12  # в трлн
                            f.write(f"  {col}: {non_null} месяцев ({percentage:.1f}%), среднее {avg:.1f} трлн\n")
                        elif col == 'isk_velocity':
                            avg = df[col].mean()
                            f.write(f"  {col}: {non_null} месяцев ({percentage:.1f}%), среднее {avg:.4f}\n")
                        else:
                            f.write(f"  {col}: {non_null} месяцев ({percentage:.1f}%)\n")
                    else:
                        f.write(f"  {col}: НЕТ ДАННЫХ\n")
            
            f.write("\n" + "=" * 50 + "\n")
            f.write("ДАННЫЕ ПО ГОДАМ:\n\n")
            
            df['year'] = df['history_date'].dt.year
            for year in sorted(df['year'].unique()):
                year_data = df[df['year'] == year]
                f.write(f"{year} год ({len(year_data)} месяцев):\n")
                
                # Производство
                if 'production_isk' in year_data.columns and year_data['production_isk'].notna().any():
                    prod_months = year_data['production_isk'].notna().sum()
                    prod_avg = year_data['production_isk'].mean() / 1e12
                    f.write(f"  Производство: {prod_months} месяцев, среднее {prod_avg:.1f} трлн\n")
                
                # Торговля
                if 'trade_value' in year_data.columns and year_data['trade_value'].notna().any():
                    trade_months = year_data['trade_value'].notna().sum()
                    trade_avg = year_data['trade_value'].mean() / 1e12
                    f.write(f"  Торговля: {trade_months} месяцев, среднее {trade_avg:.1f} трлн\n")
                
                f.write("\n")
        
        self.log_message(f"Статистика сохранена: {stats_path}")
    
    def run_full_consolidation(self):
        """Запуск полной консолидации"""
        print("=" * 70)
        print("ФИНАЛЬНАЯ КОНСОЛИДАЦИЯ ДАННЫХ EVE ONLINE")
        print("=" * 70)
        
        # Консолидируем данные
        df = self.consolidate_all_months_fixed()
        
        if df is not None and len(df) > 0:
            # Анализируем качество
            self.analyze_data_quality(df)
            
            # Добавляем индикатор войн
            df = self.add_war_indicator(df, percentile=75)
            
            # Сохраняем результаты
            output_path = self.save_results_fixed(df)
            
            print("\n" + "=" * 70)
            print("РЕЗУЛЬТАТЫ КОНСОЛИДАЦИИ:")
            print("=" * 70)
            
            print(f"\n✅ Успешно обработано месяцев: {len(df)}")
            print(f"📅 Период: {df['history_date'].min().date()} - {df['history_date'].max().date()}")
            
            # Статистика по показателям
            print(f"\n📊 ПОКРЫТИЕ ДАННЫХ:")
            key_metrics = ['production_isk', 'destruction_isk', 'mining_isk', 'trade_value', 'total_isk_destroyed']
            for metric in key_metrics:
                if metric in df.columns:
                    non_null = df[metric].notna().sum()
                    percentage = non_null / len(df) * 100
                    if non_null > 0:
                        avg = df[metric].mean() / 1e12
                        print(f"  {metric}: {non_null} месяцев ({percentage:.1f}%), среднее {avg:.1f} трлн")
            
            if 'is_war_period' in df.columns:
                war_count = df['is_war_period'].sum()
                print(f"\n⚔️ Военных месяцев: {war_count} ({war_count/len(df)*100:.1f}%)")
            
            print(f"\n💾 Основной датасет: {output_path}")
            print(f"📝 Лог консолидации: {self.log_file}")
            print(f"📊 Статистика: {self.output_dir / 'dataset_statistics_final.txt'}")
            
            return df
        else:
            print("❌ Не удалось получить данные. Проверьте лог-файл.")
            return None
    
    def analyze_data_quality(self, df):
        """Анализ качества данных"""
        self.log_message("\n" + "="*60)
        self.log_message("АНАЛИЗ КАЧЕСТВА ДАННЫХ")
        self.log_message("="*60)
        
        # Проверяем наличие ключевых показателей
        key_columns = ['production_isk', 'destruction_isk', 'mining_isk', 'trade_value', 'total_isk_destroyed']
        
        self.log_message("\nПроверка ключевых показателей:")
        for col in key_columns:
            if col in df.columns:
                non_null = df[col].notna().sum()
                unique = df[col].nunique()
                
                if non_null > 0:
                    avg = df[col].mean() / 1e12
                    self.log_message(f"  {col}: {non_null} месяцев, среднее {avg:.1f} трлн, {unique} уникальных")
                else:
                    self.log_message(f"  {col}: НЕТ ДАННЫХ")
            else:
                self.log_message(f"  {col}: ОТСУТСТВУЕТ В ДАТАСЕТЕ")

def main():
    """Основная функция"""
    consolidator = EveDataConsolidatorFinal()
    consolidator.run_full_consolidation()

if __name__ == "__main__":
    main()