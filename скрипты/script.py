import os
import pandas as pd
import glob
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class EveDataConsolidator:
    """Класс для объединения данных EVE Online из ежемесячных отчетов"""
    
    def __init__(self):
        """
        Инициализация консолидатора данных с абсолютными путями
        """
        # Определяем базовую директорию (где находится скрипт)
        self.script_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\скрипты")
        
        # Определяем пути к данным и результатам
        self.archives_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\архивы")
        self.output_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные")
        
        # Создаем выходную директорию, если её нет
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.consolidated_data = []
        
        # Словарь для маппинга возможных названий колонок
        self.column_mappings = {
            'isk_destroyed': ['isk_destroyed', 'iskDestroyed', 'iskdestroyed', 'destroyed_value', 'iskDestroyed'],
            'isk_lost': ['isk_lost', 'iskLost', 'isklost', 'lost_value'],
            'kill_datetime': ['kill_datetime', 'killTime', 'kill_time', 'killDate', 'kill_datetime'],
            'solarsystem_id': ['solarsystem_id', 'solarSystemID', 'system_id']
        }
        
        # Словарь для маппинга возможных названий файлов с УЧЁТОМ РАЗНЫХ РЕГИСТРОВ
        self.file_patterns = {
            'kill_dump': [
                'kill_dump.csv', 'Killdump.csv', 'killdump.csv', 'Kill_Dump.csv', 
                'KillDump.csv', 'KILL_DUMP.csv', 'Killdump.csv', 'killDump.csv'
            ],
            'produced_destroyed_mined': [
                'produced_destroyed_mined.csv', 'ProducedDestroyedMined.csv',
                'Produced_Destroyed_Mined.csv', 'producedDestroyedMined.csv',
                'PRODUCED_DESTROYED_MINED.csv'
            ],
            'regional_stats': [
                'regional_stats.csv', 'RegionalStats.csv',
                'Regional_Stats.csv', 'regionalStats.csv',
                'REGIONAL_STATS.csv'
            ],
            'money_supply': [
                'money_supply.csv', 'MoneySupply.csv',
                'Money_Supply.csv', 'moneySupply.csv',
                'MONEY_SUPPLY.csv'
            ],
            'mining_history': [
                'mining_history_by_security_band.csv', 
                'MiningHistoryBySecurityBand.csv',
                'Mining_History_By_Security_Band.csv',
                'miningHistoryBySecurityBand.csv',
                'MINING_HISTORY_BY_SECURITY_BAND.csv'
            ]
        }
    
    def find_file_case_insensitive(self, folder_path, file_type):
        """
        Находит файл по нескольким возможным именам с учетом регистра
        """
        if file_type not in self.file_patterns:
            return None
        
        # Сначала ищем точное совпадение
        for filename in self.file_patterns[file_type]:
            file_path = folder_path / filename
            if file_path.exists():
                return file_path
        
        # Если не нашли точного совпадения, ищем без учета регистра
        folder_path = Path(folder_path)
        if not folder_path.exists():
            return None
        
        # Собираем все файлы в папке для поиска
        all_files = list(folder_path.glob("*"))
        
        # Создаем список паттернов для поиска
        search_patterns = []
        if file_type == 'kill_dump':
            search_patterns = ['*kill*dump*.csv', '*Kill*Dump*.csv', '*KILL*DUMP*.csv']
        elif file_type == 'produced_destroyed_mined':
            search_patterns = ['*produced*destroyed*mined*.csv', '*Produced*Destroyed*Mined*.csv']
        elif file_type == 'regional_stats':
            search_patterns = ['*regional*stats*.csv', '*Regional*Stats*.csv']
        elif file_type == 'money_supply':
            search_patterns = ['*money*supply*.csv', '*Money*Supply*.csv']
        elif file_type == 'mining_history':
            search_patterns = ['*mining*history*security*.csv', '*Mining*History*Security*.csv']
        
        # Ищем по паттернам
        for pattern in search_patterns:
            found_files = list(folder_path.glob(pattern))
            if found_files:
                # Возвращаем первый найденный файл
                return found_files[0]
        
        # Если всё ещё не нашли, выводим список файлов для отладки
        print(f"  Не найдено файлов типа '{file_type}' в папке {folder_path.name}")
        print(f"  Доступные файлы в папке:")
        for f in all_files[:10]:  # Показываем первые 10 файлов
            if f.suffix.lower() == '.csv':
                print(f"    - {f.name}")
        
        return None
    
    def parse_date_from_folder(self, folder_name):
        """Извлекает дату из имени папки"""
        try:
            # Извлекаем часть после последнего '_'
            parts = folder_name.split('_')
            if len(parts) < 3:
                return None
                
            month_year = parts[-1]
            if len(month_year) < 4:
                return None
                
            month_str = month_year[:3]
            year_str = month_year[3:]
            
            month_map = {
                'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
            }
            
            if month_str in month_map and year_str.isdigit():
                return f"{year_str}-{month_map[month_str]}-01"
            else:
                return None
        except Exception as e:
            print(f"Ошибка при разборе даты из '{folder_name}': {e}")
            return None
    
    def find_column_name_case_insensitive(self, available_columns, possible_names):
        """Находит правильное название колонки среди доступных (без учета регистра)"""
        available_columns_lower = [str(col).lower() for col in available_columns]
        
        for name in possible_names:
            name_lower = name.lower()
            if name_lower in available_columns_lower:
                idx = available_columns_lower.index(name_lower)
                return available_columns[idx]
        
        # Если не нашли, пробуем поискать частичные совпадения
        for col in available_columns:
            col_lower = str(col).lower()
            for name in possible_names:
                if name.lower() in col_lower or col_lower in name.lower():
                    return col
        
        return None
    
    def process_csv_file(self, file_path, required_columns=None):
        """Универсальная функция для обработки CSV файлов"""
        try:
            if not file_path or not file_path.exists():
                return None
            
            # Сначала определяем разделитель и кодировку
            try:
                # Пробуем прочитать с разными разделителями
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    first_line = f.readline().strip()
                
                # Определяем разделитель
                if ';' in first_line:
                    sep = ';'
                else:
                    sep = ','
                
                # Пробуем прочитать файл
                df = pd.read_csv(file_path, sep=sep, low_memory=False, on_bad_lines='skip')
                
            except UnicodeDecodeError:
                # Пробуем другую кодировку
                try:
                    with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                        first_line = f.readline().strip()
                    
                    if ';' in first_line:
                        sep = ';'
                    else:
                        sep = ','
                    
                    df = pd.read_csv(file_path, sep=sep, encoding='latin-1', low_memory=False, on_bad_lines='skip')
                except Exception as e:
                    print(f"  Не удалось прочитать файл {file_path.name}: {e}")
                    return None
            except Exception as e:
                print(f"  Ошибка при чтении файла {file_path.name}: {e}")
                return None
            
            # Если указаны необходимые колонки, проверяем их наличие
            if required_columns:
                available_cols = self.find_available_columns(df, required_columns)
                if available_cols:
                    # Оставляем только доступные колонки
                    df = df[available_cols]
                else:
                    print(f"  Не найдены нужные колонки в {file_path.name}")
                    print(f"  Доступные колонки: {list(df.columns)}")
                    return None
            
            return df
            
        except Exception as e:
            print(f"  Ошибка при обработке файла {file_path.name}: {e}")
            return None
    
    def find_available_columns(self, df, required_columns_dict):
        """Находит доступные колонки из списка требуемых"""
        available_cols = []
        column_mapping = {}
        
        for target_col, possible_names in required_columns_dict.items():
            found_col = self.find_column_name_case_insensitive(list(df.columns), possible_names)
            if found_col:
                available_cols.append(found_col)
                column_mapping[found_col] = target_col
        
        # Переименовываем колонки
        if column_mapping:
            df = df.rename(columns=column_mapping)
        
        return available_cols
    
    def extract_production_data(self, folder_path):
        """Извлекает данные о производстве, уничтожении и добыче"""
        prod_file = self.find_file_case_insensitive(folder_path, 'produced_destroyed_mined')
        
        if not prod_file:
            print(f"  Файл ProducedDestroyedMined.csv не найден в {folder_path.name}")
            return {}
        
        df = self.process_csv_file(prod_file)
        if df is None:
            return {}
        
        # Ищем нужные колонки
        required_cols = {
            'production_isk': ['production_isk', 'productionisk', 'production'],
            'destruction_isk': ['destruction_isk', 'destructionisk', 'destruction'],
            'mining_isk': ['mining_isk', 'miningisk', 'mining']
        }
        
        result = {}
        for target_col, possible_names in required_cols.items():
            found_col = self.find_column_name_case_insensitive(list(df.columns), possible_names)
            if found_col and not df.empty:
                # Берем последнюю строку (часто в файле только одна строка)
                try:
                    result[target_col] = float(df[found_col].iloc[-1])
                except:
                    result[target_col] = 0.0
        
        return result
    
    def extract_trade_data(self, folder_path):
        """Извлекает данные о торговле"""
        reg_file = self.find_file_case_insensitive(folder_path, 'regional_stats')
        
        if not reg_file:
            print(f"  Файл RegionalStats.csv не найден в {folder_path.name}")
            return {}
        
        df = self.process_csv_file(reg_file)
        if df is None:
            return {}
        
        result = {}
        
        # Ищем колонки для суммирования по регионам
        trade_columns = {
            'trade_value': ['trade_value', 'tradevalue', 'trade'],
            'exports': ['exports', 'export'],
            'imports': ['imports', 'import']
        }
        
        for target_col, possible_names in trade_columns.items():
            found_col = self.find_column_name_case_insensitive(list(df.columns), possible_names)
            if found_col:
                try:
                    # Суммируем по всем регионам
                    result[target_col] = float(df[found_col].sum())
                except:
                    result[target_col] = 0.0
        
        return result
    
    def extract_kill_data(self, folder_path, date_str):
        """Извлекает данные о потерях"""
        kill_file = self.find_file_case_insensitive(folder_path, 'kill_dump')
        
        if not kill_file:
            print(f"  Файл Killdump.csv не найден в {folder_path.name}")
            return {'total_isk_destroyed': 0.0}
        
        df = self.process_csv_file(kill_file)
        if df is None:
            return {'total_isk_destroyed': 0.0}
        
        # Ищем колонку с потерями
        isk_col = self.find_column_name_case_insensitive(
            list(df.columns), 
            ['isk_destroyed', 'iskDestroyed', 'destroyed_value']
        )
        
        if not isk_col:
            # Пробуем найти колонку, содержащую 'destroyed' в названии
            for col in df.columns:
                if 'destroyed' in str(col).lower():
                    isk_col = col
                    break
        
        if isk_col:
            try:
                # Преобразуем в числовой тип и суммируем
                df[isk_col] = pd.to_numeric(df[isk_col], errors='coerce')
                total = float(df[isk_col].sum())
                return {'total_isk_destroyed': total}
            except:
                return {'total_isk_destroyed': 0.0}
        
        return {'total_isk_destroyed': 0.0}
    
    def extract_money_data(self, folder_path):
        """Извлекает данные о денежной массе"""
        money_file = self.find_file_case_insensitive(folder_path, 'money_supply')
        
        if not money_file:
            print(f"  Файл MoneySupply.csv не найден в {folder_path.name}")
            return {}
        
        df = self.process_csv_file(money_file)
        if df is None:
            return {}
        
        result = {}
        
        # Ищем колонки
        money_columns = {
            'isk_velocity': ['isk_velocity', 'iskvelocity', 'velocity'],
            'total_isk': ['total_isk', 'totalisk', 'total isk']
        }
        
        for target_col, possible_names in money_columns.items():
            found_col = self.find_column_name_case_insensitive(list(df.columns), possible_names)
            if found_col:
                try:
                    # Берем среднее значение (часто в файле несколько строк)
                    result[target_col] = float(df[found_col].mean())
                except:
                    pass
        
        return result
    
    def extract_mining_data(self, folder_path):
        """Извлекает данные о добыче"""
        mining_file = self.find_file_case_insensitive(folder_path, 'mining_history')
        
        if not mining_file:
            print(f"  Файл MiningHistoryBySecurityBand.csv не найден в {folder_path.name}")
            return {}
        
        df = self.process_csv_file(mining_file)
        if df is None:
            return {}
        
        result = {}
        
        # Ищем колонки объемов добычи
        mining_columns = [
            'asteroid_volume_mined', 'gas_volume_mined', 
            'ice_volume_mined', 'moon_volume_mined'
        ]
        
        total_volume = 0.0
        for col_pattern in mining_columns:
            # Ищем колонку без учета регистра
            found_col = self.find_column_name_case_insensitive(
                list(df.columns), 
                [col_pattern, col_pattern.replace('_', '')]
            )
            if found_col:
                try:
                    total_volume += float(df[found_col].sum())
                except:
                    pass
        
        if total_volume > 0:
            result['total_volume_mined'] = total_volume
        
        return result
    
    def extract_month_data(self, folder_path, date_str):
        """Извлекает данные из папки за конкретный месяц"""
        month_data = {"history_date": date_str}
        
        print(f"  Извлекаю данные из: {folder_path.name}")
        
        try:
            # 1. Производство, уничтожение и добыча
            prod_data = self.extract_production_data(folder_path)
            month_data.update(prod_data)
            
            # 2. Торговые показатели
            trade_data = self.extract_trade_data(folder_path)
            month_data.update(trade_data)
            
            # 3. Боевые потери
            kill_data = self.extract_kill_data(folder_path, date_str)
            month_data.update(kill_data)
            
            # 4. Денежная масса и скорость обращения
            money_data = self.extract_money_data(folder_path)
            month_data.update(money_data)
            
            # 5. Добыча по типам пространства
            mining_data = self.extract_mining_data(folder_path)
            month_data.update(mining_data)
            
            # Логируем, что получили
            print(f"  Получено данных: {len(month_data) - 1} показателей")
            for key in ['production_isk', 'total_isk_destroyed', 'trade_value']:
                if key in month_data:
                    print(f"    {key}: {month_data[key]:,.2f}")
            
            return month_data
            
        except Exception as e:
            print(f"Ошибка при обработке папки {folder_path.name}: {str(e)[:100]}")
            return None
    
    def consolidate_all_data(self, start_date="2019-12-01"):
        """Объединяет все данные начиная с указанной даты"""
        print("Начинаю обработку данных EVE Online...")
        print(f"Ищу папки в: {self.archives_dir.absolute()}")
        
        # Проверяем, существует ли директория с архивами
        if not self.archives_dir.exists():
            print(f"ОШИБКА: Директория {self.archives_dir} не существует!")
            return None
        
        # Получаем список всех папок MER
        mer_folders = sorted([f for f in os.listdir(self.archives_dir) 
                            if f.startswith("EVEOnline_MER_")])
        
        print(f"Найдено папок: {len(mer_folders)}")
        
        for folder_name in mer_folders:
            date_str = self.parse_date_from_folder(folder_name)
            if not date_str:
                print(f"  Пропускаю папку (не удалось извлечь дату): {folder_name}")
                continue
                
            # Проверяем, входит ли дата в нужный диапазон
            if pd.to_datetime(date_str) >= pd.to_datetime(start_date):
                folder_path = self.archives_dir / folder_name
                print(f"\nОбрабатываю: {folder_name} ({date_str})")
                
                month_data = self.extract_month_data(folder_path, date_str)
                if month_data:
                    self.consolidated_data.append(month_data)
                else:
                    print(f"  Предупреждение: Не удалось извлечь данные из {folder_name}")
        
        # Создаем DataFrame
        if self.consolidated_data:
            df = pd.DataFrame(self.consolidated_data)
            df["history_date"] = pd.to_datetime(df["history_date"])
            df = df.sort_values("history_date")
            
            # Заполняем пропущенные значения медианой по колонке
            for col in df.columns:
                if col != 'history_date':
                    if col in df.columns:
                        median_val = df[col].median()
                        df[col] = df[col].fillna(median_val)
            
            # Сбрасываем индекс
            df = df.reset_index(drop=True)
            
            print(f"\n" + "="*50)
            print(f"Обработка завершена!")
            print(f"Успешно обработано месяцев: {len(df)}")
            if len(df) > 0:
                print(f"Диапазон дат: {df['history_date'].min().date()} - {df['history_date'].max().date()}")
            
            return df
        else:
            print("Не удалось извлечь данные.")
            return None
    
    def save_to_csv(self, df, filename="eve_consolidated_data.csv"):
        """Сохраняет объединенные данные в CSV файл"""
        if df is not None:
            output_path = self.output_dir / filename
            df.to_csv(output_path, index=False)
            print(f"\nДанные сохранены в: {output_path.absolute()}")
            return output_path
        return None
    
    def add_war_periods(self, df, war_threshold_percentile=75):
        """Добавляет индикатор военных периодов"""
        if df is not None and "total_isk_destroyed" in df.columns:
            # Рассчитываем порог на основе процентиля
            threshold = df["total_isk_destroyed"].quantile(war_threshold_percentile / 100)
            
            # Создаем бинарный индикатор
            df["is_war_period"] = (df["total_isk_destroyed"] >= threshold).astype(int)
            
            # Рассчитываем статистику
            war_months = df["is_war_period"].sum()
            peace_months = len(df) - war_months
            
            print(f"\nСтатистика военных периодов:")
            print(f"  Порог потерь для войны: {threshold:,.2f} ISK")
            print(f"  Всего месяцев войны: {war_months} ({war_months/len(df)*100:.1f}%)")
            print(f"  Всего месяцев мира: {peace_months} ({peace_months/len(df)*100:.1f}%)")
            
            # Определяем даты войн для отчета
            war_dates = df[df["is_war_period"] == 1]["history_date"]
            if not war_dates.empty:
                print(f"\nМесяцы, классифицированные как военные:")
                for date in war_dates.dt.strftime('%Y-%m'):
                    print(f"  - {date}")
            
            return df
        return None

def main():
    """Основная функция для запуска консолидации данных"""
    print("=" * 70)
    print("КОНСОЛИДАЦИЯ ДАННЫХ EVE ONLINE (версия 4.0 - поддержка CamelCase)")
    print("=" * 70)
    
    # Создаем экземпляр консолидатора
    consolidator = EveDataConsolidator()
    
    # Консолидируем данные с декабря 2019 года
    consolidated_df = consolidator.consolidate_all_data(start_date="2019-12-01")
    
    if consolidated_df is not None and len(consolidated_df) > 0:
        # Добавляем индикатор военных периодов
        consolidated_df = consolidator.add_war_periods(consolidated_df, war_threshold_percentile=75)
        
        # Сохраняем результат
        output_file = consolidator.save_to_csv(consolidated_df, "eve_consolidated_data.csv")
        
        # Показываем краткую статистику
        print("\n" + "=" * 70)
        print("КРАТКАЯ СТАТИСТИКА ПО ДАННЫМ:")
        print("=" * 70)
        
        # Проверяем качество данных
        numeric_cols = consolidated_df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            if col not in ['is_war_period']:
                unique_count = consolidated_df[col].nunique()
                mean_val = consolidated_df[col].mean()
                print(f"\n{col}:")
                print(f"  Уникальных значений: {unique_count}")
                print(f"  Среднее: {mean_val:,.2f}")
                
                # Предупреждение если мало уникальных значений
                if unique_count < 5 and col not in ['is_war_period']:
                    print(f"  ВНИМАНИЕ: Мало уникальных значений! Возможно, данные некорректны.")
        
        print("\n" + "=" * 70)
        print("РЕКОМЕНДАЦИИ:")
        print("=" * 70)
        
        # Проверяем, какие данные выглядят реалистично
        good_columns = []
        suspicious_columns = []
        
        for col in numeric_cols:
            if col not in ['is_war_period']:
                unique_count = consolidated_df[col].nunique()
                if unique_count > 10:  # Более 10 уникальных значений - вероятно, нормальные данные
                    good_columns.append(col)
                else:
                    suspicious_columns.append(col)
        
        print(f"1. Качественные данные ({len(good_columns)}):")
        for col in good_columns:
            print(f"   - {col}")
        
        print(f"\n2. Подозрительные данные ({len(suspicious_columns)}):")
        for col in suspicious_columns:
            print(f"   - {col} (только {consolidated_df[col].nunique()} уникальных значений)")
        
        print(f"\n3. Для анализа рекомендуется использовать данные с 2023 года")
        print(f"   df_clean = df[df['history_date'] >= '2023-01-01']")
        
    else:
        print("Не удалось получить данные. Проверьте пути к архивам.")

if __name__ == "__main__":
    main()