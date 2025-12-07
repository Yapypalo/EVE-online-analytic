import pandas as pd
import numpy as np
from pathlib import Path

def check_money_supply_files():
    """Проверка исходных файлов money_supply.csv за 2022-2025 годы"""
    
    archives_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\архивы")
    
    print("="*70)
    print("АНАЛИЗ ФАЙЛОВ MONEY_SUPPLY.CSV ЗА 2022-2025")
    print("="*70)
    
    results = []
    
    # Проверяем по 2 месяца каждого года
    test_months = [
        ("2022-06", "EVEOnline_MER_Jun2022"),
        ("2022-12", "EVEOnline_MER_Dec2022"),
        ("2023-06", "EVEOnline_MER_Jun2023"),
        ("2023-12", "EVEOnline_MER_Dec2023"),
        ("2024-06", "EVEOnline_MER_Jun2024"),
        ("2024-12", "EVEOnline_MER_Dec2024"),
        ("2025-06", "EVEOnline_MER_Jun2025"),
    ]
    
    for target_month, folder_name in test_months:
        folder_path = archives_path / folder_name
        money_file = folder_path / "money_supply.csv"
        
        if not money_file.exists():
            print(f"{target_month}: Файл не найден")
            continue
        
        try:
            df = pd.read_csv(money_file)
            print(f"\n{target_month} ({folder_name}):")
            print(f"  Столбцы: {list(df.columns)}")
            print(f"  Строк: {len(df)}")
            
            if 'isk_velocity' in df.columns:
                velocity_values = df['isk_velocity'].unique()
                print(f"  Уникальных значений isk_velocity: {len(velocity_values)}")
                
                if len(velocity_values) < 5:  # Если слишком мало уникальных значений
                    print(f"  ⚠️  Подозрительно: {velocity_values[:5]}")
                
                # Проверяем вариативность
                if len(df) > 10:  # Если есть дневные данные
                    print(f"  Среднее: {df['isk_velocity'].mean():.4f}")
                    print(f"  Стандартное отклонение: {df['isk_velocity'].std():.4f}")
                    
                    # Вычисляем вручную по формуле
                    if all(col in df.columns for col in ['trade_value', 'total_isk']):
                        calculated = df['trade_value'] / df['total_isk']
                        print(f"  Вычислено (trade/total): {calculated.mean():.4f}")
            
            # Показываем первые строки
            print(f"  Первые 5 строк:")
            print(df.head().to_string())
            
        except Exception as e:
            print(f"{target_month}: Ошибка - {e}")

def analyze_velocity_consistency():
    """Анализ консистентности скорости обращения в нашем датасете"""
    
    data_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные\eve_consolidated_data_complete.csv")
    
    if not data_path.exists():
        print("Файл не найден!")
        return
    
    df = pd.read_csv(data_path)
    df['history_date'] = pd.to_datetime(df['history_date'])
    df = df.sort_values('history_date')
    
    print("\n" + "="*70)
    print("АНАЛИЗ КОНСИСТЕНТНОСТИ СКОРОСТИ ОБРАЩЕНИЯ В ДАТАСЕТЕ")
    print("="*70)
    
    # Разделяем периоды
    df_pre_2022 = df[df['history_date'] < '2022-01-01']
    df_post_2022 = df[df['history_date'] >= '2022-01-01']
    
    print(f"\nДо 2022 года ({len(df_pre_2022)} месяцев):")
    print(f"  Средняя скорость: {df_pre_2022['isk_velocity'].mean():.4f}")
    print(f"  Стандартное отклонение: {df_pre_2022['isk_velocity'].std():.4f}")
    print(f"  Минимум: {df_pre_2022['isk_velocity'].min():.4f}")
    print(f"  Максимум: {df_pre_2022['isk_velocity'].max():.4f}")
    
    print(f"\nПосле 2022 года ({len(df_post_2022)} месяцев):")
    print(f"  Средняя скорость: {df_post_2022['isk_velocity'].mean():.4f}")
    print(f"  Стандартное отклонение: {df_post_2022['isk_velocity'].std():.4f}")
    print(f"  Минимум: {df_post_2022['isk_velocity'].min():.4f}")
    print(f"  Максимум: {df_post_2022['isk_velocity'].max():.4f}")
    
    # Проверяем уникальные значения
    print(f"\nУникальных значений скорости (до 2022): {df_pre_2022['isk_velocity'].nunique()}")
    print(f"Уникальных значений скорости (после 2022): {df_post_2022['isk_velocity'].nunique()}")
    
    # Вычисляем скорость вручную для сравнения
    df['velocity_calculated'] = df['trade_value'] / df['total_isk']
    
    print(f"\nСравнение с вычисленной скоростью (trade/total):")
    print(f"  Корреляция: {df['isk_velocity'].corr(df['velocity_calculated']):.3f}")
    
    # Разница между значениями
    df['velocity_diff'] = abs(df['isk_velocity'] - df['velocity_calculated'])
    print(f"  Средняя абсолютная разница: {df['velocity_diff'].mean():.4f}")
    
    # Показываем месяцы с наибольшей разницей
    large_diff = df.nlargest(5, 'velocity_diff')[['history_date', 'isk_velocity', 'velocity_calculated', 'velocity_diff']]
    print(f"\nМесяцы с наибольшей разницей:")
    for _, row in large_diff.iterrows():
        print(f"  {row['history_date'].strftime('%Y-%m')}: "
              f"исходное={row['isk_velocity']:.4f}, "
              f"вычисленное={row['velocity_calculated']:.4f}, "
              f"разница={row['velocity_diff']:.4f}")

def recalculate_velocity_uniform():
    """Пересчёт скорости обращения по единой формуле для всего периода"""
    
    data_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные\eve_consolidated_data_complete.csv")
    output_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные\eve_consolidated_data_uniform_velocity.csv")
    
    df = pd.read_csv(data_path)
    df['history_date'] = pd.to_datetime(df['history_date'])
    df = df.sort_values('history_date')
    
    print("\n" + "="*70)
    print("ПЕРЕСЧЁТ СКОРОСТИ ОБРАЩЕНИЯ ПО ЕДИНОЙ ФОРМУЛЕ")
    print("="*70)
    
    # Варианты формул
    formulas = {
        'formula1': lambda row: row['trade_value'] / row['total_isk'] if row['total_isk'] > 0 else np.nan,
        'formula2': lambda row: (row['trade_value'] + row['destruction_isk']) / row['total_isk'] if row['total_isk'] > 0 else np.nan,
        'formula3': lambda row: row['trade_value'] / (row['total_isk'] * 0.85) if row['total_isk'] > 0 else np.nan,  # Коэффициент коррекции
    }
    
    # Тестируем формулы
    for name, formula in formulas.items():
        df[f'velocity_{name}'] = df.apply(formula, axis=1)
        valid = df[f'velocity_{name}'].notna().sum()
        mean_val = df[f'velocity_{name}'].mean()
        std_val = df[f'velocity_{name}'].std()
        
        print(f"\n{name}:")
        print(f"  Доступно месяцев: {valid}")
        print(f"  Среднее: {mean_val:.4f}")
        print(f"  Стандартное отклонение: {std_val:.4f}")
        print(f"  Диапазон: {df[f'velocity_{name}'].min():.4f} - {df[f'velocity_{name}'].max():.4f}")
    
    # Выбираем лучшую формулу (формула 2 - официальная)
    df['isk_velocity_uniform'] = df.apply(formulas['formula2'], axis=1)
    
    print(f"\nСравнение с исходными данными:")
    print(f"  Корреляция: {df['isk_velocity'].corr(df['isk_velocity_uniform']):.3f}")
    
    # Анализ по периодам
    for year in [2020, 2021, 2022, 2023, 2024, 2025]:
        year_data = df[df['history_date'].dt.year == year]
        if not year_data.empty:
            original = year_data['isk_velocity'].mean()
            uniform = year_data['isk_velocity_uniform'].mean()
            print(f"  {year}: исходное={original:.4f}, единое={uniform:.4f}, разница={abs(original-uniform):.4f}")
    
    # Заменяем оригинальную скорость на пересчитанную
    df['isk_velocity'] = df['isk_velocity_uniform']
    
    # Удаляем вспомогательные колонки
    cols_to_drop = [col for col in df.columns if col.startswith('velocity_') or col == 'isk_velocity_uniform']
    df = df.drop(columns=cols_to_drop)
    
    # Сохраняем
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ ДАННЫЕ С ЕДИНОЙ СКОРОСТЬЮ ОБРАЩЕНИЯ СОХРАНЕНЫ: {output_path}")
    
    return df

def create_simple_fix_script():
    """Создание простого скрипта для исправления скорости обращения"""
    
    script_content = '''import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

def fix_velocity_and_analyze():
    """Исправление скорости обращения и анализ"""
    
    # Пути
    input_path = Path(r"C:\\Users\\Yapupalo\\Desktop\\Учёба\\Мага\\Курсовая\\v2\\данные\\Подготовленные данные\\eve_consolidated_data_complete.csv")
    output_path = Path(r"C:\\Users\\Yapupalo\\Desktop\\Учёба\\Мага\\Курсовая\\v2\\данные\\Подготовленные данные\\eve_fixed_velocity.csv")
    
    print("="*70)
    print("ИСПРАВЛЕНИЕ СКОРОСТИ ОБРАЩЕНИЯ ДЕНЕГ")
    print("="*70)
    
    # Загружаем данные
    df = pd.read_csv(input_path)
    df['history_date'] = pd.to_datetime(df['history_date'])
    df = df.sort_values('history_date')
    
    print(f"Загружено месяцев: {len(df)}")
    print(f"Период: {df['history_date'].min().date()} - {df['history_date'].max().date()}")
    
    # 1. ВЫЧИСЛЯЕМ СКОРОСТЬ ОБРАЩЕНИЯ ПО ЕДИНОЙ ФОРМУЛЕ
    # Официальная формула EVE Online: isk_velocity = (trade_value + destruction_isk) / total_isk
    
    print("\n1. Вычисление скорости обращения по формуле:")
    print("   isk_velocity = (trade_value + destruction_isk) / total_isk")
    
    df['isk_velocity_corrected'] = (df['trade_value'] + df['destruction_isk']) / df['total_isk']
    
    # 2. АНАЛИЗ РЕЗУЛЬТАТОВ
    print("\n2. Анализ результатов:")
    
    # Статистика по годам
    df['year'] = df['history_date'].dt.year
    
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        if year_data['isk_velocity_corrected'].notna().any():
            mean_val = year_data['isk_velocity_corrected'].mean()
            std_val = year_data['isk_velocity_corrected'].std()
            print(f"   {year}: среднее={mean_val:.4f}, ст.откл={std_val:.4f}")
    
    # 3. ГРАФИК СРАВНЕНИЯ
    print("\n3. Создание графика сравнения...")
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 8))
    
    # Исходная скорость
    axes[0].plot(df['history_date'], df['isk_velocity'], 'r-', linewidth=2, label='Исходная')
    axes[0].set_title('Исходная скорость обращения', fontsize=14)
    axes[0].set_ylabel('Скорость', fontsize=12)
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()
    
    # Исправленная скорость
    axes[1].plot(df['history_date'], df['isk_velocity_corrected'], 'g-', linewidth=2, label='Исправленная')
    axes[1].set_title('Исправленная скорость обращения', fontsize=14)
    axes[1].set_ylabel('Скорость', fontsize=12)
    axes[1].set_xlabel('Дата', fontsize=12)
    axes[1].grid(True, alpha=0.3)
    axes[1].legend()
    
    plt.tight_layout()
    
    # Сохраняем график
    graph_path = Path(r"C:\\Users\\Yapupalo\\Desktop\\Учёба\\Мага\\Курсовая\\v2\\данные\\Результаты анализа\\velocity_fixed.png")
    graph_path.parent.mkdir(exist_ok=True)
    plt.savefig(graph_path, dpi=300, bbox_inches='tight')
    print(f"   График сохранён: {graph_path}")
    
    plt.show()
    
    # 4. ЗАМЕНЯЕМ СТАРУЮ СКОРОСТЬ НА ИСПРАВЛЕННУЮ
    df['isk_velocity'] = df['isk_velocity_corrected']
    df = df.drop(columns=['isk_velocity_corrected', 'year'])
    
    # 5. КОРРЕЛЯЦИОННЫЙ АНАЛИЗ
    print("\n4. Корреляционный анализ (исправленные данные):")
    
    # Корреляция Спирмена
    corr_velocity_losses = df['isk_velocity'].corr(df['total_isk_destroyed'], method='spearman')
    corr_trade_losses = df['trade_value'].corr(df['total_isk_destroyed'], method='spearman')
    
    print(f"   Корреляция скорость ↔ потери: {corr_velocity_losses:.3f}")
    print(f"   Корреляция торговля ↔ потери: {corr_trade_losses:.3f}")
    
    # 6. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    df.to_csv(output_path, index=False)
    
    print(f"\n✅ ИСПРАВЛЕННЫЙ ДАТАСЕТ СОХРАНЁН: {output_path}")
    
    # 7. СВОДКА
    print("\n" + "="*70)
    print("СВОДКА ИСПРАВЛЕНИЙ:")
    print("="*70)
    
    summary = {
        'Исходное среднее скорости': df['isk_velocity'].mean(),
        'Исходное ст. отклонение': df['isk_velocity'].std(),
        'Исправленное среднее скорости': df['isk_velocity'].mean(),
        'Исправленное ст. отклонение': df['isk_velocity'].std(),
        'Корреляция с потерями': corr_velocity_losses,
        'Месяцев обработано': len(df)
    }
    
    for key, value in summary.items():
        if 'среднее' in key.lower() or 'скорости' in key:
            print(f"   {key}: {value:.4f}")
        elif 'корреляция' in key.lower():
            print(f"   {key}: {value:.3f}")
        else:
            print(f"   {key}: {value}")
    
    return df

if __name__ == "__main__":
    fix_velocity_and_analyze()
'''
    
    # Сохраняем скрипт
    script_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\скрипты\fix_velocity_simple.py")
    with open(script_path, 'w', encoding='utf-8') as f:
        f.write(script_content)
    
    print(f"✅ ПРОСТОЙ СКРИПТ ДЛЯ ИСПРАВЛЕНИЯ СОЗДАН: {script_path}")
    return script_path

def main():
    """Основная функция"""
    print("="*70)
    print("РЕШЕНИЕ ПРОБЛЕМЫ СО СКОРОСТЬЮ ОБРАЩЕНИЯ ДЕНЕГ")
    print("="*70)
    
    print("\n1. Проверяем исходные файлы...")
    check_money_supply_files()
    
    print("\n2. Анализируем текущий датасет...")
    analyze_velocity_consistency()
    
    print("\n3. Создаём простой скрипт для исправления...")
    script_path = create_simple_fix_script()
    
    print("\n" + "="*70)
    print("ИНСТРУКЦИЯ:")
    print("="*70)
    print(f"""
1. Запустите созданный скрипт:
   python "{script_path}"

2. Скрипт сделает следующее:
   - Загрузит текущий датасет
   - Пересчитает скорость обращения по единой формуле
   - Покажет графики сравнения
   - Сохранит новый датасет

3. Формула расчёта:
   isk_velocity = (trade_value + destruction_isk) / total_isk

4. После запуска обновите путь в скрипте разведочного анализа:
   data_path = Path(r"C:\\Users\\Yapupalo\\Desktop\\Учёба\\Мага\\Курсовая\\v2\\данные\\Подготовленные данные\\eve_fixed_velocity.csv")
""")

if __name__ == "__main__":
    main()