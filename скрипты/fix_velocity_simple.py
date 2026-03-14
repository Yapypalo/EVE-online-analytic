import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

def fix_velocity_and_analyze():
    """Исправление скорости обращения и анализ"""
    
    # Пути
    input_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные\eve_consolidated_data_complete.csv")
    output_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Подготовленные данные\eve_fixed_velocity.csv")
    
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
    
    print("1. Вычисление скорости обращения по формуле:")
    print("   isk_velocity = (trade_value + destruction_isk) / total_isk")
    
    df['isk_velocity_corrected'] = (df['trade_value'] + df['destruction_isk']) / df['total_isk']
    
    # 2. АНАЛИЗ РЕЗУЛЬТАТОВ
    print("2. Анализ результатов:")
    
    # Статистика по годам
    df['year'] = df['history_date'].dt.year
    
    for year in sorted(df['year'].unique()):
        year_data = df[df['year'] == year]
        if year_data['isk_velocity_corrected'].notna().any():
            mean_val = year_data['isk_velocity_corrected'].mean()
            std_val = year_data['isk_velocity_corrected'].std()
            print(f"   {year}: среднее={mean_val:.4f}, ст.откл={std_val:.4f}")
    
    # 3. ГРАФИК СРАВНЕНИЯ
    print("3. Создание графика сравнения...")
    
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
    graph_path = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Результаты анализа\velocity_fixed.png")
    graph_path.parent.mkdir(exist_ok=True)
    plt.savefig(graph_path, dpi=300, bbox_inches='tight')
    print(f"   График сохранён: {graph_path}")
    
    plt.show()
    
    # 4. ЗАМЕНЯЕМ СТАРУЮ СКОРОСТЬ НА ИСПРАВЛЕННУЮ
    df['isk_velocity'] = df['isk_velocity_corrected']
    df = df.drop(columns=['isk_velocity_corrected', 'year'])
    
    # 5. КОРРЕЛЯЦИОННЫЙ АНАЛИЗ
    print("4. Корреляционный анализ (исправленные данные):")
    
    # Корреляция Спирмена
    corr_velocity_losses = df['isk_velocity'].corr(df['total_isk_destroyed'], method='spearman')
    corr_trade_losses = df['trade_value'].corr(df['total_isk_destroyed'], method='spearman')
    
    print(f"   Корреляция скорость ↔ потери: {corr_velocity_losses:.3f}")
    print(f"   Корреляция торговля ↔ потери: {corr_trade_losses:.3f}")
    
    # 6. СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
    df.to_csv(output_path, index=False)
    
    print(f"✅ ИСПРАВЛЕННЫЙ ДАТАСЕТ СОХРАНЁН: {output_path}")
    
    # 7. СВОДКА
    print("" + "="*70)
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
