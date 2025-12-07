import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Настройки для визуализации
plt.style.use('seaborn-v0_8-darkgrid')
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 12
sns.set_palette("husl")

class EveExploratoryAnalysis:
    """
    Класс для проведения разведочного анализа данных EVE Online
    """
    
    def __init__(self, data_path):
        """
        Инициализация анализатора
        
        Parameters:
        -----------
        data_path : str или Path
            Путь к файлу с консолидированными данными
        """
        self.data_path = Path(data_path)
        self.df = None
        self.results = {}
        
    def load_and_prepare_data(self):
        """
        Загрузка и подготовка данных для анализа
        """
        print("Загрузка данных для разведочного анализа...")
        
        # Загрузка данных
        self.df = pd.read_csv(self.data_path)
        self.df['history_date'] = pd.to_datetime(self.df['history_date'])
        
        # Сортировка по дате
        self.df = self.df.sort_values('history_date')
        
        # Проверка наличия необходимых столбцов
        required_columns = ['history_date', 'total_isk_destroyed', 'production_isk', 
                          'trade_value', 'isk_velocity', 'mining_isk', 'is_war_period']
        
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            print(f"Внимание: отсутствуют столбцы: {missing_columns}")
        
        print(f"Данные загружены. Период: {self.df['history_date'].min().date()} - "
              f"{self.df['history_date'].max().date()}")
        print(f"Всего наблюдений: {len(self.df)}")
        
        return self.df
    
    def calculate_basic_statistics(self):
        """
        Расчет базовых описательных статистик
        """
        print("\n" + "="*60)
        print("БАЗОВЫЕ СТАТИСТИЧЕСКИЕ ХАРАКТЕРИСТИКИ ПОКАЗАТЕЛЕЙ")
        print("="*60)
        
        # Выбор числовых колонок для анализа
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        exclude_cols = ['is_war_period', 'year'] if 'year' in self.df.columns else ['is_war_period']
        analysis_cols = [col for col in numeric_cols if col not in exclude_cols]
        
        stats_data = []
        
        for col in analysis_cols:
            stats = {
                'Показатель': col,
                'Среднее': f"{self.df[col].mean():,.2f}",
                'Медиана': f"{self.df[col].median():,.2f}",
                'Ст. отклонение': f"{self.df[col].std():,.2f}",
                'Минимум': f"{self.df[col].min():,.2f}",
                'Максимум': f"{self.df[col].max():,.2f}",
                'Уникальных': self.df[col].nunique()
            }
            stats_data.append(stats)
        
        stats_df = pd.DataFrame(stats_data)
        print(stats_df.to_string(index=False))
        
        self.results['basic_statistics'] = stats_df
        return stats_df
    
    def plot_time_series_with_war_periods(self, save_path=None):
        """
        Построение временных рядов ключевых показателей с выделением военных периодов
        
        Parameters:
        -----------
        save_path : str или Path, optional
            Путь для сохранения графиков
        """
        print("\nПостроение временных рядов ключевых показателей...")
        
        # Ключевые показатели для визуализации
        key_indicators = [
            ('total_isk_destroyed', 'Боевые потери', 'ISK (триллионы)'),
            ('production_isk', 'Производство', 'ISK (триллионы)'),
            ('trade_value', 'Объем торговли', 'ISK (триллионы)'),
            ('isk_velocity', 'Скорость обращения денег', 'Коэффициент'),
            ('mining_isk', 'Добыча ресурсов', 'ISK (триллионы)')
        ]
        
        # Создание отдельных окон для каждого графика
        for col_name, title, ylabel in key_indicators:
            if col_name not in self.df.columns:
                print(f"Показатель {col_name} отсутствует в данных")
                continue
            
            fig, ax = plt.subplots(figsize=(14, 6))
            
            # Построение графика
            ax.plot(self.df['history_date'], self.df[col_name], 
                   linewidth=2, color='steelblue', alpha=0.8, label=title)
            
            # Выделение военных периодов
            war_periods = self.df[self.df['is_war_period'] == 1]
            for _, row in war_periods.iterrows():
                ax.axvspan(row['history_date'], 
                          row['history_date'] + pd.DateOffset(months=1),
                          alpha=0.3, color='red', zorder=0)
            
            # Настройки графика
            ax.set_title(f'{title} ({col_name})', fontsize=14, fontweight='bold', pad=12)
            ax.set_xlabel('Дата', fontsize=12)
            ax.set_ylabel(ylabel, fontsize=12)
            ax.grid(True, alpha=0.3)
            ax.tick_params(axis='x', rotation=45)
            
            # Форматирование осей для больших чисел
            if 'ISK' in ylabel:
                ax.ticklabel_format(axis='y', style='sci', scilimits=(12, 12))
            
            # Добавление легенды
            from matplotlib.patches import Patch
            war_patch = Patch(facecolor='red', alpha=0.3, label='Военный период')
            ax.legend(handles=[war_patch], loc='upper left', fontsize=10)
            
            plt.tight_layout()
            
            if save_path:
                output_path = Path(save_path) / f'time_series_{col_name}.png'
                plt.savefig(output_path, dpi=300, bbox_inches='tight')
                print(f"График {title} сохранен в: {output_path}")
            
            plt.show()
        
        self.results['time_series_plots'] = key_indicators
        return None
    
    def plot_comparison_boxplots(self, save_path=None):
        """
        Построение боксплотов для сравнения показателей в военные и мирные периоды
        
        Parameters:
        -----------
        save_path : str или Path, optional
            Путь для сохранения графиков
        """
        print("\nСравнение распределений показателей в военные и мирные периоды...")
        
        # Показатели для сравнения
        comparison_metrics = [
            ('production_isk', 'Производство', 'Триллионы ISK'),
            ('trade_value', 'Объем торговли', 'Триллионы ISK'),
            ('isk_velocity', 'Скорость обращения денег', 'Коэффициент'),
            ('mining_isk', 'Добыча ресурсов', 'Триллионы ISK')
        ]
        
        # Создание отдельных окон для каждого графика
        for col_name, title, ylabel in comparison_metrics:
            if col_name not in self.df.columns:
                print(f"Показатель {col_name} отсутствует в данных")
                continue
            
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Разделение данных на войну и мир с удалением NaN
            war_data_raw = self.df[self.df['is_war_period'] == 1][col_name]
            peace_data_raw = self.df[self.df['is_war_period'] == 0][col_name]
            
            # Удаляем NaN значения
            war_data = war_data_raw.dropna()
            peace_data = peace_data_raw.dropna()
            
            # Построение боксплотов
            box_data = [peace_data, war_data]
            box_labels = ['Мирные периоды', 'Военные периоды']
            
            bp = ax.boxplot(box_data, labels=box_labels, patch_artist=True)
            
            # Настройка цветов
            colors = ['lightblue', 'lightcoral']
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            # Настройка графика
            ax.set_title(f'Сравнение распределения: {title}', fontsize=14, fontweight='bold', pad=12)
            ax.set_ylabel(ylabel, fontsize=12)
            ax.set_xlabel('Период', fontsize=12)
            ax.grid(True, alpha=0.3)
            
            # Добавление статистики на график
            war_mean = war_data.mean() if len(war_data) > 0 else 0
            peace_mean = peace_data.mean() if len(peace_data) > 0 else 0
            
            # Форматирование текста статистики
            if col_name == 'isk_velocity':
                stat_text = f'Среднее значение:\nМир: {peace_mean:.4f}\nВойна: {war_mean:.4f}'
                diff_text = f'Разница: {((war_mean - peace_mean) / peace_mean * 100):+.1f}%' if peace_mean != 0 else ''
            else:
                stat_text = f'Среднее значение:\nМир: {peace_mean/1e12:.2f} трлн\nВойна: {war_mean/1e12:.2f} трлн'
                diff_text = f'Разница: {((war_mean - peace_mean) / peace_mean * 100):+.1f}%' if peace_mean != 0 else ''
            
            # Вывод статистики в консоль
            print(f"\n{title}:")
            if col_name == 'isk_velocity':
                print(f"  Мирные периоды: {len(peace_data)} записей, Среднее: {peace_mean:.4f}")
                print(f"  Военные периоды: {len(war_data)} записей, Среднее: {war_mean:.4f}")
            else:
                print(f"  Мирные периоды: {len(peace_data)} записей, Среднее: {peace_mean/1e12:.2f} трлн")
                print(f"  Военные периоды: {len(war_data)} записей, Среднее: {war_mean/1e12:.2f} трлн")
            
            ax.text(0.02, 0.98, stat_text, transform=ax.transAxes,
                   fontsize=10, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            if diff_text:
                ax.text(0.02, 0.85, diff_text, transform=ax.transAxes,
                       fontsize=9, verticalalignment='top',
                       bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))
            
            plt.tight_layout()
            
            if save_path:
                output_path = Path(save_path) / f'boxplot_{col_name}.png'
                plt.savefig(output_path, dpi=300, bbox_inches='tight')
                print(f"Боксплот {title} сохранен в: {output_path}")
            
            plt.show()
        
        self.results['boxplot_comparison'] = comparison_metrics
        return None
    
    def plot_correlation_matrix(self, save_path=None):
        """
        Построение тепловой карты корреляций между показателями
        
        Parameters:
        -----------
        save_path : str или Path, optional
            Путь для сохранения графиков
        """
        print("\nАнализ корреляционных взаимосвязей между показателями...")
        
        # Выбор показателей для корреляционного анализа
        correlation_cols = [
            'total_isk_destroyed', 'production_isk', 'trade_value',
            'isk_velocity', 'mining_isk'
        ]
        
        # Фильтрация доступных столбцов
        available_cols = [col for col in correlation_cols if col in self.df.columns]
        
        if len(available_cols) < 2:
            print("Недостаточно данных для корреляционного анализа")
            return None
        
        # Расчет корреляционной матрицы
        corr_matrix = self.df[available_cols].corr(method='spearman')
        
        # Перевод названий на русский для графика
        russian_names = {
            'total_isk_destroyed': 'Боевые потери',
            'production_isk': 'Производство',
            'trade_value': 'Объем торговли',
            'isk_velocity': 'Скорость обращения',
            'mining_isk': 'Добыча ресурсов'
        }
        
        display_names = [russian_names.get(col, col) for col in available_cols]
        
        # Создание тепловой карты
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Маска для верхнего треугольника
        mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
        
        # Построение тепловой карты
        sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.3f', cmap='RdBu_r',
                   center=0, square=True, linewidths=1, cbar_kws={"shrink": 0.8},
                   xticklabels=display_names, yticklabels=display_names, ax=ax,
                   annot_kws={"size": 11, "weight": "bold"})
        
        ax.set_title('Матрица корреляций Спирмена между ключевыми показателями', 
                    fontsize=16, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
        if save_path:
            output_path = Path(save_path) / 'correlation_matrix.png'
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            print(f"Корреляционная матрица сохранена в: {output_path}")
        
        plt.show()
        
        # Детальный вывод корреляций
        print("\n" + "="*60)
        print("ДЕТАЛЬНЫЙ АНАЛИЗ КОРРЕЛЯЦИЙ")
        print("="*60)
        
        # Анализ корреляций с боевыми потерями
        if 'total_isk_destroyed' in corr_matrix.columns:
            war_correlations = corr_matrix['total_isk_destroyed'].sort_values(ascending=False)
            print("\nКорреляции с показателем боевых потерь:")
            for idx, (indicator, corr) in enumerate(war_correlations.items(), 1):
                if indicator != 'total_isk_destroyed':
                    indicator_name = russian_names.get(indicator, indicator)
                    strength = "сильная" if abs(corr) > 0.7 else "средняя" if abs(corr) > 0.3 else "слабая"
                    sign = "положительная" if corr > 0 else "отрицательная"
                    print(f"{idx:2}. {indicator_name:25} r = {corr:.3f} ({sign} {strength} связь)")
        
        # Анализ сильных корреляций
        print("\nСильные корреляции (|r| > 0.7):")
        strong_corrs = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr = corr_matrix.iloc[i, j]
                if abs(corr) > 0.7:
                    col1 = russian_names.get(corr_matrix.columns[i], corr_matrix.columns[i])
                    col2 = russian_names.get(corr_matrix.columns[j], corr_matrix.columns[j])
                    strong_corrs.append((col1, col2, corr))
        
        if strong_corrs:
            for idx, (col1, col2, corr) in enumerate(strong_corrs, 1):
                print(f"{idx:2}. {col1:25} ↔ {col2:25} r = {corr:.3f}")
        else:
            print("  Нет сильных корреляций")
        
        self.results['correlation_matrix'] = corr_matrix
        return corr_matrix
    
    def analyze_war_peace_statistics(self):
        """
        Детальный статистический анализ различий между военными и мирными периодами
        """
        print("\n" + "="*60)
        print("СТАТИСТИЧЕСКОЕ СРАВНЕНИЕ ВОЕННЫХ И МИРНЫХ ПЕРИОДОВ")
        print("="*60)
        
        # Разделение данных
        war_df = self.df[self.df['is_war_period'] == 1]
        peace_df = self.df[self.df['is_war_period'] == 0]
        
        print(f"Военные периоды: {len(war_df)} месяцев")
        print(f"Мирные периоды: {len(peace_df)} месяцев")
        print(f"Доля военных периодов: {len(war_df)/len(self.df)*100:.1f}%\n")
        
        # Показатели для сравнения
        comparison_metrics = ['production_isk', 'trade_value', 'isk_velocity', 'mining_isk']
        
        comparison_results = []
        
        for metric in comparison_metrics:
            if metric not in self.df.columns:
                continue
                
            # Статистики по группам
            war_mean = war_df[metric].mean()
            peace_mean = peace_df[metric].mean()
            war_std = war_df[metric].std()
            peace_std = peace_df[metric].std()
            
            # Относительная разница
            relative_diff = ((war_mean - peace_mean) / peace_mean * 100) if peace_mean != 0 else 0
            
            # Форматирование значений
            if metric == 'isk_velocity':
                war_mean_fmt = f"{war_mean:.4f}"
                peace_mean_fmt = f"{peace_mean:.4f}"
                war_std_fmt = f"{war_std:.4f}"
                peace_std_fmt = f"{peace_std:.4f}"
            else:
                war_mean_fmt = f"{war_mean/1e12:.2f} трлн"
                peace_mean_fmt = f"{peace_mean/1e12:.2f} трлн"
                war_std_fmt = f"{war_std/1e12:.2f} трлн"
                peace_std_fmt = f"{peace_std/1e12:.2f} трлн"
            
            # Название показателя на русском
            russian_names = {
                'production_isk': 'Производство',
                'trade_value': 'Объем торговли',
                'isk_velocity': 'Скорость обращения',
                'mining_isk': 'Добыча ресурсов'
            }
            
            result = {
                'Показатель': russian_names.get(metric, metric),
                'Среднее (война)': war_mean_fmt,
                'Среднее (мир)': peace_mean_fmt,
                'Разница, %': f"{relative_diff:+.1f}%",
                'σ (война)': war_std_fmt,
                'σ (мир)': peace_std_fmt
            }
            comparison_results.append(result)
        
        # Вывод результатов
        comparison_df = pd.DataFrame(comparison_results)
        print(comparison_df.to_string(index=False))
        
        # Анализ статистической значимости
        print("\n" + "-"*60)
        print("ИНТЕРПРЕТАЦИЯ РАЗЛИЧИЙ:")
        print("-"*60)
        
        for metric in comparison_metrics:
            if metric not in self.df.columns:
                continue
                
            war_mean = war_df[metric].mean()
            peace_mean = peace_df[metric].mean()
            relative_diff = ((war_mean - peace_mean) / peace_mean * 100) if peace_mean != 0 else 0
            
            russian_name = {
                'production_isk': 'производства',
                'trade_value': 'объема торговли',
                'isk_velocity': 'скорости обращения денег',
                'mining_isk': 'добычи ресурсов'
            }.get(metric, metric)
            
            if abs(relative_diff) > 10:
                direction = "выше" if relative_diff > 0 else "ниже"
                print(f"• В военные периоды {russian_name} в среднем на {abs(relative_diff):.1f}% {direction}, чем в мирные")
        
        self.results['war_peace_comparison'] = comparison_df
        return comparison_df
    
    def generate_summary_report(self, output_dir=None):
        """
        Генерация сводного отчета по разведочному анализу
        
        Parameters:
        -----------
        output_dir : str или Path, optional
            Директория для сохранения отчета
        """
        print("\n" + "="*60)
        print("СВОДНЫЙ ОТЧЕТ ПО РАЗВЕДОЧНОМУ АНАЛИЗУ")
        print("="*60)
        
        report_lines = []
        
        # 1. Общая информация
        report_lines.append("1. ОБЩАЯ ИНФОРМАЦИЯ О ДАННЫХ")
        report_lines.append(f"   Период анализа: {self.df['history_date'].min().date()} - "
                          f"{self.df['history_date'].max().date()}")
        report_lines.append(f"   Всего месяцев: {len(self.df)}")
        war_months = (self.df['is_war_period'] == 1).sum()
        war_percentage = war_months/len(self.df)*100
        report_lines.append(f"   Военные месяцы: {war_months} ({war_percentage:.1f}%)")
        
        # 2. Ключевые наблюдения
        report_lines.append("\n2. КЛЮЧЕВЫЕ НАБЛЮДЕНИЯ")
        
        # Анализ динамики
        if 'total_isk_destroyed' in self.df.columns:
            max_war_month = self.df.loc[self.df['total_isk_destroyed'].idxmax()]
            report_lines.append(f"   Пик боевых потерь: {max_war_month['total_isk_destroyed']/1e12:.2f} трлн ISK "
                              f"({max_war_month['history_date'].strftime('%Y-%m')})")
        
        # 3. Предварительные выводы
        report_lines.append("\n3. ПРЕДВАРИТЕЛЬНЫЕ ВЫВОДЫ")
        
        # Проверка визуальных различий
        if 'production_isk' in self.df.columns:
            war_production = self.df[self.df['is_war_period'] == 1]['production_isk'].mean()
            peace_production = self.df[self.df['is_war_period'] == 0]['production_isk'].mean()
            if war_production > peace_production:
                diff = ((war_production - peace_production) / peace_production * 100)
                report_lines.append(f"   • Производство в военные периоды выше на {diff:.1f}% (требует статистической проверки)")
        
        if 'trade_value' in self.df.columns:
            war_trade = self.df[self.df['is_war_period'] == 1]['trade_value'].mean()
            peace_trade = self.df[self.df['is_war_period'] == 0]['trade_value'].mean()
            if war_trade > peace_trade:
                diff = ((war_trade - peace_trade) / peace_trade * 100)
                report_lines.append(f"   • Объем торговли в военные периоды выше на {diff:.1f}%")
        
        # Сохранение отчета
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(exist_ok=True)
            
            report_path = output_dir / 'eda_summary_report.txt'
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            
            print(f"\nОтчет сохранен в: {report_path}")
        
        # Вывод отчета в консоль
        print('\n'.join(report_lines))
        
        return report_lines

def main():
    """
    Основная функция для запуска разведочного анализа
    """
    print("="*70)
    print("РАЗВЕДОЧНЫЙ АНАЛИЗ ДАННЫХ EVE ONLINE")
    print("="*70)
    
    # Пути к данным
    data_path = Path(r"C:\\Users\\Yapupalo\\Desktop\\Учёба\\Мага\\Курсовая\\v2\\данные\\Подготовленные данные\\eve_fixed_velocity.csv")
    output_dir = Path(r"C:\Users\Yapupalo\Desktop\Учёба\Мага\Курсовая\v2\данные\Результаты анализа")
    
    # Создание директории для результатов
    output_dir.mkdir(exist_ok=True)
    
    # Инициализация анализатора
    analyzer = EveExploratoryAnalysis(data_path)
    
    try:
        # 1. Загрузка данных
        df = analyzer.load_and_prepare_data()
        
        # 2. Базовые статистики
        analyzer.calculate_basic_statistics()
        
        # 3. Временные ряды (отдельные окна для каждого графика)
        analyzer.plot_time_series_with_war_periods(save_path=output_dir)
        
        # 4. Боксплоты сравнения (отдельные окна для каждого графика)
        analyzer.plot_comparison_boxplots(save_path=output_dir)
        
        # 5. Корреляционный анализ (одно окно)
        analyzer.plot_correlation_matrix(save_path=output_dir)
        
        # 6. Детальный статистический анализ
        analyzer.analyze_war_peace_statistics()
        
        # 7. Сводный отчет
        analyzer.generate_summary_report(output_dir=output_dir)
        
        print("\n" + "="*70)
        print("РАЗВЕДОЧНЫЙ АНАЛИЗ УСПЕШНО ЗАВЕРШЕН")
        print("="*70)
        print(f"Все графики и отчеты сохранены в: {output_dir.absolute()}")
        
    except Exception as e:
        print(f"\nОшибка при выполнении анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()