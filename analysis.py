import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os


def load_data(input_dir):
    files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.parquet')]
    df_list = [pd.read_parquet(file) for file in files]
    df = pd.concat(df_list, ignore_index=True)
    return df


def plot_data(df, criterion):
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df[df['criterion'] == criterion], x='timestamp', y='value', hue='variable')
    plt.title(f'Average Time Spent by {criterion.capitalize()}')
    plt.xlabel('Date')
    plt.ylabel('Average Time Spent')
    plt.legend(title=criterion.capitalize())
    plt.show()


def main():
    input_dir = os.path.join(os.path.dirname(__file__), "data_output")
    df = load_data(input_dir)

    plot_data(df, 'âge')
    plot_data(df, 'sexe')
    plot_data(df, 'catégorie')


if __name__ == "__main__":
    main()
