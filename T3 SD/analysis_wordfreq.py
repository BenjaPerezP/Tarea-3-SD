import csv
from pathlib import Path
import matplotlib.pyplot as plt

# Rutas por defecto (ajusta si tu estructura es distinta)
ROOT = Path(__file__).resolve().parent
HUMAN_PATH = ROOT / "batch" / "data" / "output" / "human_wordcount" / "part-r-00000"
LLM_PATH   = ROOT / "batch" / "data" / "output" / "llm_wordcount"   / "part-r-00000"

OUT_DIR = ROOT / "batch" / "data" / "analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_wordcounts(path):
    data = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Formato: palabra \t frecuencia
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            word, freq_str = parts
            try:
                freq = int(freq_str)
            except ValueError:
                continue
            data.append((word, freq))
    # Ordenamos de mayor a menor frecuencia
    data.sort(key=lambda x: x[1], reverse=True)
    return data

def save_csv_top50(data, out_path):
    top50 = data[:50]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["word", "frequency"])
        writer.writerows(top50)
    return top50

def plot_top50(top50, title, out_png):
    words = [w for w, _ in top50]
    freqs = [f for _, f in top50]

    plt.figure(figsize=(16, 6))
    plt.bar(range(len(words)), freqs)
    plt.xticks(range(len(words)), words, rotation=90)
    plt.xlabel("Palabras")
    plt.ylabel("Frecuencia")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

def main():
    if not HUMAN_PATH.exists():
        raise SystemExit(f"No se encontró {HUMAN_PATH}")
    if not LLM_PATH.exists():
        raise SystemExit(f"No se encontró {LLM_PATH}")

    human_data = load_wordcounts(HUMAN_PATH)
    llm_data   = load_wordcounts(LLM_PATH)

    print(f"Total palabras distintas (human): {len(human_data)}")
    print(f"Total palabras distintas (llm):   {len(llm_data)}")

    human_csv = OUT_DIR / "human_top50.csv"
    llm_csv   = OUT_DIR / "llm_top50.csv"
    human_png = OUT_DIR / "human_top50.png"
    llm_png   = OUT_DIR / "llm_top50.png"

    human_top50 = save_csv_top50(human_data, human_csv)
    llm_top50   = save_csv_top50(llm_data, llm_csv)

    plot_top50(human_top50, "Top 50 palabras - Yahoo! (humanas)", human_png)
    plot_top50(llm_top50,   "Top 50 palabras - LLM",             llm_png)

    print(f"CSV humanos: {human_csv}")
    print(f"CSV LLM:     {llm_csv}")
    print(f"Gráfico humanos: {human_png}")
    print(f"Gráfico LLM:     {llm_png}")

if __name__ == "__main__":
    main()
