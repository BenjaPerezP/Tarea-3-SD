import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # /opt/batch está en /opt/batch/scripts
STORAGE = Path("/opt/storage.jsonl")
OUT_DIR = ROOT / "batch" / "data"
OUT_DIR.mkdir(parents=True, exist_ok=True)

human_path = OUT_DIR / "human_responses.txt"
llm_path = OUT_DIR / "llm_responses.txt"

def main():
    human_out = human_path.open("w", encoding="utf-8")
    llm_out = llm_path.open("w", encoding="utf-8")

    with STORAGE.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)

            # Tomar respuestas reales según TU almacenamiento
            human = obj.get("reference_answer")
            llm = obj.get("llm_answer")

            if human:
                human = " ".join(human.split())
                human_out.write(human + "\n")

            if llm:
                llm = " ".join(llm.split())
                llm_out.write(llm + "\n")

    human_out.close()
    llm_out.close()

    print("OK: human_responses.txt y llm_responses.txt generados.")

if __name__ == "__main__":
    main()
