import csv
import json
import sys

def main(csv_path):
    COLUMNS = [
        "query_id", "subquery_type", "subquery_id", "rel_id", "path_id",
        "path_type", "child_paths", "startup_cost", "total_cost", "rows",
        "is_del", "rel_name", "indexoid", "level"
    ]

    paths = []
    with open(csv_path, newline='') as f:
        reader = csv.reader(f, delimiter='\t')
        for row in reader:
            record = dict(zip(COLUMNS, row))

            def clean(val):
                return None if val == '\\N' else val

            try:
                # Преобразуем level: если \N — используем 0 по умолчанию
                level_val = record["level"]
                level = int(level_val) if level_val != '\\N' else 0

                def parse_field(val, target_type, null_repr='\\N'):
                    if val == null_repr:
                        return None
                    return target_type(val)

                path_entry = {
                    "query_id": parse_field(record["query_id"], int),
                    "subquery_id": parse_field(record["subquery_id"], int) or 1,
                    "level": level,
                    "rel_id": parse_field(record["rel_id"], int),
                    "path_id": parse_field(record["path_id"], int),
                    "path_type": record["path_type"],
                    "child_paths": [] if record["child_paths"] == '\\N' else [
                        int(x.strip()) for x in record["child_paths"].strip("{}").split(",") if x.strip()
                    ],
                    "rel_name": clean(record["rel_name"]) or f"rel_{record['rel_id']}",
                    "startup_cost": parse_field(record["startup_cost"], float),
                    "total_cost": parse_field(record["total_cost"], float),
                    "rows": parse_field(record["rows"], float),
                    "is_del": parse_field(record["is_del"], lambda x: x == 't'),
                    "indexoid": parse_field(record["indexoid"], int),
                }
                paths.append(path_entry)
            except Exception as e:
                print(f"Пропущена строка: {row} ({e})", file=sys.stderr)
                continue

    # Группировка: queries -> subqueries -> levels -> relations
    data = {"queries": {}}
    for p in paths:
        qid = p["query_id"]
        sid = p["subquery_id"]
        lvl = p["level"]
        rid = p["rel_id"]

        queries = data["queries"]
        if qid not in queries:
            queries[qid] = {"subqueries": {}}
        subqueries = queries[qid]["subqueries"]
        if sid not in subqueries:
            subqueries[sid] = {"levels": {}}
        levels = subqueries[sid]["levels"]
        if lvl not in levels:
            levels[lvl] = {"relations": {}}
        relations = levels[lvl]["relations"]
        if rid not in relations:
            relations[rid] = {
                "name": p["rel_name"],
                "paths": []
            }
        relations[rid]["paths"].append(p)

    # Вывод JSON
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python3 csv_to_json.py <file.csv>", file=sys.stderr)
        sys.exit(1)
    main(sys.argv[1])
