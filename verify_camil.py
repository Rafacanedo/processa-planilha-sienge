
import sys
import pipeline
from pipeline import ColumnMapping

def main():
    input_path = 'planilha_camil.xlsx'
    output_path = 'planilha_camil_output.xlsx'

    print(f"Reading: {input_path}")
    
    # Correct mapping for planilha_camil.xlsx
    # B=1, D=3 (Desc), E=4 (Code), F=5 (Unit), G=6 (Price), H=7 (Qty)
    mapping = ColumnMapping(
        item_col=1,
        desc_col=3,
        code_col=4,
        unit_col=5,
        price_col=6,
        qty_col=7,
        start_row=7
    )
    
    items = pipeline.read_input(input_path, mapping)
    print(f"  {len(items)} items parsed")

    data_items = [it for it in items if it.is_data]
    header_items = [it for it in items if not it.is_data]
    print(f"  {len(data_items)} data items, {len(header_items)} headers")

    print("Transforming...")
    output_rows = pipeline.transform(items)
    print(f"  {len(output_rows)} output rows")

    data_output = [r for r in output_rows if r.code is not None]
    print(f"  {len(data_output)} data rows in output")
    
    # Verification checks
    print("\n=== Verification ===")
    
    # 1. Check total data items preserved
    if len(data_items) == len(data_output):
        print(f"[PASS] Data item count matches: {len(data_items)}")
    else:
        print(f"[FAIL] Data item count mismatch: In={len(data_items)}, Out={len(data_output)}")
        
    # 2. Check for flattened items (Level 5+)
    # We expect some items to be merged, e.g. 12.04.01.02.01 -> 12.04.01.0201
    flattened_count = 0
    for row in output_rows:
        parts = row.item.split('.')
        # Check if last part has > 3 digits, suggesting a merge
        if len(parts) == 4 and len(parts[3]) > 3:
            flattened_count += 1
            if flattened_count <= 5:
                 print(f"  Found flattened item: {row.item} (Desc: {row.description[:30]}...)")

    print(f"  Total flattened items found: {flattened_count}")
    # We expect 16 items at level 5 in input, so roughly 16 flattened items output
    if flattened_count >= 16:
        print(f"[PASS] Found flattened items (expected ~16, got {flattened_count})")
    else:
         print(f"[WARN] Found fewer flattened items than expected (expected ~16, got {flattened_count})")
         
    # 3. Check Level 2 Task padding
    # e.g. 01.01 -> 001.001.001.001
    print("\n[INFO] Checking specific examples:")
    
    # Check 01.01 expansion
    l2_example = [r for r in output_rows if r.item.startswith("001.001.001")]
    if l2_example:
        print("  Found 01.01 expansion:")
        for r in l2_example[:5]:
            print(f"    {r.item} | {r.description}")
    else:
        print("[FAIL] detailed expansion for 01.01 not found")

    # Check 002 expansion
    l2_002 = [r for r in output_rows if r.item.startswith("002")]
    if l2_002:
        print("  Found 002 expansion:")
        for r in l2_002[:5]:
             print(f"    {r.item} | {r.description}")
    else:
        print("[FAIL] detailed expansion for 002 not found")

    print(f"Writing: {output_path}")
    pipeline.write_output(output_rows, output_path)
    print("Done!")

if __name__ == "__main__":
    main()
