"""
Pipeline: Transform planilha_leonardo.xlsx → planilha_final.xlsx

Normalizes item hierarchy to always have 4 levels (XXX.XXX.XXX.XXX).
Data items must be at level 4; missing intermediate levels are auto-created.
"""

import sys
import re
from dataclasses import dataclass, field
from pathlib import Path

import openpyxl


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class InputItem:
    """A parsed row from the input spreadsheet."""
    raw_item: str          # e.g. "01.02.01" or "4"
    description: str
    code: str | None       # CÓDIGO  — None for header rows
    unit: str | None       # UNID.
    price: float | None    # PREÇO UNITÁRIO
    quantity: float | None # QUANTIDADE (col S)
    is_data: bool = False  # True when item has code + unit (leaf item)

    @property
    def parts(self) -> list[str]:
        """Split item into its dotted parts, zero-padded to 3 digits."""
        raw = str(self.raw_item)
        # Level-1 items may be plain integers (e.g. 1, 2, 3)
        segments = raw.split(".")
        return [seg.zfill(3) for seg in segments]

    @property
    def level(self) -> int:
        return len(self.parts)

    @property
    def padded_item(self) -> str:
        return ".".join(self.parts)


@dataclass
class OutputRow:
    """A row to write in the output spreadsheet."""
    item: str               # e.g. "001.002.001.001"
    code: str | None = None
    description: str = ""
    unit: str | None = None
    quantity: float | None = None
    price: float | None = None


# ---------------------------------------------------------------------------
# Unit normalisation
# ---------------------------------------------------------------------------

UNIT_MAP = {
    "M2":  "m2",
    "M3":  "m3",
    "M":   "m",
    "KG":  "kg",
    "UND": "un",
    "VB":  "vb",
    "MÊS": "mes",
    "MES": "mes",
}


def normalise_unit(raw: str | None) -> str | None:
    if raw is None:
        return None
    raw = str(raw)
    key = raw.strip().upper()
    return UNIT_MAP.get(key, raw.strip().lower())



# ---------------------------------------------------------------------------
# Read input
# ---------------------------------------------------------------------------


@dataclass
class ColumnMapping:
    # Default indices (0-based): A=0, B=1, ...
    item_col: int = 1   # Col B
    desc_col: int = 2   # Col C
    code_col: int = 3   # Col D
    unit_col: int = 4   # Col E
    price_col: int = 5  # Col F
    qty_col: int = 18   # Col S
    start_row: int = 7

def read_input(file_obj, mapping: ColumnMapping = ColumnMapping(), sheet_name: str | None = None) -> list[InputItem]:
    """Read the input spreadsheet and return a list of InputItems."""
    wb = openpyxl.load_workbook(file_obj, data_only=True)
    
    if sheet_name:
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet_name}' not found in workbook.")
        ws = wb[sheet_name]
    else:
        ws = wb[wb.sheetnames[0]]

    # Indices are now directly provided in mapping (0-based)
    item_idx = mapping.item_col
    desc_idx = mapping.desc_col
    code_idx = mapping.code_col
    unit_idx = mapping.unit_col
    price_idx = mapping.price_col
    qty_idx = mapping.qty_col

    items: list[InputItem] = []
    
    # We iterate over all rows, but start at mapping.start_row
    for row in ws.iter_rows(min_row=mapping.start_row, max_row=ws.max_row, values_only=False):
        # row is a tuple of Cell objects
        # We need to ensure the row has enough columns for our indices
        max_idx = max(item_idx, desc_idx, code_idx, unit_idx, price_idx, qty_idx)
        if len(row) <= max_idx:
            continue

        item_cell = row[item_idx]
        desc_cell = row[desc_idx]
        code_cell = row[code_idx]
        unit_cell = row[unit_idx]
        price_cell = row[price_idx]
        qty_cell = row[qty_idx]

        item_val = item_cell.value
        if item_val is None:
            continue

        item_str = str(item_val).strip()
        if not item_str:
            continue

        desc = str(desc_cell.value or "").strip()
        code = str(code_cell.value).strip() if code_cell.value is not None else None
        unit = str(unit_cell.value).strip() if unit_cell.value is not None else None
        price = price_cell.value
        qty = qty_cell.value

        # Determine if this is a data item (has code AND non-empty unit)
        has_code = code is not None and code != ""
        has_unit = unit is not None and unit != ""
        is_data = has_code and has_unit
            
        # Clean price and qty
        try:
             price_val = float(price) if is_data and price else None
        except (ValueError, TypeError):
             price_val = None
             
        try:
             qty_val = round(float(qty), 2) if is_data and qty else None
        except (ValueError, TypeError):
             qty_val = None

        items.append(InputItem(
            raw_item=item_str,
            description=desc,
            code=code if is_data else None,
            unit=unit if is_data else None,
            price=price_val,
            quantity=qty_val,
            is_data=is_data,
        ))

    wb.close()
    return items


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(items: list[InputItem]) -> list[OutputRow]:
    """
    Transform items so every data item is at level 4.
    
    Rules:
    - Task identification: item.is_data (has code + unit).
    - Sequential numbering: Tasks are renumbered 001, 002... under their L3 container.
    - Deepening:
        - L2 Task (Parent L1): -> Syn L2 (Parent Desc) -> Syn L3 (Parent Desc) -> L4 Task
        - L3 Task (Parent L2): -> Syn L3 (Parent Desc) -> L4 Task
        - L4 Task: Keep
        - L5+ Task: Flatten to L4 (merge suffix)
    """
    output: list[OutputRow] = []

    # Map to store descriptions of potential parents (Groups)
    desc_map: dict[str, str] = {}
    
    # Track emitted synthetic headers
    emitted_headers: set[str] = set()
    
    # Counter for sequential numbering at Level 4
    # Key = L3 Parent Item (e.g. "001.001.001"), Value = Next Index
    l3_counters: dict[str, int] = {}

    # Pass 1: Collect descriptions from Groups
    for item in items:
        if not item.is_data:
            desc_map[item.padded_item] = item.description

    # Pass 2: Generate output
    for item in items:
        padded = item.padded_item
        parts = item.parts
        level = item.level
        
        if not item.is_data:
            # GROUPS: Output as is
            output.append(OutputRow(
                item=padded, 
                description=item.description
            ))
            emitted_headers.add(padded)
        
        else:
            # TASKS
            # Determine L3 Container and Parent Description
            l3_container = ""
            parent_desc = ""
            
            if level == 2:
                # Parent is L1 (first part)
                parent_l1 = parts[0]
                parent_desc = desc_map.get(parent_l1, "")
                
                # Synthetic L2: 001.001
                syn_l2 = f"{parent_l1}.001"
                if syn_l2 not in emitted_headers:
                    output.append(OutputRow(item=syn_l2, description=parent_desc))
                    emitted_headers.add(syn_l2)
                    
                # Synthetic L3: 001.001.001
                syn_l3 = f"{syn_l2}.001"
                if syn_l3 not in emitted_headers:
                    # Note: L3 syn header also takes L1 parent desc in this case
                    output.append(OutputRow(item=syn_l3, description=parent_desc))
                    emitted_headers.add(syn_l3)
                    
                l3_container = syn_l3

            elif level == 3:
                # Parent is L2 (first 2 parts)
                parent_l2 = ".".join(parts[:2])
                parent_desc = desc_map.get(parent_l2, "")
                
                # Synthetic L3: 001.002.001 (e.g.)
                syn_l3 = f"{parent_l2}.001"
                if syn_l3 not in emitted_headers:
                    output.append(OutputRow(item=syn_l3, description=parent_desc))
                    emitted_headers.add(syn_l3)
                    
                l3_container = syn_l3
                
            elif level == 4:
                # Parent is L3 (first 3 parts)
                l3_container = ".".join(parts[:3])
                # Ensure L3 container header exists? 
                # Usually yes if it was in the input as a Group, but if this L4 task
                # appeared alone, we might need to synthetically create the L3 header?
                # Assuming input structure is valid and L3 group was seen.
                
            elif level >= 5:
                # Deep items (Flatten)
                # Parent is L3
                base_l3 = ".".join(parts[:3])
                
                # Merge suffix
                # 12.04.01.02.01 -> rest = 0201
                rest = "".join(parts[3:])
                l4_item = f"{base_l3}.{rest}"
                
                output.append(OutputRow(
                    item=l4_item,
                    code=item.code,
                    description=item.description,
                    unit=normalise_unit(item.unit),
                    quantity=item.quantity,
                    price=item.price,
                ))
                continue # Skip sequential renumbering for flattened items (keep specific ID)

            # Generate L4 Item (Renumbering for L2-L4 items)
            if l3_container:
                idx = l3_counters.get(l3_container, 1)
                l3_counters[l3_container] = idx + 1
                
                l4_item = f"{l3_container}.{idx:03d}"
                
                output.append(OutputRow(
                    item=l4_item,
                    code=item.code,
                    description=item.description,
                    unit=normalise_unit(item.unit),
                    quantity=item.quantity,
                    price=item.price,
                ))

    return output


# ---------------------------------------------------------------------------
# Write output
# ---------------------------------------------------------------------------

HEADER_ROW = [
    "ITEM",
    "CÓDIGO AUXILIAR",
    "DESCRIÇÃO DO SERVIÇO",
    "UNID.",
    "QUANTIDADE",
    "PREÇO UNITARIO",
]


def write_output(rows: list[OutputRow], file_obj_or_path) -> None:
    """Write the output spreadsheet to a file object or path."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Planilha"

    # Header
    ws.append(HEADER_ROW)

    for row in rows:
        ws.append([
            row.item,
            row.code,
            row.description,
            row.unit,
            row.quantity,
            row.price,
        ])

    wb.save(file_obj_or_path)
    wb.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 3:
        print("Usage: python pipeline.py <input.xlsx> <output.xlsx>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    print(f"Reading: {input_path}")
    items = read_input(input_path)
    print(f"  {len(items)} items parsed")

    data_items = [it for it in items if it.is_data]
    header_items = [it for it in items if not it.is_data]
    print(f"  {len(data_items)} data items, {len(header_items)} headers")

    print("Transforming...")
    output_rows = transform(items)
    print(f"  {len(output_rows)} output rows")

    data_output = [r for r in output_rows if r.code is not None]
    print(f"  {len(data_output)} data rows in output")

    print(f"Writing: {output_path}")
    write_output(output_rows, output_path)
    print("Done!")


if __name__ == "__main__":
    main()
