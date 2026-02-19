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
    key = raw.strip().upper()
    return UNIT_MAP.get(key, raw.strip().lower())



# ---------------------------------------------------------------------------
# Read input
# ---------------------------------------------------------------------------

@dataclass
class ColumnMapping:
    item_col: str = "B"
    desc_col: str = "C"
    code_col: str = "D"
    unit_col: str = "E"
    price_col: str = "F"
    qty_col: str = "S"
    start_row: int = 7

def _col_idx(col_letter: str) -> int:
    """Convert column letter to 0-based index."""
    from openpyxl.utils import column_index_from_string
    return column_index_from_string(col_letter) - 1

def read_input(file_obj, mapping: ColumnMapping = ColumnMapping()) -> list[InputItem]:
    """Read the input spreadsheet and return a list of InputItems."""
    wb = openpyxl.load_workbook(file_obj, data_only=True)
    ws = wb[wb.sheetnames[0]]

    # Determine column indices from mapping (0-based for row array access is tricky 
    # because row is a tuple of cells, 0-indexed).
    # row[0] is col A (idx 1). 
    # Wait, iter_rows yields cells. So row[0] is the cell in the first column requested.
    # If we iterate over all columns, row[0] is 'A'.
    
    item_idx = _col_idx(mapping.item_col)
    desc_idx = _col_idx(mapping.desc_col)
    code_idx = _col_idx(mapping.code_col)
    unit_idx = _col_idx(mapping.unit_col)
    price_idx = _col_idx(mapping.price_col)
    qty_idx = _col_idx(mapping.qty_col)

    items: list[InputItem] = []
    
    # We iterate over all rows, so row[i] corresponds to the i-th column in the sheet (0-based)
    # openpyxl iter_rows returns a tuple of cells for the row.
    for row in ws.iter_rows(min_row=mapping.start_row, max_row=ws.max_row, values_only=False):
        # row is a tuple of Cell objects or values if values_only=True
        # Let's ensure we have enough columns
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
        code = code_cell.value
        unit = unit_cell.value
        price = price_cell.value
        qty = qty_cell.value

        # Determine if this is a data item (has code AND non-empty unit)
        has_code = code is not None and str(code).strip() != ""
        has_unit = unit is not None and str(unit).strip() not in ("", None)
        is_data = has_code and has_unit

        if code and isinstance(code, str):
            code = code.strip()
        if unit and isinstance(unit, str):
            unit = unit.strip()
            
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
    - Level-1 and level-2 items are kept as headers.
    - Level-3 items that are headers are kept as-is.
    - Level-3 items that are data items:
        * A level-3 header is auto-created with number {parent}.001 and the
          parent (level-2) description — but only once per level-2 group.
        * The data item is pushed to level 4: {parent}.001.{original_sub}.
    - Level-4 items are kept as-is.
    """
    output: list[OutputRow] = []

    # Track descriptions for parent levels so we can repeat them
    level1_desc: dict[str, str] = {}  # "001" -> desc
    level2_desc: dict[str, str] = {}  # "001.002" -> desc

    # Track which level-2 parents have already had a .001 header emitted
    emitted_l3_header: set[str] = set()

    # Track which level-1 and level-2 headers have been emitted
    emitted_headers: set[str] = set()

    for item in items:
        padded = item.padded_item
        parts = item.parts
        level = item.level

        if level == 1:
            level1_desc[padded] = item.description
            output.append(OutputRow(item=padded, description=item.description))
            emitted_headers.add(padded)

        elif level == 2:
            level2_desc[padded] = item.description
            output.append(OutputRow(item=padded, description=item.description))
            emitted_headers.add(padded)

        elif level == 3:
            if not item.is_data:
                # It's already a level-3 header — keep as-is
                output.append(OutputRow(item=padded, description=item.description))
            else:
                # Data item at level 3 → need to create a level-3 header
                # and push this item to level 4
                parent_key = ".".join(parts[:2])  # e.g. "001.002"
                l3_header_key = f"{parent_key}.001"

                if l3_header_key not in emitted_l3_header:
                    # Create the level-3 header with parent description
                    parent_desc = level2_desc.get(parent_key, item.description)
                    output.append(OutputRow(
                        item=l3_header_key,
                        description=parent_desc,
                    ))
                    emitted_l3_header.add(l3_header_key)

                # Push data to level 4: parent.001.original_sub_number
                sub_number = parts[2]  # original 3rd segment
                l4_item = f"{parent_key}.001.{sub_number}"
                output.append(OutputRow(
                    item=l4_item,
                    code=item.code,
                    description=item.description,
                    unit=normalise_unit(item.unit),
                    quantity=item.quantity,
                    price=item.price,
                ))

        elif level == 4:
            # Already at level 4 — output as-is
            output.append(OutputRow(
                item=padded,
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
