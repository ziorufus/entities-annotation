from fastapi import FastAPI, HTTPException, UploadFile, File
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from dotenv import load_dotenv
import pandas as pd
import json
import os
import sys

from models import Location, Base
from schemas import UpdateInfoRequest, UpdateGroupRequest

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.exit("❌ Error: DB_FILENAME not set in .env file. Please define it before running the app.")

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

Base.metadata.create_all(bind=engine)

# --- FastAPI app ---
app = FastAPI(title="Geographical Entities API")

def smart_split(line: str) -> list[str]:
    """
    Split a line by commas that are not inside parentheses.
    e.g. "A, B (x, y), C" -> ["A", "B (x, y)", "C"]
    """
    parts = []
    current = []
    depth = 0

    for char in line:
        if char == ',' and depth == 0:
            part = ''.join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(char)
            if char == '(':
                depth += 1
            elif char == ')':
                depth = max(depth - 1, 0)

    # Add the last part
    part = ''.join(current).strip()
    if part:
        parts.append(part)

    return parts

def get_location_by_ref(db, ref: str, role: str, errors: list):
    """
    Parse a 'name-type' string, retrieve the Location object,
    and append errors if invalid or missing.
    role = 'leader' or 'member' (used for clearer error messages)
    """
    if "-" not in ref:
        errors.append(f"Invalid format for {role} '{ref}' (expected name-type)")
        return None

    name, type_ = ref.rsplit("-", 1)
    loc = db.query(Location).filter(Location.name == name, Location.type == type_).first()

    if not loc:
        errors.append(f"{role.capitalize()} '{ref}' not found in DB")
        return None

    return loc


def update_total_citations(db, group_id: int):
    """Recalculate total_citations for a group ID."""
    members = db.query(Location).filter(Location.group == group_id).all()
    total = sum(m.citations for m in members)
    group_entity = db.query(Location).filter(Location.id == group_id).first()
    if group_entity:
        group_entity.total_citations = total + group_entity.citations
        db.commit()
    return total


# --- Routes ---

@app.get("/location-list")
def get_locations():
    """Return all locations as JSON."""
    db = SessionLocal()
    locations = db.query(Location).all()
    db.close()
    return [
        {
            "id": loc.id,
            "name": loc.name,
            "type": loc.type,
            "citations": loc.citations,
            "total_citations": loc.total_citations,
            "group": loc.group,
            "location_info": json.loads(loc.location_info) if loc.location_info else None,
        }
        for loc in locations
    ]


@app.post("/load-data")
async def load_data(file: UploadFile = File(...)):
    """
    Upload an Excel file where the first row are column names.
    Expected columns: 'name', 'type', 'value' (value -> citations)
    """

    # Validate extension
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="File must be an Excel file (.xlsx or .xls)")

    try:
        # Read Excel file into a DataFrame
        df = pd.read_excel(file.file)

        # Check required columns
        required_columns = {"name", "type", "value"}
        if not required_columns.issubset(df.columns):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns. Found columns: {list(df.columns)}. Required: {list(required_columns)}"
            )

        # Open DB session
        db = SessionLocal()

        # Empty the table
        db.query(Location).delete()
        db.commit()

        # Insert new rows
        inserted = 0
        skipped = 0
        errors = []

        for _, row in df.iterrows():
            name = str(row["name"]).strip()
            type_ = str(row["type"]).strip()
            # value = int(row["value"]) if not pd.isna(row["value"]) else 0

            # Handle missing or invalid value
            raw_value = row.get("value", None)
            if pd.isna(raw_value):
                skipped += 1
                errors.append(f"Skipped: ({name}, {type_}) has missing value")
                continue

            try:
                value = int(raw_value)
            except ValueError:
                skipped += 1
                errors.append(f"Skipped: ({name}, {type_}) has invalid value '{raw_value}'")
                continue

            # Skip zero or negative values
            if value <= 0:
                skipped += 1
                errors.append(f"Skipped: ({name}, {type_}) has non-positive value {value}")
                continue

            new_loc = Location(
                name=name,
                type=type_,
                citations=value,
                total_citations=value,
                group=None,
                location_info=None
            )

            try:
                db.add(new_loc)
                db.commit()
                inserted += 1
            except IntegrityError:
                db.rollback()
                skipped += 1
                errors.append(f"Duplicate skipped: ({name}, {type_})")

        db.commit()
        db.close()

        return {"status": "success", "inserted": inserted, "skipped": skipped, "errors": errors}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.put("/update-info")
def update_info(request: UpdateInfoRequest):
    """Update the location_info field for the given ID."""
    db = SessionLocal()
    location = db.query(Location).filter(Location.id == request.id).first()
    if not location:
        db.close()
        raise HTTPException(status_code=404, detail="Location not found")
    location.location_info = json.dumps(request.location_info)
    db.commit()
    db.close()
    return {"status": "success", "id": request.id, "updated_info": request.location_info}


@app.post("/load-groups")
async def load_groups(file: UploadFile = File(...)):
    if not file.filename.endswith(".txt"):
        raise HTTPException(status_code=400, detail="File must be a .txt file")

    try:
        content = (await file.read()).decode("utf-8")
        lines = [line.strip() for line in content.splitlines() if line.strip()]

        db = SessionLocal()
        updated_count = 0
        errors = []
        leader_ids = set()

        for line in lines:
            # parts = [p.strip() for p in line.split(",") if p.strip()]
            parts = smart_split(line)
            if len(parts) < 2:
                errors.append(f"Invalid group line (needs at least 2 items): {line}")
                continue

            leader_ref = parts[0]
            member_refs = parts[1:]

            leader = get_location_by_ref(db, leader_ref, "leader", errors)
            if not leader:
                continue

            for member_ref in member_refs:
                member = get_location_by_ref(db, member_ref, "member", errors)
                if not member:
                    continue

                if member.id != leader.id:
                    member.group = leader.id
                    updated_count += 1

            leader_ids.add(leader.id)

        db.commit()

        # Update total_citations for all leaders involved
        for gid in leader_ids:
            update_total_citations(db, gid)

        db.close()

        return {
            "status": "success",
            "updated_members": updated_count,
            "errors": errors,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@app.put("/update-group")
def update_group(request: UpdateGroupRequest):
    """Compare citations of two locations and update the one with fewer citations."""
    db = SessionLocal()
    loc1 = db.query(Location).filter(Location.id == request.id1).first()
    loc2 = db.query(Location).filter(Location.id == request.id2).first()

    if not loc1 or not loc2:
        db.close()
        raise HTTPException(status_code=404, detail="One or both locations not found")

    # Determine f (fewer citations) and g (greater citations)
    if loc1.citations < loc2.citations:
        f, g = loc1, loc2
    else:
        f, g = loc2, loc1

    # Save old group if f already had one
    old_group_id = f.group

    if g.group:
        new_group_id = g.group
    else:
        new_group_id = g.id

    # Update any records having f as their group → assign them to g
    db.query(Location).filter(Location.group == f.id).update({"group": new_group_id})

    # Update f itself
    f.group = new_group_id
    f.total_citations = None

    # Commit intermediate changes
    db.commit()

    # Update total_citations for g
    update_total_citations(db, new_group_id)

    # If f previously had a group, update that too
    if old_group_id:
        update_total_citations(db, old_group_id)

    db.close()

    return {
        "status": "updated",
        "f": f.id,
        "g": g.id,
        "old_group_updated": old_group_id is not None,
    }
