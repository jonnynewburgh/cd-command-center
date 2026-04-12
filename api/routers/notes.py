"""
api/routers/notes.py — User notes and bookmarks.

Notes are freetext annotations attached to any entity (school, FQHC, ECE, etc.).
Bookmarks are starred entities shown in the dashboard sidebar.

Entity types: school, fqhc, ece, nmtc_project, cde, census_tract, org_990
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import db

router = APIRouter()


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class NoteCreate(BaseModel):
    note_text: str


class NoteUpdate(BaseModel):
    note_text: str


class BookmarkCreate(BaseModel):
    entity_type: str
    entity_id: str
    label: str


# ---------------------------------------------------------------------------
# Notes
# ---------------------------------------------------------------------------

@router.get("/{entity_type}/{entity_id}")
def get_notes(entity_type: str, entity_id: str):
    """Return all notes for a specific entity, newest first."""
    return db.get_user_notes(entity_type, entity_id)


@router.post("/{entity_type}/{entity_id}", status_code=201)
def create_note(entity_type: str, entity_id: str, body: NoteCreate):
    """Add a new note to an entity. Returns the new note ID."""
    note_id = db.save_user_note(entity_type, entity_id, body.note_text)
    return {"note_id": note_id}


@router.put("/{entity_type}/{entity_id}/{note_id}")
def update_note(entity_type: str, entity_id: str, note_id: int, body: NoteUpdate):
    """Update the text of an existing note."""
    db.update_user_note(note_id, body.note_text)
    return {"ok": True}


@router.delete("/{entity_type}/{entity_id}/{note_id}", status_code=204)
def delete_note(entity_type: str, entity_id: str, note_id: int):
    """Delete a note."""
    db.delete_user_note(note_id)


# ---------------------------------------------------------------------------
# Bookmarks  (mounted under /notes/bookmarks via main.py prefix)
# ---------------------------------------------------------------------------

@router.get("/bookmarks/all")
def get_bookmarks():
    """Return all bookmarks, newest first."""
    return db.get_bookmarks()


@router.post("/bookmarks", status_code=201)
def add_bookmark(body: BookmarkCreate):
    """Bookmark an entity. Silently ignores duplicates."""
    db.save_bookmark(body.entity_type, body.entity_id, body.label)
    return {"ok": True}


@router.delete("/bookmarks/{entity_type}/{entity_id}", status_code=204)
def remove_bookmark(entity_type: str, entity_id: str):
    """Remove a bookmark."""
    db.delete_bookmark(entity_type, entity_id)


@router.get("/bookmarks/{entity_type}/{entity_id}")
def check_bookmark(entity_type: str, entity_id: str):
    """Return whether an entity is bookmarked."""
    return {"bookmarked": db.is_bookmarked(entity_type, entity_id)}
