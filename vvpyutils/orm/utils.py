from agentsynthpanel.config.env import DEBUG_MODE, SQLLITE_URL
from agentsynthpanel.models import (
    Conversation as Conversation,
)
from agentsynthpanel.models import (
    Persona,
    Question,
)
from agentsynthpanel.models.base_sqlmodels import SQLModelBigQuery, SQLModelSQLite
from loguru import logger
from sqlalchemy.engine.base import Engine
from sqlmodel import Session, SQLModel, create_engine, desc, func, select

_engines: dict[str, Engine] = {}


def get_engine(db_url: str | None = None, skip_table_creation: bool = False) -> Engine:
    """
    Return a cached Engine for the given URL. On first creation, run the
    appropriate `metadata.create_all(...)` so only the correct tables are created.
    """
    global _engines

    resolved_url = db_url or SQLLITE_URL

    if resolved_url not in _engines:
        engine = create_engine(resolved_url, echo=DEBUG_MODE)
        logger.info(f"Created engine for: {resolved_url}")
        logger.debug(f"Engine details: {engine}")

        if not skip_table_creation:
            if resolved_url.startswith("sqlite"):
                SQLModelSQLite.metadata.create_all(engine)
                logger.info("Created SQLite tables")
            elif resolved_url.startswith("bigquery"):
                SQLModelBigQuery.metadata.create_all(engine)
                logger.info("Created BigQuery tables")
            else:
                SQLModel.metadata.create_all(engine)
                logger.info("Created fallback tables")

        _engines[resolved_url] = engine

    return _engines[resolved_url]


def find_duplicate_rows(
    session: Session,
    model: type[SQLModel],
    exclude_cols: list[str] | None = [],
) -> list[tuple]:
    """
    Find duplicate rows for a given SQLModel, excluding specified columns.
    Returns a list of tuples containing the values of the non-excluded columns
    plus an 'occurrences' count, ordered by occurrences descending.
    """
    # 1. Build a list of column expressions to group by, excluding unwanted columns
    cols = [
        getattr(model, col.name)
        for col in model.__table__.columns
        if col.name not in exclude_cols
    ]
    if not cols:
        raise ValueError("No columns left after excluding!")

    # 2. Construct the SELECT statement: group by remaining cols, count occurrences
    stmt = (
        select(*cols, func.count().label("occurrences"))
        .group_by(*cols)
        .having(func.count() > 1)
        .order_by(desc("occurrences"))
    )

    # 3. Execute and return results
    results = session.exec(stmt).fetchall()
    return results


# TODO - move each table's helper functions to separate files. Base class to handle common operations (e.g. fetch(), insert, update, delete). Subclasses pointing to specific models.


def fetch_personas(
    session: Session, randomize: bool = True, limit: int | None = None
) -> list[Persona]:
    stmt = select(Persona)

    if randomize:
        stmt = stmt.order_by(func.random())

    if limit is not None:
        stmt = stmt.limit(limit)

    personas = session.exec(stmt).fetchall()
    return personas


def fetch_questions(
    session: Session, randomize: bool = True, limit: int | None = None
) -> list[Question]:
    stmt = select(Question)

    if randomize:
        stmt = stmt.order_by(func.random())

    if limit is not None:
        stmt = stmt.limit(limit)

    questions = session.exec(stmt).fetchall()
    return questions


def find_question(question: str) -> Question | None:
    """
    Find a Question by its text. Returns the Question object if found, else None.
    """
    with Session(get_engine()) as session:
        stmt = select(Question).where(Question.text == question)
        result = session.exec(stmt).first()
        return result


def insert_question(question_text: str) -> Question:
    """
    Insert a new Question into the database. If it already exists, return the existing one.
    """
    with Session(get_engine()) as session:
        new_question = Question(text=question_text)
        session.add(new_question)
        session.commit()
        session.refresh(new_question)
        return new_question


def update_question_table(question: str) -> Question:
    """Updates the question table with a new question if it doesn't already exist."""
    existing_question = find_question(question=question)
    if not existing_question:
        return insert_question(question)

    return existing_question


if __name__ == "__main__":
    # from devtools import pprint
    # main_engine = get_engine()  # For default SQLLITE_URL, tables created if new
    # SQLModelSQLite.create_all(main_engine)  # Ensure tables are created
    pass
