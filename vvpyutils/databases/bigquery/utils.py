from agentsynthpanel.config.database import (
    TABLE_CONVERSATIONS,
    TABLE_PERSONAS,
    TABLE_QUESTIONS,
)
from agentsynthpanel.models.base_sqlmodels import SQLModelBigQuery
from agentsynthpanel.models.conversation import ConversationBQ as ConversationBQ
from agentsynthpanel.models.persona import PersonaBQ as PersonaBQ
from agentsynthpanel.models.question import QuestionBQ as QuestionBQ
from agentsynthpanel.utils.database import get_engine
from loguru import logger
from sqlalchemy_bigquery import BigQueryDialect
from sqlmodel import SQLModel, text
from utils.bigquery.pipeline.config import (
    BQ_DATASET_BRONZE,
    BQ_DATASET_RAW,
    GCP_PROJECT_ID,
    GCS_BUCKET_BQ_LZ,
)

TABLE_MODEL_MAP = {
    TABLE_CONVERSATIONS: ConversationBQ,
    TABLE_PERSONAS: PersonaBQ,
    TABLE_QUESTIONS: QuestionBQ,
}

engine_raw = get_engine(f"bigquery://{GCP_PROJECT_ID}/{BQ_DATASET_RAW}")
engine_bronze = get_engine(
    f"bigquery://{GCP_PROJECT_ID}/{BQ_DATASET_BRONZE}", skip_table_creation=True
)

# SQLModelBigQuery.metadata.create_all(engine_raw)


def generate_table_schema(model_class: SQLModel) -> str:
    """Generate BigQuery schema definition from SQLModel"""
    # TODO: Integrate this into the SQLModelBigQuery model class

    dialect = BigQueryDialect()
    columns = []

    for column in model_class.__table__.columns:
        col_name = column.name

        # Compile the column type for BigQuery
        try:
            col_type_compiled = column.type.compile(dialect=dialect)
            col_type = str(col_type_compiled).upper()

            if col_type in ["VARCHAR", "TEXT"]:
                col_type = "STRING"

        except Exception as e:
            logger.warning(
                f"Warning: Could not compile type for column {col_name}: {e}"
            )

        columns.append(f"    {col_name} {col_type}")

    return ",\n".join(columns)


def sql_personas_raw_to_bronze() -> str:
    sql = f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_PERSONAS}`
    SELECT raw.*
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.tmp_{TABLE_PERSONAS}` AS raw
    LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_PERSONAS}` AS bronze
    ON raw.id = bronze.id
    WHERE bronze.id IS NULL
    """
    return sql


def sql_questions_raw_to_bronze() -> str:
    sql = f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_QUESTIONS}`
    SELECT raw.*
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.tmp_{TABLE_QUESTIONS}` AS raw
    LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_QUESTIONS}` AS bronze
    ON raw.id = bronze.id
    WHERE bronze.id IS NULL
    """
    return sql


def sql_conversations_raw_to_bronze() -> str:
    sql = f"""
        INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_CONVERSATIONS}` (
            id,
            persona_id,
            question_id,
            llm_provider,
            llm_model_id,
            llm_model_name,
            created_at,
            agent_output,
            model_settings,
            messages,
            extracted_entities,
            usage
        )
        SELECT 
            id,
            persona_id,
            question_id,
            llm_provider,
            llm_model_id,
            llm_model_name,
            created_at,
            agent_output,
            PARSE_JSON(model_settings) AS model_settings,
            PARSE_JSON(messages) AS messages,
            PARSE_JSON(extracted_entities) AS extracted_entities,
            PARSE_JSON(usage) AS usage
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.tmp_{TABLE_CONVERSATIONS}` AS raw
        WHERE NOT EXISTS (
            SELECT 1 
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_CONVERSATIONS}` AS bronze
            WHERE bronze.id = raw.id
        )
    """
    return sql


def provision_raw_external_tables():
    with engine_raw.connect() as conn:
        for table_name, model_class in TABLE_MODEL_MAP.items():
            tmp_table_name = f"tmp_{table_name}"
            table_schema = generate_table_schema(model_class)
            # logger.info(
            #     f"👷 Creating external table for {table_name} with schema:\n{table_schema}"
            # )

            sql = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS
                    `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{tmp_table_name}`
                    ({table_schema})
                OPTIONS (
                    format = 'NEWLINE_DELIMITED_JSON',
                    uris = ['gs://{GCS_BUCKET_BQ_LZ}/sqlite-mirror/{table_name}*.jsonl']
                )
                """
            try:
                conn.execute(text(sql))
                logger.info(f"✅ Created external table: {tmp_table_name}")
            except Exception as e:
                logger.error(f"❌ Error creating external table {tmp_table_name}: {e}")
                logger.error(f"Failed SQL:\n{sql}")


def provision_bronze_tables():
    with engine_bronze.connect() as conn:
        # First, only create the tables that are not conversations,
        #   using the existing SQLModelBigQuery definitions
        SQLModelBigQuery.metadata.create_all(
            conn,
            tables=[
                PersonaBQ.__table__,
                QuestionBQ.__table__,
            ],
        )

        # Handle conversations table separately due to usage of JSON types.
        #  JSON type hasn't been implemented in sqlalchemy-bigquery yet.
        # TODO: look at dynamically generating the schema and replacing complex types with JSON
        sql = f"""
            CREATE TABLE IF NOT EXISTS 
                `{GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_CONVERSATIONS}` (
                    id STRING NOT NULL,
                    persona_id STRING NOT NULL,
                    question_id STRING NOT NULL,
                    llm_provider STRING NOT NULL,
                    llm_model_id STRING NOT NULL,
                    llm_model_name STRING NOT NULL,
                    created_at DATETIME NOT NULL,
                    agent_output STRING,
                    model_settings JSON,
                    messages JSON,
                    extracted_entities JSON,
                    usage JSON
                )
            OPTIONS (
                description = 'Conversations with LLMs, stored in JSON format for flexibility'
            )
        """
        conn.execute(text(sql))
        logger.info(
            f"✅ Created {GCP_PROJECT_ID}.{BQ_DATASET_BRONZE}.{TABLE_CONVERSATIONS}"
        )


def raw_to_bronze():
    sql_statements = [
        sql_personas_raw_to_bronze(),
        sql_questions_raw_to_bronze(),
        sql_conversations_raw_to_bronze(),
    ]
    with engine_bronze.connect() as conn:
        for sql in sql_statements:
            try:
                conn.execute(text(sql))
                logger.info(f"✅ Executed SQL: {sql}")
            except Exception as e:
                logger.error(f"❌ Error executing SQL: {sql}")
                logger.error(f"Error: {e}")


if __name__ == "__main__":
    provision_raw_external_tables()
    provision_bronze_tables()
    raw_to_bronze()
