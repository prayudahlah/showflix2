#!/bin/bash
set -euo pipefail

: "${POSTGRES_USER:?}"
: "${POSTGRES_DB:?}"

# -- UUIDv5 namespaces
# ROOT_NAMESPACE=6c44c005-0016-4083-b7e7-b383bbeac327
GENRE_NAMESPACE=0c0fe7f7-c8de-5bb9-83e3-1c91841421ed
REGION_NAMESPACE=337d579e-9246-5dfe-9850-074a61f20071
LANGUAGE_NAMESPACE=c3bb0b0a-47f6-5394-968a-f1da6df7c946
PRODUCTION_COMPANY_NAMESPACE=e47367bc-4f93-5f5c-8a02-c07a29087a1e

# Catalogue schema ddl
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" <<-EOSQL
    ---- Catalogue Schema
    CREATE SCHEMA IF NOT EXISTS catalogue;

    ---- UUIDv5 Converter Functions
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

    CREATE OR REPLACE FUNCTION get_genre_id(
        genre_name VARCHAR(255)
    )
    RETURNS UUID
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS \$\$
        SELECT uuid_generate_v5('${GENRE_NAMESPACE}'::UUID, genre_name);
    \$\$;

    CREATE OR REPLACE FUNCTION get_region_id(
        region_name text
    )
    RETURNS UUID
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS \$\$
        SELECT uuid_generate_v5('${REGION_NAMESPACE}'::UUID, region_name);
    \$\$;

    CREATE OR REPLACE FUNCTION get_language_id(
        language_name text
    )
    RETURNS UUID
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS \$\$
        SELECT uuid_generate_v5('${LANGUAGE_NAMESPACE}'::UUID, language_name);
    \$\$;

    CREATE OR REPLACE FUNCTION get_production_company_id(
        production_company_name text
    )
    RETURNS UUID
    LANGUAGE sql
    IMMUTABLE
    STRICT
    AS \$\$
        SELECT uuid_generate_v5('${PRODUCTION_COMPANY_NAMESPACE}'::UUID, production_company_name);
    \$\$;

    ---- Enum
    CREATE TYPE show_status_enum AS ENUM ('Post Production', 'Canceled', 'Released', 'Planned', 'In Production', 'Rumored');

    ---- Catalogue Tables
    CREATE TABLE IF NOT EXISTS catalogue.genres (
        genre_id UUID GENERATED ALWAYS AS (
            get_genre_id(genre_name)
        ) STORED PRIMARY KEY,
        genre_name VARCHAR(255) NOT NULL
            CONSTRAINT genres_genre_name_uq UNIQUE
    );

    CREATE TABLE IF NOT EXISTS catalogue.regions (
        region_id UUID GENERATED ALWAYS AS (
            get_region_id(region_name)
        ) STORED PRIMARY KEY,
        region_name VARCHAR(255) NOT NULL
            CONSTRAINT regions_region_name_uq UNIQUE
    );

    CREATE TABLE IF NOT EXISTS catalogue.languages (
        language_id UUID GENERATED ALWAYS AS (
            get_language_id(language_name)
        ) STORED PRIMARY KEY,
        language_name VARCHAR(255) NOT NULL
            CONSTRAINT languages_language_name_uq UNIQUE
    );

    CREATE TABLE IF NOT EXISTS catalogue.production_companies (
        production_company_id UUID GENERATED ALWAYS AS (
            get_production_company_id(production_company_name)
        ) STORED PRIMARY KEY,
        production_company_name TEXT NOT NULL
            CONSTRAINT production_companies_production_company_name_uq UNIQUE
    );

    CREATE TABLE catalogue.shows (
        show_id           BIGINT NOT NULL PRIMARY KEY,
        title             TEXT,
        original_title    TEXT,
        original_language VARCHAR(255),
        vote_average      NUMERIC(5, 3),
        vote_count        INTEGER,
        popularity        NUMERIC(10, 4),
        status            show_status_enum NOT NULL,
        release_date      DATE,
        budget            BIGINT,
        revenue           BIGINT,
        runtime           INTEGER,
        is_adult          BOOLEAN,
        backdrop_path     VARCHAR(255),
        poster_path       VARCHAR(255),
        homepage          TEXT,
        tagline           TEXT,
        overview          TEXT,
        keywords          TEXT
    );

    CREATE TABLE IF NOT EXISTS catalogue.show_genres (
        show_id BIGINT NOT NULL
            REFERENCES catalogue.shows,
        genre_id UUID NOT NULL
            REFERENCES catalogue.genres,
        PRIMARY KEY (show_id, genre_id)
    );

    CREATE TABLE IF NOT EXISTS catalogue.show_production_companies (
        show_id BIGINT NOT NULL
            REFERENCES catalogue.shows,
        production_company_id UUID NOT NULL
            REFERENCES catalogue.production_companies,
        PRIMARY KEY (show_id, production_company_id)
    );

    CREATE TABLE IF NOT EXISTS catalogue.show_production_regions (
        show_id BIGINT NOT NULL
            REFERENCES catalogue.shows,
        region_id UUID NOT NULL
            REFERENCES catalogue.regions,
        PRIMARY KEY (show_id, region_id)
    );

    CREATE TABLE IF NOT EXISTS catalogue.show_spoken_languages (
        show_id BIGINT NOT NULL
            REFERENCES catalogue.shows,
        language_id UUID NOT NULL
            REFERENCES catalogue.languages,
        PRIMARY KEY (show_id, language_id)
    );
EOSQL
