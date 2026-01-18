CREATE TYPE show_status_enum AS ENUM ('Post Production', 'Canceled', 'Rumored', 'Planned', 'Released', 'In Production');

ALTER TYPE show_status_enum OWNER TO showflixadmin;

