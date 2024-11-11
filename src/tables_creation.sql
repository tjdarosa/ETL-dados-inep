CREATE TABLE fato_tecnologia_escolar (
    qtd_comp_port INTEGER,              -- quantidade de computadores portáteis
    qtd_desktop INTEGER,                -- quantidade de desktops
    qtd_tablet INTEGER,                 -- quantidade de tablets
    qtd_esc_inet INTEGER,               -- quantidade de escolas com internet
    qtd_esc_blarga INTEGER,             -- quantidade de escolas com internet banda larga
    qtd_esc_inet_apd INTEGER,           -- quantiadde de escolas com internet para aprendizagem
    location_key INTEGER FOREIGN KEY,   -- chave estrangeira para dimensão localização
    dep_adm_key INTEGER FOREIGN KEY,    -- chave estrangeira para dimensão dependência administrativa
);

CREATE TABLE dim_localizacao (
    municipio VARCHAR(100),             -- municipio
    microrregiao VARCHAR(100),          -- microrregião
    mesorregiao VARCHAR(100),           -- mesorregião
    uf VARCHAR(2),                      -- unidade federativa
    regiao VARCHAR(100),                -- região
    location_key INTEGER PRIMARY KEY,   -- chave primária para localização
);

CREATE TABLE dim_dep_adm (
    dep_adm VARCHAR(20),                -- dependência administrativa
    dep_adm_key INTEGER PRIMARY KEY     -- chave primária para dependência administrativa
);
