CREATE TABLE IF NOT EXISTS osdf_nodes (
  "id" TEXT UNIQUE,
  "node_type" TEXT,
  "orig_internal_or_notes" TEXT,
  "internal_id" TEXT,
  "linkage" TEXT,
  "parent_node_type" TEXT,
  "parent_internal_id" TEXT,
  "parent_node_id" TEXT,
  "meta" TEXT,
  "ns" TEXT,
  "ver" INTEGER,
  "acl" TEXT,
  "date_retrieved" DATETIME
);

DROP VIEW visits;
CREATE VIEW visits AS SELECT * FROM osdf_nodes WHERE node_type = "visit" ORDER BY internal_id;

DROP VIEW r16s_raw_trim;
CREATE VIEW samples AS SELECT * FROM osdf_nodes WHERE node_type = "sample" ORDER BY internal_id;

DROP VIEW r16s_raw_trim;
CREATE VIEW r16S_prep AS SELECT * FROM osdf_nodes WHERE node_type = "16s_dna_prep" ORDER BY internal_id;

DROP VIEW r16s_raw_trim;
CREATE VIEW r16S_raw AS SELECT * FROM osdf_nodes WHERE node_type = "16s_raw_seq_set" ORDER BY internal_id;

DROP VIEW r16s_raw_trim;
CREATE VIEW r16S_trim AS SELECT * FROM osdf_nodes WHERE node_type = "16s_trimmed_seq_set" ORDER BY internal_id;

DROP VIEW r16s_raw_trim;
CREATE VIEW r16s_raw_trim as
SELECT r16S_raw.internal_id AS raw_id, r16S_raw.id AS raw_node_id,
    r16S_trim.internal_id AS clean_id, r16S_trim.id AS clean_node_id
FROM r16S_raw
LEFT OUTER JOIN r16S_trim
ON r16S_raw.id = r16S_trim.parent_node_id
ORDER BY clean_id;

DROP VIEW r16s_nodes;
CREATE VIEW r16s_nodes AS
select
    samples.internal_id AS sample_id, samples.id AS sample_node_id,
    r16S_prep.internal_id AS prep_id, r16S_prep.id AS prep_node_id,
    r16S_raw.internal_id AS raw_id, r16S_raw.id AS raw_node_id,
    r16S_trim.internal_id AS clean_id, r16S_trim.id AS clean_node_id
FROM samples
LEFT OUTER JOIN r16S_prep ON samples.internal_id = r16S_prep.parent_internal_id
LEFT OUTER JOIN r16S_raw ON r16S_prep.internal_id = r16S_raw.parent_internal_id
LEFT OUTER JOIN r16S_trim ON r16S_raw.internal_id = r16S_trim.parent_internal_id;

DROP VIEW sample_16S_prep;
CREATE VIEW sample_16S_prep AS
SELECT samples.internal_id AS samples_id, samples.id AS samples_node_id,
    r16S_raw.internal_id AS raw_id, r16S_raw.id AS raw_node_id
FROM samples
LEFT OUTER JOIN r16S_raw
ON r16S_raw.internal_id = samples.parent_internal_id;
