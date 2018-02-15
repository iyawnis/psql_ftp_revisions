
-- See which docass documents will be deleted
SELECT docass_id
FROM docass
WHERE docass_target_type='FILE' AND docass_source_type='I' AND
docass_target_id IN
(SELECT file_id
FROM
    (SELECT file_id,
     ROW_NUMBER() OVER( PARTITION BY file_title
    ORDER BY file_id DESC ) AS row_num
    FROM file ) t
    WHERE t.row_num > 1 );


-- See which files will be deleted
SELECT file_id, file_title FROM file WHERE file_id IN
(SELECT file_id
 FROM
    (SELECT file_id,
     ROW_NUMBER() OVER( PARTITION BY file_title
    ORDER BY file_id DESC ) AS row_num
    FROM file ) t
    WHERE t.row_num > 1 );


-- Delete documents EXECUTE BEFORE FILE
DELETE FROM docass
WHERE docass_target_type='FILE' AND docass_source_type='I' AND
docass_target_id IN
(SELECT file_id
FROM
    (SELECT file_id,
     ROW_NUMBER() OVER( PARTITION BY file_title
    ORDER BY file_id DESC ) AS row_num
    FROM file ) t
    WHERE t.row_num > 1 );


-- Delete files DO NOT EXECUTE BEFORE DOCASS
DELETE FROM file WHERE file_id IN (SELECT file_id
FROM
    (SELECT file_id,
     ROW_NUMBER() OVER( PARTITION BY file_title
    ORDER BY file_id DESC ) AS row_num
    FROM file ) t
    WHERE t.row_num > 1 );
