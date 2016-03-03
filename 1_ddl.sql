CREATE OR REPLACE TYPE ty_row_str_split  as object (strValue VARCHAR2 (4000));
CREATE OR REPLACE TYPE ty_tbl_str_split IS TABLE OF ty_row_str_split ;