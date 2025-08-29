create or replace PACKAGE "LTF_BICC_UTILITY_PKG" AS 
																									
	 PROCEDURE LTF_ORCHESTRATOR (i_file_content in clob, i_mode in varchar2);						
     PROCEDURE LTF_STAGE_DATA (i_file_name in VARCHAR2,  i_stage_table_name in VARCHAR2, i_interface_table_name in VARCHAR2);			
     PROCEDURE LTF_MERGE_TABLES (i_source_table IN VARCHAR2,i_target_table IN VARCHAR2);
     PROCEDURE LTF_SYNC_PRIMARY_KEYS (i_file_name IN VARCHAR2, i_pk_table_name IN VARCHAR2, i_stage_table_name IN VARCHAR2, i_interface_table_name IN VARCHAR2);

END LTF_BICC_UTILITY_PKG;