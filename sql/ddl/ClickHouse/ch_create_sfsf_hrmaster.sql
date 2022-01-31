CREATE TABLE IF NOT EXISTS commercial.hr_master_test
(
   Nationality Nullable(String),
   Gender Nullable(String),
   LastNameJP Nullable(String),
   MiddleNameJP Nullable(String),
   FirstNameJP Nullable(String),
   LastNameEN Nullable(String),
   MiddleNameEN Nullable(String),
   FirstNameEN Nullable(String),
   Status Nullable(String),
   Email String,
   EmployeeCode String,
   ManagerEmployeeCode Nullable(String),
   CompanyCode Nullable(String),
   JobCode Nullable(String),
   DepartmentCode Nullable(String),
   JobTitleEN Nullable(String),
   JobTitleJP Nullable(String),
   JobTitleCommonEN Nullable(String),
   JobTitleCommonJP Nullable(String),
   SectionNameJP Nullable(String),
   SectionCD Nullable(String),
   SectionNameEN Nullable(String),
   LCode Nullable(String),
   L0Code Nullable(String),
   L0NameEN Nullable(String),
   L0NameJP Nullable(String),
   L1Code Nullable(String),
   L1NameEN Nullable(String),
   L1NameJP Nullable(String),
   L2Code Nullable(String),
   L2NameEN Nullable(String),
   L2NameJP Nullable(String),
   L2_5Code Nullable(String),
   L2_5NameEN Nullable(String),
   L2_5NameJP Nullable(String),
   L3Code Nullable(String),
   L3NameEN Nullable(String),
   L3NameJP Nullable(String),
   L4_1Code Nullable(String),
   L4_1NameEN Nullable(String),
   L4_1NameJP Nullable(String),
   L4_2Code Nullable(String),
   L4_2NameEN Nullable(String),
   L4_2NameJP Nullable(String),
   L4_3Code Nullable(String),
   L4_3NameEN Nullable(String),
   L4_3NameJP Nullable(String),
   L4_4Code Nullable(String),
   L4_4NameEN Nullable(String),
   L4_4NameJP Nullable(String),
   L5_1Code Nullable(String),
   L5_1NameEN Nullable(String),
   L5_1NameJP Nullable(String),
   L5_2Code Nullable(String),
   L5_2NameEN Nullable(String),
   L5_2NameJP Nullable(String),
   L6Code Nullable(String),
   L6NameEN Nullable(String),
   L6NameJP Nullable(String),
   identifier Nullable(String)
)
ENGINE = MergeTree() 
ORDER BY EmployeeCode;