package hcsc

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by DPatil on 8/30/2016.
  */
class CompressionMaster(sparkSession: SparkSession) {
  def load_CompressionMaster(): Unit = {

    print("\nload_dstfacout\n")
    load_dstfacout()

    print("\nload_dstprof\n")
    load_dstprof()

    print("\nload_last_seq2\n")
    load_last_seq2()

    print("\nload_last_seq_inpat\n")
    load_last_seq_inpat()

    print("\nload_multgestdiags\n")
    load_multgestdiags()

    print("\nload_last_seq_inpat_multgest\n")
    load_last_seq_inpat_multgest()

    print("\nload_pretermdiags\n")
    load_pretermdiags()

    print("\nload_last_seq_inpat_preterm\n")
    load_last_seq_inpat_preterm()

    sparkSession.sql("drop table if exists pretermdiags")


    print("\nload_last_seq_inpat_servicetype\n")
    load_last_seq_inpat_servicetype()

    print("\nload_first_claims\n")
    load_first_claims()

    print("\nload_babies\n")
    load_babies()

    sparkSession.sql("""drop table if exists last_seq_inpat""")

    print("\nload_first_claims2\n")
    load_first_claims2()

    sparkSession.sql("""drop table if exists first_claims""")

    print("\nload_adm1\n")
    load_adm1()

    sparkSession.sql("drop table first_claims2")

    print("\nload_spans\n")
    load_spans()



    print("\nload_mergeprovadms\n")
    load_mergeprovadms()

    sparkSession.sql("drop table if exists adm1")

    print("\nload_admxclm\n")
    load_admxclm()

    sparkSession.sql("drop table if exists spans")

    print("\nload_temp\n")
    load_temp()

    print("\nload_embeddedmergeprovadms\n")
    load_embeddedmergeprovadms()

    print("\nload_embeddedadmxclm\n")
    load_embeddedadmxclm()

    print("\ndelete_mergeprovadms\n")
    delete_mergeprovadms()

    print("\ndelete_admxclm\n")
    delete_admxclm()
    sparkSession.sql("drop table if exists temp")

    print("\nload_preadmits\n")
    load_preadmits()
    sparkSession.sql("drop table if exists mergeprovadms")

    print("\nload_first_claims2_3\n")
    load_first_claims2_3()
    sparkSession.sql("drop table if exists adm1")


    print("\nload_adm1_1\n")
    load_adm1_1()
    sparkSession.sql("drop table first_claims2")

    print("\nload_spans_1\n")
    load_spans_1()

    print("\nload_babymergeprovadms\n")
    load_babymergeprovadms()

    sparkSession.sql("drop table if exists adm1")

    print("\nload_babyadmxclm\n")
    load_babyadmxclm()

    sparkSession.sql("drop table if exists spans")
    sparkSession.sql("drop table if exists last_seq_inpat_servicetype")






    print("\nload_local_hcsc_icd_map\n")
    load_local_hcsc_icd_map()

    print("\nload_pre_facout_claims\n")
    load_pre_facout_claims()

    print("\nload_facout_claims\n")
    load_facout_claims()
    sparkSession.sql("drop table if exists last_seq2")

    print("\nload_prefacvisits\n")
    load_prefacvisits()


    /*
    10	Emergency Room
    11	Other Services
    20	Dialysis
    30	Surgical
    40	Observation Room
    50	Psychiatric
    60	Radiology
    70	Medical Specialties Services
    71	Radiology
    80	Laboratory
    90	PT/OT/ST
    99	Other Services
    */



    print("\nload_visitservicetypelong\n")
    load_visitservicetypelong()


    print("\nload_facvisitsxclm\n")
    load_facvisitsxclm()

    print("\nload_facvisitlapse\n")
    load_facvisitlapse()

    print("\nload_prefacvisits_1\n")
    load_prefacvisits_1()


    print("\nload_facvisitservicetype\n")
    load_facvisitservicetype()

    print("\nload_facvisits\n")
    load_facvisits()

    print("\nload_last_seq2_1\n")
    load_last_seq2_1()

    print("\nload_prof_claims_1\n")
    load_prof_claims_1()

    print("\nload_prof_claims\n")
    load_prof_claims()

    sparkSession.sql("drop table if exists last_seq2")


    print("\nload_preprofvisits\n")
    load_preprofvisits()

    print("\nload_visitservicetypelong_1\n")
    load_visitservicetypelong_1()

    print("\nload_profvisitsxclm\n")
    load_profvisitsxclm()


    print("\nload_profvisitlapse\n")
    load_profvisitlapse()

    print("\nload_new_preprofvisits\n")
    load_new_preprofvisits()

    print("\nload_profvisitservicetype\n")
    load_profvisitservicetype()

    print("\nload_profvisits\n")
    load_profvisits()

    print("\nload_admitlapse\n")
    load_admitlapse()

    print("\nload_preadmits2\n")
    load_preadmits2()

    print("\nload_admitservicetype\n")
    load_admitservicetype()

    print("\nload_admits\n")
    load_admits()
       sparkSession.sql("drop table if exists preadmits")
        sparkSession.sql("drop table if exists preadmits2")
        sparkSession.sql("drop table if exists admitlapse")

    print("\nload_admitservicetype1\n")
    load_admitservicetype1()

    print("\nload_babyadmits\n")
    load_babyadmits()

    print("\nload_facoutcompressionmaster\n")
    load_facoutcompressionmaster()

    print("\nload_profcompressionmaster\n")
    load_profcompressionmaster()

    print("\nload_admxcli\n")
    load_admxcli()

    print("\nload_admitcompressionmaster\n")
    load_admitcompressionmaster()


    print("\nload_rblines\n")
    load_rblines()

    print("\nload_update_admitcompressionmaster\n")
    load_update_admitcompressionmaster()

        sparkSession.sql("drop table if exists prerblines")
        sparkSession.sql(" drop table if exists rblines")


    print("\nload_babyadmxcli\n")
    load_babyadmxcli()

    print("\nload_babyadmitcompressionmaster\n")
    load_babyadmitcompressionmaster()


    print("\nload_rblines_1\n")
    load_rblines_1()



    //****************************************************************
    //****************************************************************
    //****************************************************************


    sparkSession.sql("drop table if exists encounterCompressionMaster")
    val encounterCompressionMaster =sparkSession.sql("CREATE TABLE IF NOT EXISTS encounterCompressionMaster("+
      "ClaimControlID		varchar(30),"+
      "DWClaimKey		varchar(20),"+
      "ClaimLineNumber	varchar(5),"+
      "ServiceID		int,"+
      "ServiceCategory	varchar(25),"+
      "UtilizationServiceType	varchar(50),"+
      "UtilizationDetailServiceType    varchar(90),"+
      "ServiceType		varchar(50),"+
      "ServiceTypeDetail	varchar(100),"+
      "ERFlag			char(1),"+
      "ConcurrentERFlag	char(1),"+
      "ServiceDate		date,"+
      "DischargeDate		date,"+
      "UtilizationDays	int,"+
      "DayLapsePreviousDischarge	int,"+
      "DayLagNextAdmission	int,"+
      "UtilizationDWClaimKey	varchar(20),"+
      "ProcedureID		varchar(50),"+
      "NICUDays			int,"+
      "NICUFlag			char(1),"+
      "MultipleGestationFlag	char(1),"+
      "DeliveryLT39WeeksFlag char(1),"+
      "ERNonEmergentFlag	char(1))" +

      "STORED AS PARQUET"
    )


    val encounterCompressionMaster1=sparkSession.sql(
      """
        |insert into encounterCompressionMaster
        |select
        |ClaimControlID
        |,DWClaimKey
        |,ClaimLineNumber
        |,ServiceID
        |,ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,ServiceType
        |,ServiceTypeDetail
        |,ERFlag
        |,ConcurrentERFlag
        |,ServiceDate
        |,DischargeDate
        |,UtilizationDays
        |,day(cast(DayLapsePreviousDischarge  as date))
        |,day(cast (DayLagNextAdmission as date))
        |,UtilizationDWClaimKey
        |,null as ProcedureID
        |,0 as NICUDays
        |,'N' as NICUFlag
        |,'N' as MultipleGestationFlag
        |,'N' as DeliveryLT39WeeksFlag
        |,ERNonEmergentFlag
        | from facoutcompressionmaster f
        |
        |
        |
        |
      """.stripMargin)

    val encounterCompressionMaster2=sparkSession.sql(
      """
        |insert into encounterCompressionMaster
        |select
        |ClaimControlID
        |,DWClaimKey
        |,ClaimLineNumber
        |,ServiceID
        |,ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,ServiceType
        |,ServiceTypeDetail
        |,ERFlag
        |,ConcurrentERFlag
        |,ServiceDate
        |,DischargeDate
        |,case when UtilizationDays<0 then -1*utilizationdays else utilizationdays end
        |,day(cast ( DayLapsePreviousDischarge as date))
        |,day( cast ( DayLagNextAdmission as date))
        |,UtilizationDWClaimKey
        |,null as ProcedureID
        |,NICUDays
        |,NICUFlag
        |,MultipleGestationFlag
        |,DeliveryLT39WeeksFlag
        |,'N' as ERNonEmergentFlag
        | from admitcompressionmaster a
        |
        |
      """.stripMargin)



    val encounterCompressionMaster3= sparkSession.sql(
      """
        |insert into encounterCompressionMaster
        |select
        |ClaimControlID
        |,DWClaimKey
        |,ClaimLineNumber
        |,ServiceID
        |,ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,ServiceType
        |,ServiceTypeDetail
        |,ERFlag
        |,ConcurrentERFlag
        |,ServiceDate
        |,DischargeDate
        |,UtilizationDays
        |,day( cast ( DayLapsePreviousDischarge as date))
        |, day (cast ( DayLagNextAdmission as date))
        |,UtilizationDWClaimKey
        |, ProcedureID
        |,0 as NICUDays
        |,'N' as NICUFlag
        |,'N' as MultipleGestationFlag
        |,'N' as DeliveryLT39WeeksFlag
        |, ERNonEmergentFlag
        |from profcompressionmaster
        |
        |
        |
      """.stripMargin)

    val encounterCompressionMaster4=sparkSession.sql(
      """insert into encounterCompressionMaster
        |select
        |ClaimControlID
        |,DWClaimKey
        |,ClaimLineNumber
        |,ServiceID
        |,ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,ServiceType
        |,ServiceTypeDetail
        |,ERFlag
        |,ConcurrentERFlag
        |,ServiceDate
        |,DischargeDate
        |,case when UtilizationDays<0 then -1*utilizationdays else utilizationdays end
        |,DayLapsePreviousDischarge
        |, DayLagNextAdmission
        |,UtilizationDWClaimKey
        |,null as ProcedureID
        |,NICUDays
        |,NICUFlag
        |,MultipleGestationFlag
        |,DeliveryLT39WeeksFlag
        |,'N' as ERNonEmergentFlag
        |from babyadmitcompressionmaster b
        |
        |
        |
        |
      """.stripMargin)



    sparkSession.sql("drop table if exists facoutcompressionmaster")
    sparkSession.sql("drop table if exists admitcompressionmaster")
    sparkSession.sql("drop table if exists profcompressionmaster")
    sparkSession.sql("drop table if exists dentalcompressionmaster")
    sparkSession.sql("drop table if exists rxcompressionmaster")
    sparkSession.sql("drop table if exists babyadmitcompressionmaster")







  } //end of def load compression master

  def load_dstfacout(): Unit = {

    sparkSession.sql("""drop table if exists dstfacout""")
    val dstfacout = List(
      (40, "Nose, Mouth & Pharynx: Tonsillectomy/Adenoidectomy"),
      (18, "Urgent Care"),
      (19, "Emergency Room"),
      (20, "Urgent Care Clinic"),
      (21, "Hemodialysis"),
      (22, "Peritoneal Dialysis"),
      (23, "Continuous Ambulatory Peritoneal Dialysis (CAPD)"),
      (24, "Continuous Cycling Peritoneal Dialysis (CCPD)"),
      (25, "Miscellaneous Dialysis"),
      (26, "Digestive: Appendectomy"),
      (27, "Digestive: Cholesystectomy"),
      (28, "Digestive: Colonoscopy"),
      (29, "Digestive: Esophagogastroduodenoscopy (EGD)"),
      (30, "Digestive: Flexible Sigmoidoscopy"),
      (31, "Digestive: Hernia Repair"),
      (32, "Digestive: Laparoscopy"),
      (33, "Digestive: Polypectomy"),
      (34, "Digestive: Hemorrhoid Procedures"),
      (35, "Digestive:  Other"),
      (36, "Cardiovascular: Cardiac Catheterization"),
      (37, "Nervous: Carpal Tunnel Release"),
      (38, "Eye: Cataract Removal"),
      (39, "Ear: Myringotomy"),
      (41, "Female Genital: Dilation & Curettage (D&C)"),
      (42, "Female Genital: Hysterectomy"),
      (43, "Musculoskeletal: Bunionectomy"),
      (44, "Musculoskeletal: Arthroscopy"),
      (45, "Integumentary: Breast Biopsy"),
      (46, "Integumentary: Lumpectomy"),
      (47, "Integumentary: Subtotal Mastectomy"),
      (48, "Integumentary: Mastectomy"),
      (49, "Integumentary: Vascular Access"),
      (50, "Nervous:  Other"),
      (51, "Endocrine System"),
      (52, "Eye:  Other"),
      (53, "Ear:  Other"),
      (54, "Nose, Mouth & Pharynx:  Other"),
      (55, "Respiratory System"),
      (56, "Cardiovascular:  Other"),
      (57, "Lymphatic System"),
      (58, "Urinary System"),
      (59, "Male Genital System"),
      (60, "Female Genital:  Other"),
      (61, "Obstetrical Procedures"),
      (62, "Musculoskeletal:  Other"),
      (63, "Integumentary: Other"),
      (64, "Operating Room Services"),
      (65, "Ambulatory Surgical Care"),
      (66, "Gastrointestinal Services"),
      (67, "Observation Room"),
      (68, "Day/Night Treatment"),
      (69, "Individual/Group/Family Therapy"),
      (70, "Electroshock Therapy"),
      (71, "Other Psychiatric"),
      (72, "Drug Rehabilitation"),
      (73, "Alcohol Rehabilitation"),
      (74, "Angiocardiography"),
      (75, "Arthrography"),
      (76, "Artheriography"),
      (77, "CT Scan"),
      (78, "MRI"),
      (79, "Diagnostic Radiology: Other"),
      (80, "Lithrotripsy (ESWL)"),
      (81, "Therapeutic Radiology"),
      (82, "Chemotherapy"),
      (83, "Nuclear Medicine"),
      (84, "Other Cardiology"),
      (85, "Stress Test"),
      (86, "Echocardiology"),
      (87, "EKG/ECG"),
      (88, "Holter Monitor"),
      (89, "Telemetry"),
      (90, "Cardiac Rehabilitation"),
      (91, "EEG"),
      (92, "Respiratory"),
      (93, "Pulmonary Function"),
      (94, "Other Imaging"),
      (95, "Laboratory"),
      (96, "Pathology"),
      (97, "Physical Therapy"),
      (98, "Occupational Therapy"),
      (99, "Speech Therapy"),
      (100, "Other Services"))

    val rdd1 = sparkSession.sparkContext.parallelize(dstfacout).map(row => Row(row._1, row._2))
    val schema1 = StructType(Array(StructField("rank", IntegerType, true), StructField("detailservicetype", StringType, true)))
    val dstfacout1 = sparkSession.createDataFrame(rdd1, schema1)
    dstfacout1.createOrReplaceTempView("dstfacout")

  }

  def load_dstprof(): Unit = {
    sparkSession.sql("drop table if exists dstprof")
    val dstprof = List(
      (168, "Evocative / Suppresion Testing  (From Mcgraw Hill)"),
      (101, "Head"),
      (102, "Neck"),
      (103, "Thorax"),
      (104, "Intrathoracic"),
      (105, "Spine & Spinal Cord"),
      (106, "Upper Abdomen"),
      (107, "Lower Abdomen"),
      (108, "Perineum"),
      (109, "Pelvis"),
      (110, "Upper Leg"),
      (111, "Knee & Popliteal Area"),
      (112, "Lower Leg"),
      (113, "Shoulder & Axilla"),
      (114, "Upper Arm & Elbow"),
      (115, "Forearm, Wrist & Hand"),
      (116, "Radiological Procedures"),
      (117, "Obstetric"),
      (118, "Miscellaneous Procedures"),
      (119, "Office Visits & Outpatient Services"),
      (120, "Office Visits & Outpatient Services"),
      (121, "Hospital Observation"),
      (122, "Hospital Inpatient Services"),
      (123, "Hospital Inpatient Services"),
      (124, "Consultations"),
      (125, "Emergency Dept Services"),
      (126, "Critical Care Services"),
      (127, "Nursing Facility Services"),
      (128, "Nursing Facility Services"),
      (129, "Domiciliary, Rest Home, or Custodial Care"),
      (130, "Home Services"),
      (131, "Home Services"),
      (132, "Prolonged Services"),
      (133, "Case Mgmt Services"),
      (134, "Care Plan Oversight Svcs"),
      (135, "Preventive Services"),
      (136, "Counseling"),
      (137, "Newborn Care"),
      (138, "Newborn Care"),
      (139, "Special E & M Services"),
      (140, "Other E & M Services"),
      (141, "Ambulance"),
      (142, "Chiropractic"),
      (143, "Med & Surg Supplies"),
      (144, "Additional Ostomy Supplies"),
      (145, "Administrative"),
      (146, "Enteral & Parentral Therapy"),
      (147, "Durable Medical Equipment"),
      (148, "Professional Services"),
      (149, "Rehab Services"),
      (150, "Drugs (Injected)"),
      (151, "Chemotherapy Drugs"),
      (152, "DME - Regional"),
      (153, "Orthotic Procedures"),
      (154, "Prosthetic Procedures"),
      (155, "HCFA Assignment of Services"),
      (156, "Laboratory Tests"),
      (157, "Temporary Codes (Q0xxx)"),
      (158, "Diagnostic Radiology"),
      (159, "Vision Services"),
      (160, "Hearing Services"),
      (161, "Speech Language Pathology"),
      (162, "Dental"),
      (163, "Locally Grown HCPCS (W-Z)"),
      (164, "Injection Codes for EPO"),
      (165, "Automated Multichannel Tests"),
      (166, "Organ Or Disease Oriented Panels"),
      (167, "Therapeutic Drug Assays & Testing"),
      (169, "Consultations"),
      (170, "Urinalysis"),
      (171, "Chemistry"),
      (172, "Hematology & Coagulation"),
      (173, "Immunology"),
      (174, "Transfusion Medicine"),
      (175, "Microbiology"),
      (176, "Anatomic Pathology"),
      (177, "Cytopathology"),
      (178, "Cytogenetic Studies"),
      (179, "Surgical Pathology"),
      (180, "Transcutaneous Procedures"),
      (181, "Other Procedures"),
      (182, "Reproductive Medicine Procedures"),
      (183, "Immune Globulins"),
      (184, "Immunization Administration"),
      (185, "Vaccines & Toxoids"),
      (186, "Therapeutic or Diagnostic Infusions"),
      (187, "Therapeutic, Prophylactic or Diag Injections"),
      (188, "Psychiatry"),
      (189, "Biofeedback"),
      (190, "Dialysis"),
      (191, "Gastroenterology"),
      (192, "Ophthalmology"),
      (193, "Special Otorhinolaryngologic Svcs"),
      (194, "Cardiovascular"),
      (195, "Non-Invasive Vascular Studies"),
      (196, "Pulmonary"),
      (197, "Allergy & Clinical Immunology"),
      (198, "Endocrinology"),
      (199, "Neurology"),
      (200, "Central Nervous System Tests"),
      (201, "Health and Behavior Assessment"),
      (202, "Chemotherapy Administration"),
      (203, "Photodynamic Therapy"),
      (204, "Special Dermatological Procedures"),
      (205, "Physical Medicine and Rehabilitation"),
      (206, "Medical Nutrition Therapy"),
      (207, "Osteopathic Manipulation"),
      (208, "Chiropractic Manipulation"),
      (209, "Special Services & Reports"),
      (210, "Qualifying Circumstances for Anesthesia"),
      (211, "Sedation With or Without Analgesia"),
      (212, "Other Medical Services and Procedures"),
      (213, "Home Health Procedures & Services"),
      (214, "Radiology: CT"),
      (215, "Radiology: MRI"),
      (216, "Radiology: MRA"),
      (217, "Radiology: Bone Density"),
      (218, "Radiology: Mammogram"),
      (219, "Radiology: Other"),
      (220, "Ultrasound"),
      (221, "Radiation Oncology"),
      (222, "Nuclear Medicine: PET"),
      (223, "Nuclear Medicine: Other"),
      (224, "General"),
      (225, "Integumentary System"),
      (226, "Musculoskeletal System"),
      (227, "Respiratory System"),
      (228, "Cardiovascular System"),
      (229, "Hemic & Lymphatic Systems"),
      (230, "Mediastinum & Diaphragm"),
      (231, "Digestive System"),
      (232, "Urinary System"),
      (233, "Male Genital System"),
      (234, "Intersex Surgery"),
      (235, "Female Genital System"),
      (236, "Maternity Care & Delivery"),
      (237, "Endocrine System"),
      (238, "Nervous System"),
      (239, "Eye & Ocular Adnexa"),
      (240, "Auditory System"),
      (241, "Operating Microscope"),
      (242, "Speech Evaluation/Prosthesis/Therapy"),
      (243, "Evaluation/Re-evaluation"),
      (244, "Modalities"),
      (245, "Orthotic Management and Prosthetic Management"),
      (246, "Osteopathic Manipulation"),
      (247, "Tests and Measurements"),
      (248, "Therapeutic Procedures"),
      (249, "Other"))

    val rdd2 = sparkSession.sparkContext.parallelize(dstprof).map(row => Row(row._1, row._2))
    val schema2 = StructType(Array(StructField("rank", IntegerType, true), StructField("detailservicetype", StringType, true)))
    val dstprof1 = sparkSession.createDataFrame(rdd2, schema2)
    dstprof1.createOrReplaceTempView("dstprof")

  }

  def load_last_seq2(): Unit = {
    sparkSession.sql("drop table if exists last_seq2")
    val last_seq2 = sparkSession.sql(
      """
        |
        |select distinct clm_id,max(dw_clm_key)over(partition by clm_id) dw_clm_key
        |from cvtencounterilhmo
        |where clm_filing_cd='01'
        |
      """.stripMargin)

    last_seq2.createOrReplaceTempView("last_seq2")

  }

  def load_last_seq_inpat(): Unit = {
    sparkSession.sql("drop table if exists last_seq_inpat")
    val last_seq_inpat_tmp = sparkSession.sql(
      """
        select l.dw_clm_key,
                c.clm_id as dw_clm_cntrl_key,
                c.incurd_dt_clm,
                c.corp_ent_cd,
                concat(c.corp_ent_Cd,databaseid,dw_alt_indivl_key,case when pat_relshp_cd='01' then 'SUB' else 'DEP' end) as mbrid,
                bllg_prov_tax_id as billingprovidertaxid,
                pat_lst_name,
                pat_fst_name,
                inpat_outpat_cd,
                rvnu_cd,
                svc_unit_cnt,
                svc_from_dt,
                svc_to_dt
        from cvtencounterilhmo c
        join groupmaster g
        on g.corpentcd=c.corp_ent_cd
        and c.acct_nbr=g.accountid
        and c.subacct_grp_id=g.groupid
        and c.subacct_sect_id=sectionid
        join
        last_seq2 l
        on c.dw_clm_key=l.dw_clm_key

      """)

    last_seq_inpat_tmp.createOrReplaceTempView("last_seq_inpat_tmp")


    val last_seq_inpat_tmp2 = sparkSession.sql(
      """
           select dw_clm_key,
                dw_clm_cntrl_key,
                incurd_dt_clm,
                corp_ent_cd,
                mbrid,
                billingprovidertaxid,
                pat_lst_name,
                pat_fst_name,
                inpat_outpat_cd,
                case when rvnu_cd between '0100' and '0179'
                      or rvnu_cd between '1000' and '1005'
                      or rvnu_cd between '0190' and '0219' or (rvnu_cd = '0224' or rvnu_cd ='0656')
                      then 1 else 0 end as pre_RBlines,
                case when rvnu_cd between '0100' and '0179'
                      or rvnu_cd between '1000' and '1005'
                      or rvnu_cd between '0190' and '0219' or (rvnu_cd = '0224' or rvnu_cd ='0656')
                      then svc_unit_cnt else 0 end as pre_RBdays,
                      case when rvnu_cd between '0170' and '0179' then cast(svc_unit_cnt as int) else 0 end as pre_nurserydays,
                      case when rvnu_cd between '0170' and '0171' then cast(svc_unit_cnt as int) else 0 end as pre_lownurserydays,
                      case when rvnu_cd = '0170' then cast(svc_unit_cnt as int) else 0 end as pre_lownurserydays170,
                      case when rvnu_cd = '0174' then cast(svc_unit_cnt as int) else 0 end as pre_NICUdays,
                 svc_from_dt,
                 svc_to_dt,
                 case when  rvnu_cd between '0100' and '0179'
                      or rvnu_cd between '1000' and '1005'
                      or rvnu_cd between '0190' and '0219' or (rvnu_cd = '0224' or rvnu_cd = '0656')
                     then svc_from_dt else cast('9999-12-31' as date) end as pre_rbfromdt,
                     case when inpat_outpat_cd='IP' then 1 else 0 end as pre_inpatlines
                 from last_seq_inpat_tmp""")

    last_seq_inpat_tmp2.createOrReplaceTempView("last_seq_inpat_tmp2")

    val last_seq_inpat_tmp3 = sparkSession.sql(
      """
        select der.*,
        case when rbdays=0 then 'Y' else 'N' end as zerodaysind
        from
        (select dw_clm_key,
         dw_clm_cntrl_key,
        incurd_dt_clm,
        corp_ent_cd,
        mbrid,
        billingprovidertaxid,
        pat_lst_name,
        pat_fst_name,sum(pre_inpatlines) as inpatlines ,
        max(inpat_outpat_cd) as inpat_outpat_cd,
        count(distinct inpat_outpat_cd) as count_inpat_outpat_cd,
        sum(pre_RBlines) as RBlines,
        sum(pre_RBdays) as RBdays,
        sum(pre_nurserydays) as nurserydays,
        sum(pre_lownurserydays) as lownurserydays,
        sum(pre_lownurserydays170) as lownurserydays170,
        sum(pre_NICUdays) as NICUdays,
        min(cast (svc_from_dt as date)) as fromdt,
        min(pre_rbfromdt) as rbfromdt,
        max(cast (svc_to_dt as date)) as todt,
        datediff(max(cast (svc_to_dt as date)),min(cast (svc_from_dt as date))) as datedays,
        count(*)as lines
        from last_seq_inpat_tmp2
        group by dw_clm_key,dw_clm_cntrl_key,incurd_dt_clm,corp_ent_cd,mbrid,billingprovidertaxid,pat_lst_name,pat_fst_name
        ) as der""")
    last_seq_inpat_tmp3.createOrReplaceTempView("last_seq_inpat")


  }

  def load_multgestdiags(): Unit = {
    sparkSession.sql("drop table if exists multgestdiags")
    val multgestdiags = List(("651"),
      ("6510"),
      ("65100"),
      ("65101"),
      ("65103"),
      ("6511"),
      ("65110"),
      ("65111"),
      ("65113"),
      ("6512"),
      ("65120"),
      ("65121"),
      ("65123"),
      ("65130"),
      ("65131"),
      ("65133"),
      ("6514"),
      ("65141"),
      ("65143"),
      ("65150"),
      ("65151"),
      ("65153"),
      ("6516"),
      ("65160"),
      ("65161"),
      ("65163"),
      ("6517"),
      ("65170"),
      ("65171"),
      ("65173"),
      ("6518"),
      ("65180"),
      ("65181"),
      ("65183"),
      ("6519"),
      ("65190"),
      ("65191"),
      ("65193"),
      ("652.6"),
      ("65260"),
      ("65261"),
      ("65263"),
      ("66050"),
      ("66051"),
      ("66053"),
      ("66230"),
      ("66231"),
      ("66233"),
      ("67810"),
      ("67811"),
      ("67813"),
      ("7594"),
      ("7615"),
      ("V272"),
      ("V273"),
      ("V274"),
      ("V31"),
      ("V310"),
      ("V3100"),
      ("V3101"),
      ("V311"),
      ("V3110 "),
      ("V3111"),
      ("V312"),
      ("V32"),
      ("V320"),
      ("V3200"),
      ("V3201"),
      ("V321"),
      ("V322"),
      ("V33"),
      ("V330"),
      ("V3300"),
      ("V3301"),
      ("V331"),
      ("V332"))
    val rdd3 = sparkSession.sparkContext.parallelize(multgestdiags).map(row => Row(row))
    val schema3 = StructType(Array(StructField("diagcd", StringType, true)))
    val multgestdiags1 = sparkSession.createDataFrame(rdd3, schema3)
    multgestdiags1.createOrReplaceTempView("multgestdiags")

  }

  def load_last_seq_inpat_multgest(): Unit = {
    sparkSession.sql("drop table if exists last_seq_inpat_multgest")
    val last_seq_inpat_multgest = sparkSession.sql(
      """
        |select distinct l.dw_clm_key
        |from last_seq_inpat l
        |inner join
        |cvtencounterilhmo c
        |on l.dw_clm_key=c.dw_clm_key
        |where diag_cd1 in (select diagcd from multgestdiags)
        |or diag_cd2 in (select diagcd from multgestdiags)
        |or diag_cd3 in (select diagcd from multgestdiags)
        |or diag_cd4 in (select diagcd from multgestdiags)
        |or diag_cd5 in (select diagcd from multgestdiags)
        |or diag_cd6 in (select diagcd from multgestdiags)
        |or diag_cd7 in (select diagcd from multgestdiags)
        |or diag_cd8 in (select diagcd from multgestdiags)
        |or diag_cd9 in (select diagcd from multgestdiags)
        |or diag_cd10 in (select diagcd from multgestdiags)
        |
        |
        |
      """.stripMargin)
    last_seq_inpat_multgest.createOrReplaceTempView("last_seq_inpat_multgest")

    sparkSession.sql("drop table if exists multgestdiags")
    sparkSession.sql("drop table if exists pretermdiags")
  }

  def load_pretermdiags(): Unit = {
    val pretermdiags = List(
      ("76510"),
      ("76511"),
      ("76512"),
      ("76513"),
      ("76514"),
      ("76515"),
      ("76516"),
      ("76517"),
      ("76518"),
      ("76519"),
      ("76500"),
      ("76501"),
      ("76502"),
      ("76503"),
      ("76504"),
      ("76505"),
      ("76506"),
      ("76507"),
      ("76508"),
      ("76509"))
    val rdd4 = sparkSession.sparkContext.parallelize(pretermdiags).map(row => Row(row))
    val schema4 = StructType(Array(StructField("diagcd", StringType, true)))
    val pretermdiags1 = sparkSession.createDataFrame(rdd4, schema4)
    pretermdiags1.createOrReplaceTempView("pretermdiags")


  }

  def load_last_seq_inpat_preterm(): Unit = {
    sparkSession.sql("drop table if exists last_seq_inpat_preterm")

    val last_seq_inpat_preterm = sparkSession.sql(
      """
        |select distinct l.dw_clm_key
        |from last_seq_inpat l
        |inner join
        |cvtencounterilhmo c
        |on l.dw_clm_key=c.dw_clm_key
        |where diag_cd1 in (select diagcd from pretermdiags)
        |or diag_cd2 in (select diagcd from pretermdiags)
        |or diag_cd3 in (select diagcd from pretermdiags)
        |or diag_cd4 in (select diagcd from pretermdiags)
        |or diag_cd5 in (select diagcd from pretermdiags)
        |or diag_cd6 in (select diagcd from pretermdiags)
        |or diag_cd7 in (select diagcd from pretermdiags)
        |or diag_cd8 in (select diagcd from pretermdiags)
        |or diag_cd9 in (select diagcd from pretermdiags)
        |or diag_cd10 in (select diagcd from pretermdiags)
      """.stripMargin)
    last_seq_inpat_preterm.createOrReplaceTempView("last_seq_inpat_preterm")

  }

  def load_last_seq_inpat_servicetype(): Unit = {
    val last_seq_inpat_servicetype = sparkSession.sql(
      """(
        |select dw_clm_key,detailservicetypelong,MultipleGestationFlag,DeliveryLT39WeeksFlag
        |from
        |(
        |select l.dw_clm_key,
        |case when m.dw_clm_key is not null then 'Y' else 'N' end as MultipleGestationFlag,
        |case when d.dw_clm_key is not null then 'Y' else 'N' end as DeliveryLT39WeeksFlag,
        |min(
        |case
        |when   RVNU_CD between  '0190' and '0199'     then   '111SNF'
        |when   SVCG_PROV_TYP_CD in ('A9', 'BM', 'BP', 'BN', 'SF', 'AX')     then   '121SNF'
        |
        |when   SVCG_PROV_TYP_CD in ('FR', 'J4', 'TJ')     then   '141Rehabilitation'
        |when   RVNU_CD in ('0118', '0128', '0138', '0148', '0158')     then   '151Rehabilitation'
        |when   RVNU_CD in ('0115', '0125', '0135', '0145', '0155', '0650', '0656', '0658', '0659')     then   '161Hospice'
        |when   SVCG_PROV_TYP_CD in ('0S', 'F8', 'J1')     then   '171Hospice'
        |when   RVNU_CD = '0655'     then   '181Respite'
        |else   '526Other' end) as detailservicetypelong
        |from last_seq_inpat l
        |inner join
        |cvtencounterilhmo c
        |on l.dw_Clm_key=c.dw_clm_key
        |
        |left outer join
        |last_seq_inpat_multgest m
        |on l.dw_clm_key=m.dw_clm_key
        |left outer join
        |last_seq_inpat_preterm d
        |on l.dw_clm_key=d.dw_clm_key
        |group by l.dw_clm_key,case when m.dw_clm_key is not null then 'Y' else 'N' end,case when d.dw_clm_key is not null then 'Y' else 'N' end) der)
      """.stripMargin)
    last_seq_inpat_servicetype.createOrReplaceTempView("last_seq_inpat_servicetype")

  }

  def load_first_claims(): Unit = {
    sparkSession.sql("drop table if exists first_claims")
    val first_claims_temp = sparkSession.sql(
      """
        |select * from last_seq_inpat where dw_clm_key not in
        |		(select dw_clm_key from last_seq_inpat
        |		where  lownurserydays=rbdays)
      """.stripMargin)
    first_claims_temp.createOrReplaceTempView("first_claims_temp")


    val first_claims = sparkSession.sql(
      """
        |
        |select dw_clm_key,dw_clm_cntrl_key,incurd_dt_clm,corp_ent_cd,mbrid,billingprovidertaxid,pat_lst_name,pat_fst_name,inpat_outpat_cd ,count_inpat_outpat_cd,RBlines,nurserydays,lownurserydays, lownurserydays170,NICUdays,fromdt, rbfromdt, todt, datedays, inpatlines,lines,zerodaysind, case when rbdays<>nurserydays and nurserydays<>0 then rbdays-nurserydays else rbdays end as rbdays from
        |
        |first_claims_temp
        |
      """.stripMargin)
    first_claims.createOrReplaceTempView("first_claims")

  }

  def load_babies(): Unit = {
    sparkSession.sql("""drop table if exists babies""")
    val babies = sparkSession.sql("select * from last_seq_inpat where dw_clm_key not  in (select dw_Clm_key from first_claims)")
    babies.createOrReplaceTempView("babies")
  }

  def load_first_claims2(): Unit ={
    sparkSession.sql("drop table if exists first_claims2")


    val first_claims2 = sparkSession.sql(
      """
        |select case
        |when rbdays=0 and datedays<>0 then '01rbdays=0 and datedays<>0'
        |when datedays=rbdays+365 then '03datedays=rbdays+365'
        |when datedays=rbdays+366 then '04datedays=rbdays+366'
        |when datedays=rbdays+100 then '05datedays=rbdays+100'
        |when datedays=rbdays+1000 then '06datedays=rbdays+1000'
        |when datedays=rbdays+10000 then '07datedays=rbdays+10000'
        |
        |when rbdays=datedays then '11rbdays=datedays'
        |when rbdays=datedays+1 then '12rbdays=datedays+1'
        |when rbdays+1=datedays then '21rbdays+1=datedays'
        |
        |else '?' end as class,
        |case
        |when rbdays=0 and datedays<>0 then datedays
        |when datedays=rbdays+365 then rbdays
        |when datedays=rbdays+366 then rbdays
        |when datedays+100=rbdays then datedays
        |when datedays+1000=rbdays then datedays
        |when datedays+10000=rbdays then datedays
        |
        |when rbdays=datedays then rbdays
        |when rbdays=datedays+1 then rbdays
        |when rbdays+1=datedays then rbdays
        |
        |else rbdays end as LOS,
        |
        |		f.*
        |from first_claims f
        |
      """.stripMargin)
    first_claims2.createOrReplaceTempView("first_claims2")
    first_claims2.show(5)
  }

  def load_adm1(): Unit ={
    sparkSession.sql("""drop table if exists adm1""")

    val adm1=sparkSession.sql(
      """
        |(select f.*,
        |
        |fromdt as startdt,
        |date_add(fromdt,los) as enddt,
        |
        |sum(1) over (order by mbrid,billingprovidertaxid,fromdt asc rows unbounded preceding) as rowid
        |from first_claims2 f)
        |
      """.stripMargin)
    adm1.createOrReplaceTempView("adm1")
    adm1.show(5)

  }

  def load_spans(): Unit ={
    sparkSession.sql("drop table if exists spans")

    val spans = sparkSession.sql(
      """
        |select der.*,sum(case when startdt>maxend then 1 else 0 end) over (partition by mbrid,billingprovidertaxid
        |				order by startdt asc, enddt desc, rowid asc rows unbounded preceding)as spannum
        |from
        |	(
        |	select a.*,
        |	max(enddt) over (partition by mbrid,billingprovidertaxid order by startdt asc, enddt desc, rowid asc
        |					rows  between unbounded preceding and 1 preceding)as maxend
        |	from adm1 a)der
      """.stripMargin)

    spans.createOrReplaceTempView("spans")


  }

  def load_mergeprovadms(): Unit ={
                        sparkSession.sql("drop table if exists mergeprovadms")

                        val mergeprovadms=sparkSession.sql(
                          """
                            |select mbrid,billingprovidertaxid,spannum,0 as numembedded,1 as numprovs,idoffset,
                            |				sum(NICUdays) NICUdays,
                            |				count(*) as numclms,idoffset + min(rowid)as admid,min(startdt) as startdt,max(enddt) as enddt,
                            |				sum(los) as sumlos, datediff(max(enddt),min(startdt)) as los
                            |from spans
                            |inner join
                            |(select max(serviceid) idoffset from compressionmaster)der
                            |on 1=1
                            |group by mbrid,billingprovidertaxid,spannum,idoffset,'0','1'
                            |
                          """.stripMargin)
                        mergeprovadms.createOrReplaceTempView("mergeprovadms")

                      }

  def load_admxclm()={

                      sparkSession.sql("drop table if exists admxclm")

                      val admxclm= sparkSession.sql(
                        """
                          |select admid,s.*,
                          |max(multiplegestationflag) over (partition by admid) multiplegestationflag,
                          |max(DeliveryLT39WeeksFlag) over (partition by admid) DeliveryLT39WeeksFlag,
                          |min(detailservicetypelong) over (partition by admid) detailservicetypelong,
                          |case when substring(detailservicetypelong,3,1) ='1' then 'Non-Acute'
                          |when substring(detailservicetypelong,3,1) ='2' then 'Maternity'
                          |when substring(detailservicetypelong,3,1) ='3' then 'Mental Health/Substance Abuse'
                          |when substring(detailservicetypelong,3,1) ='4' then 'Surgical'
                          |when substring(detailservicetypelong,3,1) ='5' then 'Medical'
                          |else 'OTHER' end as ServiceType,
                          |substring(detailservicetypelong,4,length(detailservicetypelong)-3) as DetailServiceType
                          |from
                          |spans s
                          |inner join
                          |last_seq_inpat_servicetype ss
                          |on s.dw_clm_key=ss.dw_clm_key
                          |inner join
                          |mergeprovadms m
                          |on s.mbrid=m.mbrid
                          |and s.billingprovidertaxid=m.billingprovidertaxid
                          |and s.spannum=m.spannum
                          |
                          |
                          |
                        """.stripMargin)
                      admxclm.createOrReplaceTempView("admxclm")
                     admxclm.show(5)
                    }

  def load_temp()={

                        sparkSession.sql("drop table if exists temp")

                        val temp= sparkSession.sql(
                          """
                            |select der.*,case when startdt>=minstart and enddt<=maxend then 'Y' else 'N' end as embedflag
                            |from
                            |(
                            |select m.*,min(startdt) over (partition by mbrid order by startdt asc, enddt desc , numclms desc, admid desc
                            |									rows between unbounded preceding and 1 preceding) minstart
                            |,max(enddt) over (partition by mbrid order by startdt asc, enddt desc , numclms desc, admid desc
                            |									rows between unbounded preceding and 1 preceding) maxend
                            |,max(admid) over (partition by mbrid order by startdt asc, enddt desc , numclms desc, admid desc
                            |									rows between unbounded preceding and 1 preceding) prevadmid
                            |,sum(1) over (partition by mbrid order by startdt asc, enddt desc ,numclms desc, admid desc
                            |									rows unbounded preceding) rowid
                            |from mergeprovadms m)
                            |der
                          """.stripMargin)
                        temp.createOrReplaceTempView("temp")
                         temp.show(5)



                      }

  def load_embeddedmergeprovadms()={
                      sparkSession.sql("drop table if exists embeddedmergeprovadms")
                      val embeddedmergeprovadms= sparkSession.sql("select * from mergeprovadms where admid in (select admid from temp where embedflag='Y')")
                      embeddedmergeprovadms.createOrReplaceTempView("embeddedmergeprovadms")
    embeddedmergeprovadms.show(5)

                    }

  def load_embeddedadmxclm()={

                        sparkSession.sql("drop table if exists embeddedadmxclm ")
                        val embeddedadmxclm = sparkSession.sql(" select * from admxclm where admid in (select admid from temp where embedflag='Y')")
                        embeddedadmxclm.createOrReplaceTempView("embeddedadmxclm")
                        embeddedadmxclm.show(5)
                      }

  def delete_mergeprovadms()={

                      sparkSession.sql("select * from mergeprovadms where admid not in (select admid from temp where embedflag='Y')").createOrReplaceTempView("mergeprovadms")
                    }
  def delete_admxclm()={
                      sparkSession.sql("select * from admxclm where admid not in (select admid from temp where embedflag='Y') ").createOrReplaceTempView("admxclm")
                    }

  def load_preadmits()={


    sparkSession.sql("drop table if exists preadmits")
    val preadmits=sparkSession.sql(
      """
        |
        |select
        |mbrid
        |,billingprovidertaxid
        |,spannum
        |,numembedded
        |,numprovs
        |,numclms
        |,admid
        |,case when prevend>startdt then prevend else startdt end startdt
        |,enddt
        |,NICUdays
        |,sumlos
        |,los
        |
        |,startdt as origstartdt
        |from
        |(select
        |m.*,
        |max(enddt)over (partition by mbrid order by startdt asc, enddt desc rows between 1 preceding and 1 preceding) as prevend
        |from mergeprovadms m) der
      """.stripMargin)
    preadmits.createOrReplaceTempView("preadmits")
    preadmits.show(5)

  }

  def load_first_claims2_3()={


    sparkSession.sql("drop table if exists first_claims2")

    val first_claims2_3= sparkSession.sql(
      """
        |select case
        |when rbdays=0 and datedays<>0 then '01rbdays=0 and datedays<>0'
        |when datedays=rbdays+365 then '03datedays=rbdays+365'
        |when datedays=rbdays+366 then '04datedays=rbdays+366'
        |when datedays=rbdays+100 then '05datedays=rbdays+100'
        |when datedays=rbdays+1000 then '06datedays=rbdays+1000'
        |when datedays=rbdays+10000 then '07datedays=rbdays+10000'
        |when rbdays=datedays then '11rbdays=datedays'
        |when rbdays=datedays+1 then '12rbdays=datedays+1'
        |when rbdays+1=datedays then '21rbdays+1=datedays'
        |else '?' end as class,
        |case
        |when rbdays=0 and datedays<>0 then datedays
        |when datedays=rbdays+365 then rbdays
        |when datedays=rbdays+366 then rbdays
        |when datedays+100=rbdays then datedays
        |when datedays+1000=rbdays then datedays
        |when datedays+10000=rbdays then datedays
        |
        |when rbdays=datedays then rbdays
        |when rbdays=datedays+1 then rbdays
        |when rbdays+1=datedays then rbdays
        |else rbdays end as LOS,
        |
        |		f.*
        |from babies f
        |
        |
      """.stripMargin)


    first_claims2_3.createOrReplaceTempView("first_claims2")
    first_claims2_3.show(5)
  }

  def load_adm1_1()={

    val adm1_1= sparkSession.sql(
      """
        |select f.*,fromdt as startdt, date_add(fromdt,los) as enddt,
        |maxadmid+sum(1) over (order by mbrid,billingprovidertaxid,fromdt asc rows unbounded preceding) as rowid
        |from first_claims2 f
        |inner join (select max(admid) as maxadmid from preadmits)der
        |on 1=1
      """.stripMargin)
    adm1_1.createOrReplaceTempView("adm1")
    adm1_1.show(5)
  }

  def load_spans_1()={

    sparkSession.sql("drop table if exists spans")

    val spans_1 =sparkSession.sql(
      """
        |select der.*,sum(case when startdt>maxend then 1 else 0 end) over (partition by mbrid,billingprovidertaxid
        |				order by startdt asc, enddt desc, rowid asc rows unbounded preceding)as spannum
        |from
        |(
        |select a.*,
        |max(enddt) over (partition by mbrid,billingprovidertaxid order by startdt asc, enddt desc, rowid asc
        |					rows  between unbounded preceding and 1 preceding)as maxend
        |from adm1 a
        |)der
        |
      """.stripMargin)
    spans_1.createOrReplaceTempView("spans")
    spans_1.show(5)

  }

  def load_babymergeprovadms()={

    sparkSession.sql("drop table if exists babymergeprovadms")
    val babymergeprovadms=sparkSession.sql(
      """
        |select der.*
        |,los as reportinglos
        |from(
        |select mbrid,billingprovidertaxid,spannum,0 as numembedded,1 as numprovs,
        |				count(*) as numclms,min(rowid)as admid,min(startdt) as startdt,max(enddt) as enddt,
        |				sum(los) as sumlos,datediff(max(enddt),min(startdt) )as los
        |from spans
        |group by mbrid,billingprovidertaxid,spannum)der
        |
        |
      """.stripMargin)
    babymergeprovadms.createOrReplaceTempView("babymergeprovadms")
    babymergeprovadms.show(5)


  }

  def load_babyadmxclm()={
    sparkSession.sql("drop table if exists babyadmxclm")
    val babyadmxclm= sparkSession.sql(
      """
        |select admid,s.*,
        |max(multiplegestationflag) over (partition by admid) multiplegestationflag,
        |max(DeliveryLT39WeeksFlag) over (partition by admid) DeliveryLT39WeeksFlag,
        |min(detailservicetypelong) over (partition by admid) detailservicetypelong,
        |case when substring(detailservicetypelong,3,1) ='1' then 'Non-Acute'
        |when substring(detailservicetypelong,3,1) ='2' then 'Maternity'
        |when substring(detailservicetypelong,3,1) ='3' then 'Mental Health/Substance Abuse'
        |when substring(detailservicetypelong,3,1) ='4' then 'Surgical'
        |when substring(detailservicetypelong,3,1) ='5' then 'Medical'
        |else 'OTHER' end as ServiceType,
        |substring(detailservicetypelong,4,length(detailservicetypelong)-3) as DetailServiceType
        |from
        |spans s
        |inner join
        |last_seq_inpat_servicetype sparkSession
        |on s.dw_clm_key=sparkSession.dw_clm_key
        |inner join
        |babymergeprovadms m
        |on s.mbrid=m.mbrid
        |and s.billingprovidertaxid=m.billingprovidertaxid
        |and s.spannum=m.spannum
        |
        |
        |
        |
      """.stripMargin)
   babyadmxclm.createOrReplaceTempView("babyadmxclm")




  }

  def load_local_hcsc_icd_map()={

    sparkSession.sql("drop table if exists local_hcsc_icd_map")
    val local_hcsc_icd_map =sparkSession.sql(
      """
        |select icd10code,max(icd9code) as icd9code
        |from _cvticdmap
        |group by icd10code
        |
      """.stripMargin)
    local_hcsc_icd_map.createOrReplaceTempView("local_hcsc_icd_map")
    local_hcsc_icd_map.show(5)

  }


  def load_pre_facout_claims()={


    sparkSession.sql("drop table if exists pre_facout_claims")


    val pre_facout_claims= sparkSession.sql(
      """
        |select c.rowid,c.dw_clm_key,li_num,inpat_outpat_cd,c.clm_id dw_clm_cntrl_key
        |,rvnu_cd,coalesce(m.icd9code,ICD_PROC_CD_1) as ICD_PROC_CD_1,
        |concat(c.corp_ent_cd,databaseid,dw_alt_indivl_key,case when pat_relshp_cd='01' then 'SUB' else 'DEP' end) as mbrid,
        |bllg_prov_tax_id as billingprovidertaxid,
        |cast(svc_from_Dt as date) as servicedate,
        |case when m.icd9code is not null then 'Y' else 'N' end as mappedicdflag
        |from cvtencounterilhmo c
        |inner join groupmaster g
        |on g.corpentcd=c.corp_ent_cd
        |and c.acct_nbr=g.accountid
        |and c.subacct_grp_id=g.groupid
        |and c.subacct_sect_id=sectionid
        |inner join
        |last_seq2 l
        |on c.dw_clm_key=l.dw_clm_key
        |
        |left outer join
        |local_hcsc_icd_map m
        |on c.icd_proc_cd_1=m.icd10code
        |and '11'='10'
        |
        |
        |
        |where c.dw_clm_key not in (select dw_clm_key from admxclm)
        |and c.dw_clm_key not in (select dw_clm_key from embeddedadmxclm)
        |and c.dw_clm_key not in (select dw_clm_key from babyadmxclm)
        |and c.disp_cd_cli='A'
        |
        |
        |
      """.stripMargin)
    pre_facout_claims.createOrReplaceTempView("pre_facout_claims")
    pre_facout_claims.show(5)

  }


  def load_facout_claims()={

    sparkSession.sql("drop table if exists facout_claims")

    val facout_claims= sparkSession.sql(
      """
        |
        |select der.*,substring(detailservicetypelong,2,length(detailservicetypelong)-1) as detailservicetype,
        |case when substring(detailservicetypelong,1,1)='0' then 'Surgical'
        |when substring(detailservicetypelong,1,1)='1' then 'Dialysis'
        |when substring(detailservicetypelong,1,1)='2' then 'Emergency Room'
        |when substring(detailservicetypelong,1,1)='3' then 'Laboratory'
        |when substring(detailservicetypelong,1,1)='4' then 'Medical Specialties Services'
        |when substring(detailservicetypelong,1,1)='5' then 'Observation Room'
        |when substring(detailservicetypelong,1,1)='6' then 'Other Services'
        |when substring(detailservicetypelong,1,1)='7' then 'Psychiatric'
        |when substring(detailservicetypelong,1,1)='8' then 'PT/OT/ST'
        |when substring(detailservicetypelong,1,1)='9' then 'Radiology'
        |else 'Other Services' end as ServiceType
        |from
        |(
        |select p.*,
        |case when    RVNU_CD between '0840' and '0849' then      '1Continuous Ambulatory Peritoneal Dialysis (CAPD)'
        |when    RVNU_CD between  '0850' and '0859' then      '1Continuous Cycling Peritoneal Dialysis (CCPD)'
        |when    RVNU_CD between '0820' and '0829' then      '1Hemodialysis'
        |when    RVNU_CD between '0880' and '0889' then      '1Miscellaneous Dialysis'
        |when    RVNU_CD between '0830' and '0839' then      '1Peritoneal Dialysis'
        |when    RVNU_CD between '0450' and '0455'
        |	or rvnu_cd between '0457' and '0459' or rvnu_cd= '0981' then      '2Emergency Room'
        |when    RVNU_CD = '0456' then      '2Urgent Care'
        |when    RVNU_CD between '0300' and '0309' then      '3Laboratory'
        |when    RVNU_CD between '0310' and '0319' then      '3Pathology'
        |when    RVNU_CD = '0943' then      '4Cardiac Rehabilitation'
        |when    RVNU_CD in( '0331', '0332', '0335') then      '4Chemotherapy'
        |when    RVNU_CD = '0483' then      '4Echocardiology'
        |when    RVNU_CD between '0740' and '0749' then      '4EEG'
        |when    RVNU_CD = '0730' or rvnu_cd between '0733' and '0739' then      '4EKG/ECG'
        |when    RVNU_CD = '0731' then      '4Holter Monitor'
        |when    RVNU_CD = '0480' or rvnu_cd between '0484' and '0489' then      '4Other Cardiology'
        |when    RVNU_CD between '0460' and '0469' then      '4Pulmonary Function'
        |when    RVNU_CD between '0410' and '0419' then      '4Respiratory'
        |when    RVNU_CD = '0482' then      '4Stress Test'
        |when    RVNU_CD = '0732' then      '4Telemetry'
        |when    RVNU_CD between '0760' and '0769' then      '5Observation Room'
        |when    RVNU_CD in ('0516', '0526') then      '6Urgent Care Clinic'
        |when    RVNU_CD = '0945' then      '7Alcohol Rehabilitation'
        |when    RVNU_CD between '0912' and '0913' then      '7Day/Night Treatment'
        |when    RVNU_CD = '0944' then      '7Drug Rehabilitation'
        |when    RVNU_CD = '0901' then      '7Electroshock Therapy'
        |when    RVNU_CD between '0914' and '0916' then      '7Individual/Group/Family Therapy'
        |when    RVNU_CD = '0900' or rvnu_cd between '0902' and '0911' then      '7Other Psychiatric'
        |when    RVNU_CD between '0917' and '0919' then      '7Other Psychiatric'
        |when    ICD_PROC_CD_1 between '9376' and '9378' or icd_proc_cd_1 between '9380' and '9389' then      '8Occupational Therapy'
        |when    RVNU_CD between '0430' and '0439' or rvnu_cd= '0978' then      '8Occupational Therapy'
        |when    ICD_PROC_CD_1 between '930' and' 934' or icd_proc_cd_1 between '9301' and '9346'
        |				or icd_proc_cd_1 = '936' or icd_proc_cd_1 between '9361' and '9367' then      '8Physical Therapy'
        |when    RVNU_CD between '0420'  and  '0429' or rvnu_cd= '0977' then      '8Physical Therapy'
        |when    ICD_PROC_CD_1 = '937' or icd_proc_cd_1 between '9371' and '9375' then      '8Speech Therapy'
        |when    RVNU_CD between '0440'  and  '0449' or rvnu_cd= '0979' then      '8Speech Therapy'
        |when    RVNU_CD = '0321' then      '9Angiocardiography'
        |when    RVNU_CD = '0323' then      '9Artheriography'
        |when    RVNU_CD = '0322' then      '9Arthrography'
        |when    ICD_PROC_CD_1  in ('8703', '8741', '8771', '8801', '8838') then      '9CT Scan'
        |when    RVNU_CD between '0350'  and  '0359' then      '9CT Scan'
        |when    ICD_PROC_CD_1 between '87' and '89' then      '9Diagnostic Radiology: Other'
        |when    RVNU_CD in ('0320','0972') or rvnu_cd between '0324'  and  '0329' then      '9Diagnostic Radiology: Other'
        |when    ICD_PROC_CD_1 = '985' then      '9Lithrotripsy (ESWL)'
        |when    RVNU_CD between '0790'  and  '0799' then      '9Lithrotripsy (ESWL)'
        |when    ICD_PROC_CD_1 between '8891' and '8897' then      '9MRI'
        |when    RVNU_CD between '0610'  and  '0619' then      '9MRI'
        |when    RVNU_CD between '0340'  and  '0349' or rvnu_Cd= '0974' then      '9Nuclear Medicine'
        |when    RVNU_CD between '0400'  and  '0409' then      '9Other Imaging'
        |when    RVNU_CD  in ( '0330', '0333', '0339', '0973') then      '9Therapeutic Radiology'
        |when    RVNU_CD between '0490'  and  '0499' then      '0Ambulatory Surgical Care'
        |when    ICD_PROC_CD_1 between '35' and '39' and icd_proc_cd_1<> '3899' then      '0Cardiovascular:  Other'
        |when    ICD_PROC_CD_1 between '3721' and '3723' then      '0Cardiovascular: Cardiac Catheterization'
        |when    RVNU_CD = '0481' then      '0Cardiovascular: Cardiac Catheterization'
        |when    ICD_PROC_CD_1 between '42' and '54' then      '0Digestive:  Other'
        |when    ICD_PROC_CD_1 between '4701' and '472' then      '0Digestive: Appendectomy'
        |when    ICD_PROC_CD_1 between '470' and '472' then      '0Digestive: Appendectomy'
        |when    ICD_PROC_CD_1 = '512' then      '0Digestive: Cholesystectomy'
        |when    ICD_PROC_CD_1 between '5121' and '5124' then      '0Digestive: Cholesystectomy'
        |when    ICD_PROC_CD_1 = '4523' then      '0Digestive: Colonoscopy'
        |when    ICD_PROC_CD_1 = '4516' then      '0Digestive: Esophagogastroduodenoscopy (EGD)'
        |when    ICD_PROC_CD_1 = '4524' then      '0Digestive: Flexible Sigmoidoscopy'
        |when    ICD_PROC_CD_1 between '4941' and '4949' then      '0Digestive: Hemorrhoid Procedures'
        |when    ICD_PROC_CD_1 = '53' then      '0Digestive: Hernia Repair'
        |when    ICD_PROC_CD_1 between '5300' and '539' then      '0Digestive: Hernia Repair'
        |when    ICD_PROC_CD_1 = '5421' then      '0Digestive: Laparoscopy'
        |when    ICD_PROC_CD_1 = '4542' then      '0Digestive: Polypectomy'
        |when    ICD_PROC_CD_1 between '18' and '20' then      '0Ear:  Other'
        |when    ICD_PROC_CD_1 = '200' then      '0Ear: Myringotomy'
        |when    ICD_PROC_CD_1 between '2001' and '2009' then      '0Ear: Myringotomy'
        |when    ICD_PROC_CD_1 between '06' and '07' then      '0Endocrine System'
        |when    ICD_PROC_CD_1 between '08' and '16' then      '0Eye:  Other'
        |when    ICD_PROC_CD_1 between '1311' and '1364' then      '0Eye: Cataract Removal'
        |when    ICD_PROC_CD_1 between '131' and '136' then      '0Eye: Cataract Removal'
        |when    ICD_PROC_CD_1 between '65' and '70' then      '0Female Genital:  Other'
        |when    ICD_PROC_CD_1 = '690' then      '0Female Genital: Dilation & Curettage (D&C)'
        |when    ICD_PROC_CD_1 between '6901' and '6909' then      '0Female Genital: Dilation & Curettage (D&C)'
        |when    ICD_PROC_CD_1 between '683' and '6859' then      '0Female Genital: Hysterectomy'
        |when    RVNU_CD between '0750'  and  '0759' then      '0Gastrointestinal Services'
        |when    ICD_PROC_CD_1 between '8511' and '8512' then      '0Integumentary: Breast Biopsy'
        |when    ICD_PROC_CD_1 = '8521' then      '0Integumentary: Lumpectomy'
        |when    ICD_PROC_CD_1 between '8541' and '8549' then      '0Integumentary: Mastectomy'
        |when    ICD_PROC_CD_1 between '85' and '86' then      '0Integumentary: Other'
        |when    ICD_PROC_CD_1 = '8523' then      '0Integumentary: Subtotal Mastectomy'
        |when    ICD_PROC_CD_1 = '8607' then      '0Integumentary: Vascular Access'
        |when    ICD_PROC_CD_1 between '40' and '41' then      '0Lymphatic System'
        |when    ICD_PROC_CD_1 between '60' and '64' then      '0Male Genital System'
        |when    ICD_PROC_CD_1 between '76' and '84' then      '0Musculoskeletal:  Other'
        |when    ICD_PROC_CD_1 between '8020' and '8029' then      '0Musculoskeletal: Arthroscopy'
        |when    ICD_PROC_CD_1 between '7751' and '7754' then      '0Musculoskeletal: Bunionectomy'
        |when    ICD_PROC_CD_1 between '01' and '05' then      '0Nervous:  Other'
        |when    ICD_PROC_CD_1 = '0043' then      '0Nervous: Carpal Tunnel Release'
        |when    ICD_PROC_CD_1 between '21' and '29' then      '0Nose, Mouth & Pharynx:  Other'
        |when    ICD_PROC_CD_1 between '282' and '283' or icd_proc_cd_1= '286' then      '0Nose, Mouth & Pharynx: Tonsillectomy/Adenoidectomy'
        |when    ICD_PROC_CD_1 between '72' and '75' then      '0Obstetrical Procedures'
        |when    RVNU_CD between '0360'  and  '0369' then      '0Operating Room Services'
        |when    ICD_PROC_CD_1 between '30' and '34' then      '0Respiratory System'
        |when    ICD_PROC_CD_1 between '55' and '59' then      '0Urinary System'
        |else     '6Other Services' end as detailservicetypelong
        |from pre_facout_claims p)der
        |
      """.stripMargin).withColumnRenamed("RVNU_CD","RVNU_CDS").withColumnRenamed("RVNU_CDS","RVNU_CD")

     facout_claims.createOrReplaceTempView("facout_claims")
    facout_claims.show(5)

  }


  def load_prefacvisits()={


    sparkSession.sql("drop table if exists prefacvisits")

    val prefacvisits=sparkSession.sql(
      """
        |select mbrid,billingprovidertaxid,idoffset,servicedate,erflag,
        |case when erflag='Y' and ernonemergentflag='Y' then 'Y' else 'N' end as ERnonEmergentFlag,
        |idoffset+sum(1) over (order by mbrid,servicedate rows unbounded preceding) as facvisitid
        |from
        |(
        |select mbrid,
        |billingprovidertaxid,idoffset,
        | servicedate,
        |max(case when substring(rvnu_cd,2,2)='45' then 'Y' else 'N' end) as erflag,
        |--max(case when prvsn_cd = 'ERSO' then 'Y' else 'N' end) as ernonemergentflag
        |'N'as ernonemergentflag
        |from facout_claims c
        |inner join
        |(select max(admid) as idoffset from babymergeprovadms) der2
        |on 1=1
        |group by mbrid,billingprovidertaxid,idoffset,servicedate
        |)der
        |
      """.stripMargin)
    prefacvisits.createOrReplaceTempView("prefacvisits")
    prefacvisits.show(5)



  }

  def load_visitservicetypelong()={


  sparkSession.sql("drop table if exists visitservicetypelong")
  val visitservicetypelong= sparkSession.sql(
    """
      |
      |select der.* from
      |(select facvisitid,c.detailservicetypelong,sum(1) over
      |		(partition by facvisitid order by rank asc,
      |		case when substring(c.detailservicetypelong,1,1)='0' then  30 /*'Surgical'*/
      |when substring(c.detailservicetypelong,1,1)='1' then 20 /*'Dialysis'*/
      |when substring(c.detailservicetypelong,1,1)='2'  then 10 /*'Emergency Room'*/
      |when substring(c.detailservicetypelong,1,1)='3' then 80  /* 'Laboratory'*/
      |when substring(c.detailservicetypelong,1,1)='4' then 70 /*'Medical Specialties Services'*/
      |when substring(c.detailservicetypelong,1,1)='5' then 40 /*'Observation Room'*/
      |when substring(c.detailservicetypelong,1,1)='6' then 99  /*'Other Services'*/
      |when substring(c.detailservicetypelong,1,1)='7' then 50 /*'Psychiatric'*/
      |when substring(c.detailservicetypelong,1,1)='8' then 90 /*'PT/OT/ST'*/
      |when substring(c.detailservicetypelong,1,1)='9' then 60 /*'Radiology'*/
      |else 99 end asc, rank asc rows unbounded preceding) recnum
      |from facout_claims c
      |inner join
      |prefacvisits p
      |on c.mbrid=p.mbrid
      |and c.billingprovidertaxid=p.billingprovidertaxid
      |and c.servicedate=p.servicedate
      |left outer join
      |dstfacout x
      |on trim(c.detailservicetype)=trim(x.detailservicetype)
      |) der
      |where recnum=1
    """.stripMargin).withColumnRenamed("detailservicetypelong","detailservicetypelongs").withColumnRenamed("detailservicetypelongs","detailservicetypelong")


  visitservicetypelong.createOrReplaceTempView("visitservicetypelong")
    visitservicetypelong.show(5)


//    val test=sparkSession.read.parquet("D:\\workspace\\EncounterCompressionMaster\\EncounterCompressionMaster\\save_debug_time\\visitservicetypelong")
//    test.createOrReplaceTempView("visitservicetypelong1")
}


  def load_facvisitsxclm()={
    sparkSession.sql("drop table if exists facvisitsxclm")

    val facvisitsxclm=sparkSession.sql(
      """
        |
        |select rowid,dw_clm_key,li_num,p.facvisitid,dw_clm_cntrl_key,servicetype,
        |v.detailservicetypelong,detailservicetype from facout_claims c
        |inner join
        |prefacvisits p
        |on c.mbrid=p.mbrid
        |and c.billingprovidertaxid=p.billingprovidertaxid
        |and c.servicedate=p.servicedate
        |inner join
        |visitservicetypelong v
        |on p.facvisitid=v.facvisitid
      """.stripMargin)

    facvisitsxclm.createOrReplaceTempView("facvisitsxclm")
    facvisitsxclm.show(5)
  }

  def load_facvisitlapse()={


    sparkSession.sql("drop table if exists facvisitlapse")

    val facvisitlapse=sparkSession.sql(
      """|
        |select der.*,datediff(servicedate,previousdischarge) as daylapsepreviousdischarge
        |from
        |(select p.*,a.enddt as previousdischarge,
        |		sum(1) over (partition by facvisitid order by a.enddt desc rows unbounded preceding) as seqnum
        |from prefacvisits p
        |inner join
        |preadmits a
        |on p.mbrid=a.mbrid
        |and a.enddt<=p.servicedate)der
        |where seqnum=1
        |
      """.stripMargin)
    facvisitlapse.createOrReplaceTempView("facvisitlapse_temp")


    val insert_facvisitlapse=sparkSession.sql(
      """
        |select p.*,null as previousdischarge,0 as seqnum,null as dayslapsepreviousdischarge
        |from prefacvisits p where facvisitid not in (select facvisitid from facvisitlapse_temp)
      """.stripMargin)


    val facvisitlapse_1 =facvisitlapse.union(insert_facvisitlapse)
    facvisitlapse_1.createOrReplaceTempView("facvisitlapse")
    facvisitlapse_1.show(5)


  sparkSession.sql("drop table if exists facvisitlapse_temp")



  }

  def load_prefacvisits_1()={

    sparkSession.sql("drop table if exists prefacvisits")

    val prefacvisits_1 = sparkSession.sql(
      """
        |select der.*,datediff(nextadmit,servicedate) as daylagenextadmit
        |from
        |(select p.*,a.startdt as nextadmit,
        |		sum(1) over (partition by facvisitid order by a.startdt asc rows unbounded preceding) as seqnum2
        |from facvisitlapse p
        |inner join
        |preadmits a
        |on p.mbrid=a.mbrid
        |and p.servicedate<=a.startdt)der
        |where seqnum2=1
        |
      """.stripMargin)
    prefacvisits_1.createOrReplaceTempView("prefacvisits_temp")
    prefacvisits_1.show(5)


    val prefacvisits_1_insert =sparkSession.sql(
      """
        |
        |select p.*,null as nextadmit,0 as seqnum2,null as dayslagnextadmit
        |from facvisitlapse p where facvisitid not in (select facvisitid from prefacvisits_temp)
        |
      """.stripMargin)

    val prefacvisits_1_afterinsert= prefacvisits_1.union(prefacvisits_1_insert)
    prefacvisits_1_afterinsert.createOrReplaceTempView("prefacvisits")
    sparkSession.sql("drop table if exists prefacvisits_temp")
  }

  def load_facvisitservicetype()={


    sparkSession.sql("drop table if exists facvisitservicetype")
    val facvisitservicetype=sparkSession.sql(
      """
        |
        |select distinct facvisitid,detailservicetypelong
        |from facvisitsxclm
        |
      """.stripMargin)

    facvisitservicetype.createOrReplaceTempView("facvisitservicetype")
    facvisitservicetype.show(5)

  }

  def load_facvisits()={

    sparkSession.sql("drop table if exists facvisits")



    val facvisits = sparkSession.sql(
      """
        |
        |select p.*,substring(detailservicetypelong,2,length(detailservicetypelong)-1) as detailservicetype,
        |case when substring(detailservicetypelong,1,1)='0' then 'Surgical'
        |when substring(detailservicetypelong,1,1)='1' then 'Dialysis'
        |when substring(detailservicetypelong,1,1)='2' then 'Emergency Room'
        |when substring(detailservicetypelong,1,1)='3' then 'Laboratory'
        |when substring(detailservicetypelong,1,1)='4' then 'Medical Specialties Services'
        |when substring(detailservicetypelong,1,1)='5' then 'Observation Room'
        |when substring(detailservicetypelong,1,1)='6' then 'Other Services'
        |when substring(detailservicetypelong,1,1)='7' then 'Psychiatric'
        |when substring(detailservicetypelong,1,1)='8' then 'PT/OT/ST'
        |when substring(detailservicetypelong,1,1)='9' then 'Radiology'
        |else 'Other Services' end as ServiceType
        |from prefacvisits p
        |inner join
        |facvisitservicetype pp
        |on p.facvisitid=pp.facvisitid
        |
        |
      """.stripMargin)
    facvisits.createOrReplaceTempView("facvisits")
    facvisits.show(5)

  }


  def load_last_seq2_1()={


    sparkSession.sql("drop table if exists last_seq2")


    val last_seq2_1= sparkSession.sql(
      """
        |select distinct clm_id,max(dw_clm_key)over(partition by clm_id) dw_clm_key
        |from cvtencounterilhmo
        |where clm_filing_cd='02'
      """.stripMargin)
    last_seq2_1.createOrReplaceTempView("last_seq2")
    last_seq2_1.show(5)

  }


  def load_prof_claims_1()={

    sparkSession.sql("drop table if exists prof_claims_1")


    val prof_claims_1= sparkSession.sql(
      """
        |
        |
        |select c.rowid,c.dw_clm_key,li_num,c.corp_ent_Cd,inpat_outpat_cd, c.clm_id dw_clm_cntrl_key,
        |hcpcs_cpt_cd,hcpcs_cpt_modr_cd_1,
        |plc_of_trmnt_cd,
        |case when  HCPCS_CPT_CD  between  '00300'  and  '00352' then  '1 Neck'
        |when HCPCS_CPT_CD  between  '00400'  and  '00474'  then  '1 Thorax'
        |when  HCPCS_CPT_CD  between  '00500'  and  '00580'  then  '1 Intrathoracic'
        |when  HCPCS_CPT_CD  between  '00600'  and  '00670'  then  '1 Spine & Spinal Cord'
        |when  HCPCS_CPT_CD  between  '00700'  and  '00797'  then  '1 Upper Abdomen'
        |when  HCPCS_CPT_CD  between  '00800'  and  '00882'  then  '1 Lower Abdomen'
        |when  HCPCS_CPT_CD  between  '00902'  and  '00952'  then  '1 Perineum'
        |when  HCPCS_CPT_CD  between '00100'  and  '00222'  then  '1 Head'
        |when  HCPCS_CPT_CD  between  '99201'  and  '99215'  then  '2 Office Visits & Outpatient Services'
        |when  HCPCS_CPT_CD  between  '99217'  and  '99220'  then  '2 Hospital Observation'
        |when  HCPCS_CPT_CD  between  '99221'  and  '99239'  then  '2 Hospital Inpatient Services'
        |when  HCPCS_CPT_CD  between  '99241'  and  '99275'  then  '2 Consultations'
        |when  HCPCS_CPT_CD  between  '99281'  and  '99288'  then  '2 Emergency Dept Services'
        |when  HCPCS_CPT_CD  between  '99289'  and  '99296'  then  '2 Critical Care Services'
        |when  HCPCS_CPT_CD  between  '99301'  and  '99316'  then  '2 Nursing Facility Services'
        |when  HCPCS_CPT_CD  between  '99321'  and  '99333'  then  '2 Domiciliary, Rest Home, or Custodial Care'
        |when  HCPCS_CPT_CD  between  '99341'  and  '99350'  then  '2 Home Services'
        |when  HCPCS_CPT_CD  between  '99354'  and  '99360' then  '2 Prolonged Services'
        |when  HCPCS_CPT_CD  between  '99361'  and  '99373'  then  '2 Case Mgmt Services'
        |when  HCPCS_CPT_CD  between  '99374' and  '99380'  then  '2 Care Plan Oversight Svcs'
        |when  HCPCS_CPT_CD  between  '99381'  and  '99397'  then  '2 Preventive Services'
        |when  HCPCS_CPT_CD  between  '99401'  and  '99429'  then  '2 Counseling'
        |when  HCPCS_CPT_CD  between  '99431'  and  '99440'  then  '2 Newborn Care'
        |when  HCPCS_CPT_CD  between  '99450'  and  '99456'  then  '2 Special E & M Services'
        |when  HCPCS_CPT_CD = '99499' then  '2 Other E & M Services'
        |
        |when  HCPCS_CPT_CD  between '92506'  and  '92508'
        |		or HCPCS_CPT_CD= '92597'
        |		or HCPCS_CPT_CD between  '92607' and '92609'  then  '6 Speech Evaluation/Prosthesis/Therapy'
        |when  HCPCS_CPT_CD  between '97000'  and  '97006'  then  '6 Evaluation/Re and evaluation'
        |when  HCPCS_CPT_CD  between '97010'  and  '97039'  then  '6 Modalities'
        |when  HCPCS_CPT_CD  between '97110'  and  '97546'
        |		or HCPCS_CPT_CD='99509'  then  '6 Therapeutic Procedures'
        |when  HCPCS_CPT_CD  in ( '97700', '97701', '97703', '97750', '97752', '97755')  then  '6 Tests and Measurements'
        |when  HCPCS_CPT_CD  between '97760'  and  '97799'  then  '6 Orthotic Management and Prosthetic Management'
        |when  HCPCS_CPT_CD  between '98925'  and  '98929'  then  '6 Osteopathic Manipulation'
        |when  HCPCS_CPT_CD  between  '90281'  and  '90399'  then  '3 Immune Globulins'
        |when  HCPCS_CPT_CD  between  '90400'  and  '90464' then  '3 Nursing Facility Services'
        |when  HCPCS_CPT_CD  between  '90465'  and  '90474'  then  '3 Immunization Administration'
        |when  HCPCS_CPT_CD  between  '90476'  and  '90749'  then  '3 Vaccines & Toxoids'
        |when  HCPCS_CPT_CD  between  '90780'  and  '90781'  then  '3 Therapeutic or Diagnostic Infusions'
        |when  HCPCS_CPT_CD  between  '90782'  and  '90799'  then  '3 Therapeutic, Prophylactic or Diag Injections'
        |when  HCPCS_CPT_CD  between  '90801'  and  '90899'  then  '3 Psychiatry'
        |when  HCPCS_CPT_CD  between  '90901'  and  '90911'  then  '3 Biofeedback'
        |when  HCPCS_CPT_CD  between  '90918'  and  '90999'  then  '3 Dialysis'
        |when  HCPCS_CPT_CD  between  '91000'  and  '91299'  then  '3 Gastroenterology'
        |when  HCPCS_CPT_CD  between  '92002'  and  '92499'  then  '3 Ophthalmology'
        |when  HCPCS_CPT_CD  between  '92502'  and  '92700'  then  '3 Special Otorhinolaryngologic Svcs'
        |when  HCPCS_CPT_CD  between  '92950'  and  '93799'  then  '3 Cardiovascular'
        |when  HCPCS_CPT_CD  between  '93875'  and  '93990'  then  '3 Non and Invasive Vascular Studies'
        |when  HCPCS_CPT_CD  between  '94010'  and  '94799'  then  '3 Pulmonary'
        |when  HCPCS_CPT_CD  between  '95004'  and  '95199'  then  '3 Allergy & Clinical Immunology'
        |when  HCPCS_CPT_CD  between  '95805'  and  '96004'  then  '3 Neurology'
        |when  HCPCS_CPT_CD  between  '96100'  and  '96117'  then  '3 Central Nervous System Tests'
        |when  HCPCS_CPT_CD  between  '96150'  and  '96155'  then  '3 Health and Behavior Assessment'
        |when  HCPCS_CPT_CD  between  '96400'  and  '96549' then  '3 Chemotherapy Administration'
        |when  HCPCS_CPT_CD  between  '96567'  and  '96571'  then  '3 Photodynamic Therapy'
        |when  HCPCS_CPT_CD  between  '96900'  and  '96999'  then  '3 Special Dermatological Procedures'
        |when  HCPCS_CPT_CD  between  '97001'  and  '97799'  then  '3 Physical Medicine and Rehabilitation'
        |when  HCPCS_CPT_CD  between  '97802'  and  '97804'  then  '3 Medical Nutrition Therapy'
        |when  HCPCS_CPT_CD  between  '98925'  and  '98929'  then  '3 Osteopathic Manipulation'
        |when  HCPCS_CPT_CD  between  '98940'  and  '98943'  then  '3 Chiropractic Manipulation'
        |when  HCPCS_CPT_CD  between  '99000'  and  '99091'  then  '3 Special Services & Reports'
        |when  HCPCS_CPT_CD  between  '99100'  and  '99140'  then  '3 Qualifying Circumstances for Anesthesia'
        |when  HCPCS_CPT_CD  between  '99141'  and  '99142'  then  '3 Sedation With or Without Analgesia'
        |when  HCPCS_CPT_CD  between  '99170'  and  '99199'  then  '3 Other Medical Services and Procedures'
        |when  HCPCS_CPT_CD  between  '99500'  and  '99602'  then  '3 Home Health Procedures & Services'
        |when  HCPCS_CPT_CD = '95250' then  '3 Endocrinology'
        |when  HCPCS_CPT_CD  between  'A0000'  and  'A0999'  then  '4 Ambulance'
        |when  HCPCS_CPT_CD  =  'A2000'            then  '4 Chiropractic'
        |when  HCPCS_CPT_CD  between  'A4000'  and  'A4999'  then  '4 Med & Surg Supplies'
        |when  HCPCS_CPT_CD  between  'A5051'  and  'A5507'  then  '4 Additional Ostomy Supplies'
        |when  HCPCS_CPT_CD  between  'A9000'  and  'A9999'  then  '4 Administrative'
        |when  HCPCS_CPT_CD  between  'B4000'  and  'B9999'  then  '4 Enteral & Parentral Therapy'
        |when  HCPCS_CPT_CD  between  'D0110'  and  'D9999'  then  '4 Dental'
        |when  HCPCS_CPT_CD  between  'E0100'  and  'E1702'  then  '4 Durable Medical Equipment'
        |when  HCPCS_CPT_CD  between  'G0001'  and  'G0025'  then  '4 Professional Services'
        |when  HCPCS_CPT_CD  between  'H5000'  and  'H5300'  then  '4 Rehab Services'
        |when  HCPCS_CPT_CD  between  'J0110'  and  'J8999'  then  '4 Drugs (Injected)'
        |when  HCPCS_CPT_CD  between  'J9000'  and  'J9999'  then  '4 Chemotherapy Drugs'
        |when  HCPCS_CPT_CD  between  'K0001'  and  'K0285'  then  '4 DME  and  Regional'
        |when  HCPCS_CPT_CD  between  'L0100'  and  'L4999'  then  '4 Orthotic Procedures'
        |when  HCPCS_CPT_CD  between  'L5000'  and  'L9999'  then  '4 Prosthetic Procedures'
        |when  HCPCS_CPT_CD  between  'M0000'  and  'M9999'  then  '4 HCFA Assignment of Services'
        |when  HCPCS_CPT_CD  between  'P0000'  and  'P9999'  then  '4 Laboratory Tests'
        |when  HCPCS_CPT_CD  between  'Q0034'  and  'Q0126'  then  '4 Temporary Codes (Q0xxx)'
        |when  HCPCS_CPT_CD  between  'Q9920'  and  'Q9999'  then  '4 Injection Codes for EPO'
        |when  HCPCS_CPT_CD  between  'R0000'  and  'R0999'  then  '4 Diagnostic Radiology'
        |when  HCPCS_CPT_CD  between  'V0000'  and  'V2799'  then  '4 Vision Services'
        |when  HCPCS_CPT_CD  between  'V5000'  and  'V5299'  then  '4 Hearing Services'
        |when  HCPCS_CPT_CD  between  'V5300'  and  'V5399' then  '4 Speech Language Pathology'
        |when  HCPCS_CPT_CD  between  'W0000'  and  'Z9999'  then  '4 Locally Grown HCPCS (W and Z)'
        |when  HCPCS_CPT_CD  between  '80002'  and  '80019'  then  '5 Automated Multichannel Tests'
        |when  HCPCS_CPT_CD  between  '80048'  and  '80099'  then  '5 Organ Or Disease Oriented Panels'
        |when  HCPCS_CPT_CD  between  '80100'  and  '80299'  then  '5 Therapeutic Drug Assays & Testing'
        |when  HCPCS_CPT_CD  between  '80400'  and  '80440'  then  '5 Evocative / Suppresion Testing  /* From Mcgraw Hill */'
        |when  HCPCS_CPT_CD  between  '80500'  and  '80502'  then  '5 Consultations'
        |when  HCPCS_CPT_CD  between  '81000'  and  '81099'  then  '5 Urinalysis'
        |when  HCPCS_CPT_CD  between  '82000'  and  '84999'  then  '5 Chemistry'
        |when  HCPCS_CPT_CD  between  '85002'  and  '85999'  then  '5 Hematology & Coagulation'
        |when  HCPCS_CPT_CD  between  '86000'  and  '86849'  then  '5 Immunology'
        |when  HCPCS_CPT_CD  between  '86850'  and  '86999'  then  '5 Transfusion Medicine'
        |when  HCPCS_CPT_CD  between  '87001'  and  '87999'  then  '5 Microbiology'
        |when  HCPCS_CPT_CD  between  '88000'  and  '88099'  then  '5 Anatomic Pathology'
        |when  HCPCS_CPT_CD  between  '88104'  and  '88199'  then  '5 Cytopathology'
        |when  HCPCS_CPT_CD  between  '88230'  and  '88299'  then  '5 Cytogenetic Studies'
        |when  HCPCS_CPT_CD  between  '88300'  and  '88399'  then  '5 Surgical Pathology'
        |when  HCPCS_CPT_CD  between  '89050'  and  '89240'  then  '5 Other Procedures'
        |when  HCPCS_CPT_CD  between  '89250'  and  '89356'  then  '5 Reproductive Medicine Procedures'
        |when  HCPCS_CPT_CD = '88400' then  '5 Transcutaneous Procedures'
        |when  HCPCS_CPT_CD  between '70010'  and  '76499'
        |		or HCPCS_CPT_CD between '77000'  and  '77260'then  '7 Radiology: Other'
        |when  HCPCS_CPT_CD  between  '76506'  and  '76999'  then  '7 Ultrasound'
        |when  HCPCS_CPT_CD  between  '77261'  and  '77799'  then  '7 Radiation Oncology'
        |when  HCPCS_CPT_CD  between  '78000'  and  '78458'
        |     or HCPCS_CPT_CD between '78460'  and  '78490'
        |     or HCPCS_CPT_CD between '78493'  and  '78607'
        |     or HCPCS_CPT_CD between '78610'  and  '78809'
        |     or HCPCS_CPT_CD between '78817'  and  '79999'  then  '7 Nuclear Medicine: Other'
        | when  HCPCS_CPT_CD in ( '78608', '78609', '78459', '78491', '78492', '78810',  '78816') then  '7 Nuclear Medicine: PET'
        |when  HCPCS_CPT_CD in ('70336', '70540', '70541', '70542', '70543', '70550', '70551', '70552', '70553','71550', '71551', '71552',
        |'72140', '72141', '72142', '72143', '72146', '72147','72148', '72149',
        |'72156', '72157', '72158', '72195', '72196', '72197', '73218','73219', '73220', '73221', '73222', '73223', '73718',
        | '73719', '73720', '73721','73722', '73723', '74181', '74182', '74183', '75552', '75553', '75554', '75555',
        |'75556', '76093', '76094', '76390', '76400', '77021', '77022', '77058', '77059', '77084')  then  '7 Radiology: MRI'
        |when  HCPCS_CPT_CD in('70450', '70460', '70470', '70480', '70481', '70482', '70486', '70487', '70488','70490', '70491',
        | '70492', '70496', '70498', '71250', '71260', '71270', '71275',     '72125', '72126', '72127', '72128', '72129', '72130',
        | '72131', '72132', '72133',     '72192', '72193', '72194', '73200', '73201', '73202', '73700', '73701', '73702',
        |  '74150', '74160', '74170', '74175', '75635', '76070', '76355', '76360', '76365',
        |  '76370', '76375', '76380' , '77011', '77012', '77013', '77014', '77078', '77079') then  '7 Radiology: CT'
        |when  HCPCS_CPT_CD in('70544', '70545', '70546', '70547', '70548', '70549', '71555', '72159', '72198',
        |     '73225', '73725', '74185')  then  '7 Radiology: MRA'
        |when  HCPCS_CPT_CD in ('76075', '76076' , '77080', '77081', '77082') then  '7 Radiology: Bone Density'
        |when  HCPCS_CPT_CD in( '76083', '76090', '76091', '76092' , '77051', '77052', '77055', '77056',
        |          '77057', 'G0202', 'G0204', 'G0206') then  '7 Radiology: Mammogram'
        |when  HCPCS_CPT_CD  between  '01112'  and  '01190'  then  '8 Pelvis'
        |when  HCPCS_CPT_CD  between  '01200'  and  '01274'  then  '8 Upper Leg'
        |when  HCPCS_CPT_CD  between  '01320'  and  '01444'  then  '8 Knee & Popliteal Area'
        |when  HCPCS_CPT_CD  between  '01462'  and  '01522'  then  '8 Lower Leg'
        |when  HCPCS_CPT_CD  between  '01600'  and  '01682'  then  '8 Shoulder & Axilla'
        |when  HCPCS_CPT_CD  between  '01700'  and  '01782'  then  '8 Upper Arm & Elbow'
        |when  HCPCS_CPT_CD  between  '01800'  and  '01860'  then  '8 Forearm, Wrist & Hand'
        |when  HCPCS_CPT_CD  between  '01900'  and  '01933'  then  '8 Radiological Procedures'
        |when  HCPCS_CPT_CD  between  '01958'  and  '01969'  then  '8 Obstetric'
        |when  HCPCS_CPT_CD  between  '01990'  and  '01999'  then  '8 Miscellaneous Procedures'
        |when  HCPCS_CPT_CD  between  '10021'  and  '10022'  then  '8 General'
        |when  HCPCS_CPT_CD  between  '10040'  and  '19499'  then  '8 Integumentary System'
        |when  HCPCS_CPT_CD  between  '20000'  and  '29999'  then  '8 Musculoskeletal System'
        |when  HCPCS_CPT_CD  between  '30000'  and  '32999'  then  '8 Respiratory System'
        |when  HCPCS_CPT_CD  between  '33010'  and  '37799'  then  '8 Cardiovascular System'
        |when  HCPCS_CPT_CD  between  '38100'  and  '38999'  then  '8 Hemic & Lymphatic Systems'
        |when  HCPCS_CPT_CD  between  '39000'  and  '39599'  then  '8 Mediastinum & Diaphragm'
        |when  HCPCS_CPT_CD  between  '40490'  and  '49999'  then  '8 Digestive System'
        |when  HCPCS_CPT_CD  between  '50010'  and  '53899'  then  '8 Urinary System'
        |when  HCPCS_CPT_CD  between  '54000'  and  '55899'  then  '8 Male Genital System'
        |when  HCPCS_CPT_CD  between  '55970'  and  '55980'  then  '8 Intersex Surgery'
        |when  HCPCS_CPT_CD  between  '56405'  and  '58999'  then  '8 Female Genital System'
        |when  HCPCS_CPT_CD  between  '59000'  and  '59899'  then  '8 Maternity Care & Delivery'
        |when  HCPCS_CPT_CD  between  '60000'  and  '60699'  then  '8 Endocrine System'
        |when  HCPCS_CPT_CD  between  '61000'  and  '64999'  then  '8 Nervous System'
        |when  HCPCS_CPT_CD  between  '65091'  and  '68899'  then  '8 Eye & Ocular Adnexa'
        |when  HCPCS_CPT_CD  between  '69000'  and  '69979'  then  '8 Auditory System'
        |when  HCPCS_CPT_CD = '69990' then  '8 Operating Microscope'
        |when  HCPCS_CPT_CD  between  '90000'  and  '90080'  then  '9 Office Visits & Outpatient Services'
        |when  HCPCS_CPT_CD  between  '90100'  and  '90170'  then  '9 Home Services'
        |when  HCPCS_CPT_CD  between  '90200'  and  '90220'  then  '9 Hospital Inpatient Services'
        |when  HCPCS_CPT_CD  between  '90240'  and  '90280'  then  '9 Hospital Inpatient Services'
        |when  HCPCS_CPT_CD = '90225' then  '9 Newborn Care'
        | else   '9 Other' end as detailservicetypelong,
        |concat(c.corp_ent_cd,databaseid,dw_alt_indivl_key,case when pat_relshp_cd='01' then 'SUB' else 'DEP' end) as mbrid,
        |bllg_prov_tax_id as billingprovidertaxid,
        |cast(svc_from_Dt as date) as servicedate
        |from cvtencounterilhmo c
        |inner join groupmaster g
        |on g.corpentcd=c.corp_ent_cd
        |and c.acct_nbr=g.accountid
        |and c.subacct_grp_id=g.groupid
        |and c.subacct_sect_id=sectionid
        |inner join
        |last_seq2 l
        |on c.dw_clm_key=l.dw_clm_key
        |where Disp_cd_cli='A'
        |
        |
      """.stripMargin)

    prof_claims_1.createOrReplaceTempView("prof_claims_1")
    prof_claims_1.show(5)
  }

  def load_prof_claims()={

    sparkSession.sql("""drop table if exists prof_claims""".stripMargin)
    val prof_claims = sparkSession.sql(
      """
        |select der.*,substring(detailservicetypelong,2,length(detailservicetypelong)-1) as DetailServiceType,
        |case when substring(detailservicetypelong,1,1)='1' then 'Anesthesia'
        |when substring(detailservicetypelong,1,1)='2' then 'Evaluation & Management'
        |when substring(detailservicetypelong,1,1)='3' then 'Medical'
        |when substring(detailservicetypelong,1,1)='4' then 'Medical Services & Supplies (HCPCS II)'
        |when substring(detailservicetypelong,1,1)='5' then 'Pathology & Laboratory'
        |when substring(detailservicetypelong,1,1)='6' then 'PT/OT/ST'
        |when substring(detailservicetypelong,1,1)='7' then 'Radiology'
        |when substring(detailservicetypelong,1,1)='8' then 'Surgical'
        |when substring(detailservicetypelong,1,1)='9' then 'Other'
        |else 'Other' end as ServiceType
        |from prof_claims_1 der
      """.stripMargin)
    prof_claims.createOrReplaceTempView("prof_claims")
    prof_claims.show(5)

  }

  def load_preprofvisits()={

    sparkSession.sql("drop table if exists preprofvisits")

    val preprofvisits = sparkSession.sql(
      """
        |select der.*,idoffset+sum(1) over (order by mbrid,servicedate rows unbounded preceding) as profvisitid
        |from
        |(
        |select mbrid,
        |billingprovidertaxid,idoffset,
        | servicedate
        |from prof_claims c
        |inner join
        |(select max(facvisitid) as idoffset from facvisits) der2
        |on 1=1
        |group by mbrid,billingprovidertaxid,idoffset,servicedate
        |)der
        |
      """.stripMargin)
    preprofvisits.createOrReplaceTempView("preprofvisits")
    preprofvisits.show(5)


  }


  def load_visitservicetypelong_1()={

    sparkSession.sql("""drop table if exists visitservicetypelong""")

    val visitservicetypelong_1= sparkSession.sql(
      """
        |select der.* from
        |(select profvisitid,detailservicetypelong,sum(1) over
        |		(partition by profvisitid order by
        |		rank asc,
        |case when substring(detailservicetypelong,1,1)='1' then 10  /* 'Anesthesia'*/
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Pelvis' then 11
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Upper Leg' then 12
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Knee & Popliteal Area' then 13
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Lower Leg' then 14
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Shoulder & Axilla' then 15
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Upper Arm & Elbow' then 16
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Forearm, Wrist & Hand' then 17
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Radiological Procedures' then 18
        | when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)='Obstetric' then 19
        |when substring(detailservicetypelong,1,1)='8'
        |			and substring(detailservicetypelong,2,length(detailservicetypelong)-1)=' Miscellaneous Procedures' then 20
        |when substring(detailservicetypelong,1,1)='2' then 21 /* 'Evaluation & Management'*/
        |when substring(detailservicetypelong,1,1)='3' then  24 /*'Medical'*/
        |when substring(detailservicetypelong,1,1)='4' then 27 /*'Medical Services & Supplies (HCPCS II)'*/
        |when substring(detailservicetypelong,1,1)='5' then 28 /*'Pathology & Laboratory'*/
        |when substring(detailservicetypelong,1,1)='6' then 32 /* 'PT/OT/ST'*/
        |when substring(detailservicetypelong,1,1)='7' then 30 /*'Radiology'*/
        |when substring(detailservicetypelong,1,1)='8' then 31/* 'Surgical'*/
        |else 99 end asc rows unbounded preceding) recnum
        |from prof_claims c
        |inner join
        |preprofvisits p
        |on c.mbrid=p.mbrid
        |and c.billingprovidertaxid=p.billingprovidertaxid
        |and c.servicedate=p.servicedate
        |left outer join
        |dstprof x
        |on trim(c.detailservicetype)=trim(x.detailservicetype)
        |) der
        |where recnum=1
        |
        |
      """.stripMargin)

    visitservicetypelong_1.createOrReplaceTempView("visitservicetypelong")
    visitservicetypelong_1.show(5)

  }


  def load_profvisitsxclm()={

    sparkSession.sql("""drop table if exists profvisitsxclm""")

    val profvisitsxclm= sparkSession.sql(
      """
        |select rowid,dw_clm_key,li_num,hcpcs_cpt_cd,hcpcs_cpt_modr_cd_1,p.profvisitid,dw_clm_cntrl_key,servicetype,detailservicetype,
        |concat(p.profvisitid,hcpcs_cpt_cd,hcpcs_cpt_modr_cd_1) as ProcedureID,
        |
        |v.detailservicetypelong,
        |			max(case when plc_of_trmnt_cd in('22','23') then 'Y' else 'N' end) over (partition by p.profvisitid)as ERConcurrentFlag
        |
        |from prof_claims c
        |inner join
        |preprofvisits p
        |on c.mbrid=p.mbrid
        |and c.billingprovidertaxid=p.billingprovidertaxid
        |and c.servicedate=p.servicedate
        |inner join
        |visitservicetypelong v
        |on p.profvisitid=v.profvisitid
        |
      """.stripMargin)

    profvisitsxclm.createOrReplaceTempView("profvisitsxclm")
    profvisitsxclm.show(5)

  }

  def load_profvisitlapse()={



    sparkSession.sql("drop table if exists profvisitlapse")

    val profvisitlapse= sparkSession.sql(
      """
        |
        |select der.*,datediff(servicedate,previousdischarge) as daylapsepreviousdischarge
        |from
        |(select p.*,a.enddt as previousdischarge,
        |		sum(1) over (partition by profvisitid order by a.enddt desc rows unbounded preceding) as seqnum
        |from preprofvisits p
        |inner join
        |preadmits a
        |on p.mbrid=a.mbrid
        |and a.enddt<=p.servicedate)der
        |where seqnum=1
        |
        |
      """.stripMargin)
    profvisitlapse.createOrReplaceTempView("profvisitlapse")

    val profvisitlapse_insert= sparkSession.sql(
      """
        |select p.*,null as previousdischarge,0 as seqnum,null as dayslapsepreviousdischarge
        |from preprofvisits p where profvisitid not in (select profvisitid from profvisitlapse)
        |
      """.stripMargin)

    val profvisitlapse_table= profvisitlapse.union(profvisitlapse_insert)

    profvisitlapse_table.createOrReplaceTempView("profvisitlapse")
    profvisitlapse_table.show(5)
  }

  def load_new_preprofvisits()={

    sparkSession.sql("drop table if exists preprofvisits")

    val new_preprofvisits=sparkSession.sql(
      """
        |select der.*,datediff(nextadmit,servicedate) as daylagenextadmit
        |from
        |(select p.*,a.startdt as nextadmit,
        |		sum(1) over (partition by profvisitid order by a.startdt asc rows unbounded preceding) as seqnum2
        |from profvisitlapse p
        |inner join
        |preadmits a
        |on p.mbrid=a.mbrid
        |and p.servicedate<=a.startdt)der
        |where seqnum2=1
        |
      """.stripMargin)
    new_preprofvisits.createOrReplaceTempView("new_preprofvisits_temp")


    val new_preprofvisits_insert= sparkSession.sql(
      """
        |select p.*,null as nextadmit,0 as seqnum2,null as dayslagnextadmit
        |from profvisitlapse p where profvisitid not in (select profvisitid from new_preprofvisits_temp)
        |
      """.stripMargin)

    val  new_preprofvisits_table= new_preprofvisits.union(new_preprofvisits_insert)
    new_preprofvisits_table.createOrReplaceTempView("preprofvisits")
    new_preprofvisits_table.show(5)

  }

  def load_profvisitservicetype()={

    sparkSession.sql("drop table if exists profvisitservicetype")

    val profvisitservicetype=sparkSession.sql(
      """
        |select distinct profvisitid,detailservicetypelong,ERConcurrentFlag
        |from profvisitsxclm
        |
      """.stripMargin)
    profvisitservicetype.createOrReplaceTempView("profvisitservicetype")
    profvisitservicetype.show(5)


  }


  def load_profvisits()={

    sparkSession.sql("drop table if exists profvisits")


    val profvisits= sparkSession.sql(
      """
        |select p.*,substring(detailservicetypelong,2,length(detailservicetypelong)-1) as DetailServiceType,
        |case when substring(detailservicetypelong,1,1)='1' then 'Anesthesia'
        |when substring(detailservicetypelong,1,1)='2' then 'Evaluation & Management'
        |when substring(detailservicetypelong,1,1)='3' then 'Medical'
        |when substring(detailservicetypelong,1,1)='4' then 'Medical Services & Supplies (HCPCS II)'
        |when substring(detailservicetypelong,1,1)='5' then 'Pathology & Laboratory'
        |when substring(detailservicetypelong,1,1)='6' then 'PT/OT/ST'
        |when substring(detailservicetypelong,1,1)='7' then 'Radiology'
        |when substring(detailservicetypelong,1,1)='8' then 'Surgical'
        |when substring(detailservicetypelong,1,1)='9' then 'Other'
        |else 'Other' end as ServiceType,
        |
        |case when f.mbrid is not null and pp.ERConcurrentFlag='Y' then 'Y' else 'N' end as ConcurrentERFlag,
        |case when  f.mbrid is not null and pp.erconcurrentflag='Y' and ernonemergentflag='Y' then 'Y' else 'N'end as ERnonEmergentflag
        |from preprofvisits p
        |inner join
        |profvisitservicetype pp
        |on p.profvisitid=pp.profvisitid
        |left join
        |(select mbrid,servicedate,max(ernonemergentflag) ernonemergentflag from facvisits where erflag='Y' group by mbrid,servicedate)f
        |on p.mbrid=f.mbrid
        |and p.servicedate=f.servicedate
        |
      """.stripMargin)
    profvisits.createOrReplaceTempView("profvisits")
    profvisits.show(5)
  }

  def load_admitlapse()={


  sparkSession.sql("drop table if exists admitlapse")
  val admitlapse=sparkSession.sql(
    """
      |select der.*,datediff(startdt,previousdischarge) as daylapsepreviousdischarge
      |    from
      |    (select p.*,a.enddt as previousdischarge,
      |    sum(1) over (partition by p.admid order by a.enddt desc rows unbounded preceding) as seqnum
      |      from preadmits p
      |    inner join
      |      preadmits a
      |      on p.mbrid=a.mbrid
      |    and a.enddt<=p.startdt)der
      |    where seqnum=1
    """.stripMargin)

  admitlapse.createOrReplaceTempView("admitlapse_temp")

  val admitlapse_insert =sparkSession.sql(
    """
      |select p.*,null as previousdischarge,0 as seqnum, null as dayslapsepreviousdischarge
      |from preadmits p where admid not in (select admid from admitlapse_temp)
    """.stripMargin)

  val admitlapse_table= admitlapse.union(admitlapse_insert)
  admitlapse_table.createOrReplaceTempView("admitlapse")
    admitlapse_table.show(5)
  sparkSession.sql("drop table if exists admitlapse_temp")

}


  def load_preadmits2()={

    sparkSession.sql("drop table if exists preadmits2")

    val preadmits2=sparkSession.sql(
      """
        |
        |select der.*,datediff(nextadmit,enddt) as daylagenextadmit
        |from
        |(select p.*,a.startdt as nextadmit,
        |		sum(1) over (partition by p.admid order by a.startdt asc rows unbounded preceding) as seqnum2
        |from admitlapse p
        |inner join
        |preadmits a
        |on p.mbrid=a.mbrid
        |and p.enddt<=a.startdt)der
        |where seqnum2=1
        |
      """.stripMargin)
    preadmits2.createOrReplaceTempView("preadmits2_temp")

    val preadmits2_insert = sparkSession.sql(
      """
        |select p.*,null as nextadmit,0 as seqnum2, null as dayslagnextadmit
        |from admitlapse p where admid not in (select admid from preadmits2_temp)
      """.stripMargin)

    val preadmits2_table= preadmits2.union(preadmits2_insert)

    preadmits2_table.createOrReplaceTempView("preadmits2")
    preadmits2_table.show(5)
    sparkSession.sql("drop table if exists preadmits2_temp")
  }

  def load_admitservicetype()={

    sparkSession.sql("drop table if exists admitservicetype")

    val admitservicetype= sparkSession.sql(
      """
        |
        |
        |
        |select admid,min(detailservicetypelong ) detailservicetypelong,max(multiplegestationflag) multiplegestationflag,
        |max(DeliveryLT39WeeksFlag) DeliveryLT39WeeksFlag
        |from admxclm
        |group by admid
        |
        |
      """.stripMargin)
    admitservicetype.show(5)
    admitservicetype.createOrReplaceTempView("admitservicetype")


  }


  def load_admits()={
  sparkSession.sql("drop table if exists admits")

  val admits= sparkSession.sql(
    """
      |select distinct p.*
      |
      |,'N'  as ConcurrentERFlag
      |,MultipleGestationFlag
      |, DeliveryLT39WeeksFlag,
      |case when substring(detailservicetypelong,3,1) ='1' then 'Non-Acute'
      |when substring(detailservicetypelong,3,1) ='2' then 'Maternity'
      |when substring(detailservicetypelong,3,1) ='3' then 'Mental Health/Substance Abuse'
      |when substring(detailservicetypelong,3,1) ='4' then 'Surgical'
      |when substring(detailservicetypelong,3,1) ='5' then 'Medical'
      |else 'OTHER' end as ServiceType,
      |substring(detailservicetypelong,4,length(detailservicetypelong)-3) as DetailServiceType
      |from preadmits2 p
      |left outer join
      |admitservicetype pp
      |on p.admid=pp.admid
      |left outer join
      |(select  mbrid,servicedate from facvisits where erflag='Y' group by mbrid,servicedate) f
      |on p.mbrid=f.mbrid
      |and f.servicedate between startdt and enddt
      |
      |
    """.stripMargin)

  admits.createOrReplaceTempView("admits")
    admits.show(5)
}

  def load_admitservicetype1()={

    sparkSession.sql("drop table if exists admitservicetype")


    val admitservicetype1=sparkSession.sql(
      """
        |select admid,min(detailservicetypelong ) detailservicetypelong,max(multiplegestationflag) multiplegestationflag,
        |max(DeliveryLT39WeeksFlag) DeliveryLT39WeeksFlag
        |from babyadmxclm
        |group by admid
        |
        |
      """.stripMargin)
    admitservicetype1.show(5)

    admitservicetype1.createOrReplaceTempView("admitservicetype")

  }

  def load_babyadmits()={





    sparkSession.sql("drop table if exists babyadmits")

    val babyadmits= sparkSession.sql(
      """
        |select distinct p.*
        |--,max(case when f.mbrid is not null then 'Y' else 'N' end) over
        |--			(partition by p.admid)as ConcurrentERFlag,
        |,'N' as ConcurrentERFlag,
        |MultipleGestationFlag
        |,DeliveryLT39WeeksFlag,
        |case when substring(detailservicetypelong,3,1) ='1' then 'Non-Acute'
        |when substring(detailservicetypelong,3,1) ='2' then 'Maternity'
        |when substring(detailservicetypelong,3,1) ='3' then 'Mental Health/Substance Abuse'
        |when substring(detailservicetypelong,3,1) ='4' then 'Surgical'
        |when substring(detailservicetypelong,3,1) ='5' then 'Medical'
        |else 'OTHER' end as ServiceType,
        |substring(detailservicetypelong,4,length(detailservicetypelong)-3) as DetailServiceType
        |from babymergeprovadms p
        |left outer join
        |admitservicetype pp
        |on p.admid=pp.admid
        |left outer join
        |(select  mbrid,servicedate from facvisits where erflag='Y' group by mbrid,servicedate) f
        |on p.mbrid=f.mbrid
        |and f.servicedate between startdt and enddt
        |
      """.stripMargin)
    babyadmits.show(5)
    babyadmits.createOrReplaceTempView("babyadmits")


  }


  def load_facoutcompressionmaster()={




    sparkSession.sql("drop table if exists facoutcompressionmaster")

    val facoutcompressionmaster=sparkSession.sql(
      """
        |
        |select
        |dw_clm_cntrl_key as ClaimControlID
        |,dw_clm_key as DWClaimKey
        |,li_num as ClaimLineNumber
        |,facvisitid as ServiceID
        |,'FACILITY OUTPATIENT' as ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,servicetype as ServiceType
        |,detailservicetype as ServiceTypeDetail
        |,ERFlag
        |,ERNonEmergentFlag
        |,'N' as ConcurrentERFlag
        |, ServiceDate
        |,cast('0000-01-01' as date) as DischargeDate
        |, 0 as UtilizationDays
        |,DayLapsePreviousDischarge
        |,daylagenextadmit DayLagNextAdmission
        |,util_dw_clm_key as UtilizationDWClaimKey
        |from
        |(
        |select f.facvisitid,erflag,ernonemergentflag,f.servicedate,DayLapsePreviousDischarge,daylagenextadmit
        |,c.clm_id dw_clm_cntrl_key,c.dw_clm_key,x.li_num,x.servicetype,x.detailservicetype,f.servicetype as UtilizationServiceType,
        |f.detailservicetype as UtilizationDetailServiceType,
        |x.dw_clm_key as util_dw_clm_key,
        |sum(1) over (partition by c.clm_id,x.li_num order by c.pd_dt asc rows unbounded preceding)rowid
        |from facvisits f
        |inner join
        |facvisitsxclm x
        |on f.facvisitid=x.facvisitid
        |inner join
        |cvtencounterilhmo c
        |on x.dw_clm_cntrl_key=c.clm_id
        |and x.li_num=c.li_num
        |--and dbit_cr_cd <>'C'
        |--and disp_cd_cli<>'D'
        |) der
        |where rowid=1
        |
      """.stripMargin)
    facoutcompressionmaster.createOrReplaceTempView("facoutcompressionmaster")
    facoutcompressionmaster.show(5)


  }


  def load_profcompressionmaster()={

    sparkSession.sql("drop table if exists profcompressionmaster")


    val profcompressionmaster=sparkSession.sql(
      """
        |
        |select
        |dw_clm_cntrl_key as ClaimControlID
        |,dw_clm_key as DWClaimKey
        |,li_num as ClaimLineNumber
        |,profvisitid as ServiceID
        |,'PROFESSIONAL' as ServiceCategory
        |--,servicetype as UtilizationServiceType  line changed 10/2
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,servicetype as ServiceType
        |,detailservicetype as ServiceTypeDetail
        |,'N' as ERFlag
        |,ConcurrentERFlag
        |,ERNonEmergentFlag
        |, ServiceDate
        |,cast('0000-01-01' as date) as DischargeDate
        |, 0 as UtilizationDays
        |,DayLapsePreviousDischarge
        |,daylagenextadmit DayLagNextAdmission
        |,util_dw_clm_key as UtilizationDWClaimKey
        |,procedureid
        |from
        |(
        |select f.profvisitid,f.servicedate,DayLapsePreviousDischarge,daylagenextadmit,ConcurrentERFlag,ErNonEmergentFlag
        |,c.clm_id dw_clm_cntrl_key,c.dw_clm_key,x.li_num,x.servicetype,x.detailservicetype,f.servicetype as utilizationservicetype,
        |f.detailservicetype as UtilizationDetailServiceType,
        |x.dw_clm_key as util_dw_clm_key,procedureid,
        |sum(1) over (partition by c.clm_id,x.li_num order by c.pd_dt asc rows unbounded preceding)rowid
        |from profvisits f
        |inner join
        |profvisitsxclm x
        |on f.profvisitid=x.profvisitid
        |inner join
        |cvtencounterilhmo c
        |on x.dw_clm_cntrl_key=c.clm_id
        |and x.li_num=c.li_num
        |--and dbit_cr_cd <>'C'
        |--and disp_cd_cli<>'D'
        |) der
        |where rowid=1
        |
        |
        |
        |
      """.stripMargin)

    profcompressionmaster.createOrReplaceTempView("profcompressionmaster")
    profcompressionmaster.show(5)

  }

  def load_admxcli()={




    sparkSession.sql("drop table if exists admxcli")
    val admxcli=sparkSession.sql(
      """
        |
        |
        |
        |select a.*,li_num,case when rvnu_cd between '0100' and '0179' or rvnu_cd between '0190' and '0219'
        |      		or rvnu_cd in ('0224','0656') then 'Y' else 'N' end as rbind,pd_Dt
        |from admxclm a
        |inner join
        |cvtencounterilhmo c
        |on a.dw_clm_key=c.dw_clm_key
        |
        |
        |
      """.stripMargin).withColumnRenamed("admid","admids").withColumnRenamed("admids","admid").withColumnRenamed("dw_clm_cntrl_key","dw_clm_cntrl_keys").withColumnRenamed("dw_clm_cntrl_keys","dw_clm_cntrl_key").withColumnRenamed("li_num","li_nums").withColumnRenamed("li_nums","li_num")
    admxcli.createOrReplaceTempView("admxcli")

    admxcli.show(5)

  }

  def load_admitcompressionmaster()={

    sparkSession.sql("drop table if exists admitcompressionmaster")

    val admitcompressionmaster=sparkSession.sql(
      """
        |select
        |dw_clm_cntrl_key as ClaimControlID
        |,dw_clm_key as DWClaimKey
        |,li_num as ClaimLineNumber
        |,admid as ServiceID
        |,'FACILITY INPATIENT' as ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,servicetype as ServiceType
        |,detailservicetype as ServiceTypeDetail
        |,'N' as ERFlag
        |,'N' as ConcurrentERFlag
        |,'N' as ERNonEmergentFlag
        |, ServiceDate
        |,date_add(Servicedate,LOS) as DischargeDate
        |, LOS as UtilizationDays
        |,DayLapsePreviousDischarge
        |,daylagenextadmit DayLagNextAdmission
        |,util_dw_clm_key as UtilizationDWClaimKey
        |,NICUDays
        |,case when NICUDays<>0 then 'Y' else 'N' end as NICUFlag
        |,MultipleGestationFlag
        |,DeliveryLT39WeeksFlag
        |from
        |(
        |select f.admid,f.startdt as servicedate,DayLapsePreviousDischarge,daylagenextadmit,ConcurrentERFlag,
        |f.MultipleGestationFlag,f.DeliveryLT39WeeksFlag,f.NICUDays,f.LOS
        |,c.clm_id dw_clm_cntrl_key,c.dw_clm_key,x.li_num,x.servicetype,x.detailservicetype,f.servicetype as utilizationservicetype,
        |f.detailservicetype as utilizationdetailservicetype,
        |x.dw_clm_key as util_dw_clm_key,
        |sum(1) over (partition by c.clm_id,x.li_num order by c.pd_dt asc rows unbounded preceding)rowid
        |from admits f
        |inner join
        |admxcli x
        |on f.admid=x.admid
        |inner join
        |cvtencounterilhmo c
        |on x.dw_clm_cntrl_key=c.clm_id
        |and x.li_num=c.li_num
        |--and dbit_cr_cd <>'C'
        |--and disp_cd_clm<>'D'
        |) der
        |where rowid=1
        |
        |
        |
      """.stripMargin)

    admitcompressionmaster.createOrReplaceTempView("admitcompressionmaster")
    admitcompressionmaster.show(5)

  }

  def load_rblines()={

    sparkSession.sql("drop table if exists rblines")

    val rblines= sparkSession.sql(
      """
        |
        |select dw_clm_cntrl_key,li_num from
        |(
        |select dw_clm_cntrl_key,li_num,sum(1) over (partition by admid order by  rbind desc, pd_dt asc,
        |			li_num asc, dw_clm_cntrl_key asc rows unbounded preceding) as rowid
        |
        |from
        |admxcli a)der
        |where rowid=1
      """.stripMargin)

    rblines.createOrReplaceTempView("rblines")
    rblines.show(5)
  }

  def load_update_admitcompressionmaster()={


    val update_admitcompressionmaster=sparkSession.sql(
      """
        |
        |select ClaimControlID,DWClaimKey,ClaimLineNumber,ServiceID,ServiceCategory,UtilizationServiceType,UtilizationDetailServiceType,ServiceType,ServiceTypeDetail,ERFlag,ConcurrentERFlag,ERNonEmergentFlag,ServiceDate,
        |DischargeDate,case when (claimcontrolid,claimlinenumber) not in (select dw_clm_cntrl_key,li_num from rblines) then 0 else utilizationdays end as utilizationdays ,DayLapsePreviousDischarge,DayLagNextAdmission,
        |UtilizationDWClaimKey,NICUDays,NICUFlag,MultipleGestationFlag,DeliveryLT39WeeksFlag from admitcompressionmaster
        |
        |
        |
        |
      """.stripMargin)
    update_admitcompressionmaster.createOrReplaceTempView("admitcompressionmaster")
    update_admitcompressionmaster.show(5)
  }


  def load_babyadmxcli()={



    sparkSession.sql("drop table if exists babyadmxcli")
    val babyadmxcli=sparkSession.sql(
      """
        |
        |select a.*,li_num,case when rvnu_cd between '0100' and '0179' or rvnu_cd between '0190' and '0219'
        |			or rvnu_cd in ('0224','0656') then 'Y' else 'N' end as rbind,pd_dt
        |from babyadmxclm a
        |inner join
        |cvtencounterilhmo c
        |on a.dw_clm_key=c.dw_clm_key
        |
      """.stripMargin)
     babyadmxcli.createOrReplaceTempView("babyadmxcli")
    babyadmxcli.show(5)
    //babyadmxcli.show()


  }


  def load_babyadmitcompressionmaster()={


    sparkSession.sql("drop table if exists babyadmitcompressionmaster")


    val babyadmitcompressionmaster=sparkSession.sql(
      """
        |select
        |dw_clm_cntrl_key as ClaimControlID
        |,dw_clm_key as DWClaimKey
        |,li_num as ClaimLineNumber
        |,admid as ServiceID
        |,'FACILITY INPATIENT' as ServiceCategory
        |,UtilizationServiceType
        |,UtilizationDetailServiceType
        |,servicetype as ServiceType
        |,detailservicetype as ServiceTypeDetail
        |,'N' as ERFlag
        |,ConcurrentERFlag
        |,'N' as ERNonEmergentFlag
        |, ServiceDate
        |,date_add(Servicedate,LOS) as DischargeDate
        |, ReportingLOS as UtilizationDays
        |,cast (null as int) as DayLapsePreviousDischarge
        |,cast(null as int)as DayLagNextAdmission
        |,util_dw_clm_key as UtilizationDWClaimKey
        |,NICUDays
        |,case when NICUDays<>0 then 'Y' else 'N' end as NICUFlag
        |,MultipleGestationFlag
        |,DeliveryLT39WeeksFlag
        |from
        |(
        |select f.admid,f.startdt as servicedate,ConcurrentERFlag,
        |f.MultipleGestationFlag,f.DeliveryLT39WeeksFlag,f.ReportingLOS,f.los,0 as NICUdays
        |,c.clm_id dw_clm_cntrl_key,c.dw_clm_key,x.li_num,x.servicetype,x.detailservicetype,f.servicetype as utilizationservicetype,
        |f.detailservicetype as UtilizationDetailServiceType,
        |x.dw_clm_key as util_dw_clm_key,
        |sum(1) over (partition by c.clm_id ,x.li_num order by c.pd_dt asc rows unbounded preceding)rowid
        |from babyadmits f
        |inner join
        |babyadmxcli x
        |on f.admid=x.admid
        |inner join
        |cvtencounterilhmo c
        |on x.dw_clm_cntrl_key=c.clm_id
        |and x.li_num=c.li_num
        |
        |) der
        |where rowid=1
        |
        |
        |
        |
      """.stripMargin)
    babyadmitcompressionmaster.createOrReplaceTempView("babyadmitcompressionmaster")


    val update_babyadmitcompressionmaster=sparkSession.sql(
      """
        |
        |select ClaimControlID,DWClaimKey,ClaimLineNumber,ServiceID,ServiceCategory,UtilizationServiceType,UtilizationDetailServiceType,ServiceType,ServiceTypeDetail,ERFlag,ConcurrentERFlag,ERNonEmergentFlag,ServiceDate,
        |DischargeDate,case when (claimcontrolid,claimlinenumber) not in (select dw_clm_cntrl_key,li_num from rblines) then 0 else utilizationdays end as utilizationdays ,DayLapsePreviousDischarge,DayLagNextAdmission,
        |UtilizationDWClaimKey,NICUDays,NICUFlag,MultipleGestationFlag,DeliveryLT39WeeksFlag from babyadmitcompressionmaster
        |
        |
        |
      """.stripMargin)
    update_babyadmitcompressionmaster.createOrReplaceTempView("babyadmitcompressionmaster")
    update_babyadmitcompressionmaster.show(5)

  }

  def load_rblines_1()={


    sparkSession.sql("drop table if exists rblines")

    val rblines_1= sparkSession.sql(
      """
        |
        |
        |select dw_clm_cntrl_key,li_num from
        |(
        |select dw_clm_cntrl_key,li_num,sum(1) over (partition by admid order by  rbind desc, pd_Dt asc,
        |			li_num asc, dw_clm_cntrl_key asc rows unbounded preceding) as rowid
        |
        |from
        |babyadmxcli a)der
        |where rowid=1
        |
        |
        |
      """.stripMargin)

    rblines_1.createOrReplaceTempView("rblines")

    sparkSession.sql("drop table if exists rblines")
    rblines_1.show(5)


  }




}
