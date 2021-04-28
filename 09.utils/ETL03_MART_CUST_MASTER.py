#-*-coding:utf-8-*-

# =========================================== 
# *** ETL03_MART_CUST_MASTER.py 
#        (from ETL03_MART_CUST_MASTER.sas)
class ETL03: 
    
    def __init__(self, ORACLE_ID, ORACLE_PW, DB_HOST, DB_PORT, DB_SID, logger, STATUS_='BATCH'): 
        # =========================================== 
        # proc 1.) Analysis Settings
        # =========================================== 
        self.logger = logger
        self.logger.debug('ETL03 Log Instance Created')
        self.logger.info('proc 1.) Analysis Settings ') 
        self.CONNECTION_INFO = f"{ORACLE_ID}/{ORACLE_PW}@{DB_HOST}:{DB_PORT}/{DB_SID}"
        self.STATUS_ = STATUS_

    # def run(self, base_ym, exec_date, ANA_LIB_PATH, MART_KCB_INDEX_base_ym):
    def run(self, base_ym, exec_date, ANA_LIB_PATH):
        # # Set instances
        # base_ym
        # exec_date
        CONNECTION_INFO = self.CONNECTION_INFO
        read_oracle = ut.read_oracle
        logger = self.logger
        STATUS_ = self.STATUS_
        
        # # Load Prerequisite ANA_LIB Table    # <<< (Deprecated)
        # MART_KCB_INDEX_base_ym = MART_KCB_INDEX_base_ym
        
        # =========================================== 
        # * Settings for test 
        # =========================================== 
        # Sample clients for test 
        if STATUS_ == 'TEST':
            clnt_list = pd.read_csv(ANA_LIB_PATH+f'TEST_CLNT_B{base_ym}_V{exec_date}.csv', encoding='utf-8')
            test_clnts = "', '".join(clnt_list['GRP_MD_NO'])
            test_clnts = str("'"+test_clnts+"'")    # "'4EA4E517651A....', '4EA53186F...', '...
            self.logger.debug(f'{len(clnt_list)} Clients for Sample Test has been collected. ')
        else :
            clnt_list = None
            test_clnts = None
        
        # Main Proc ----------------------------------------------------------------------
        # # =========================================== 
        # # proc 2.) Main Process
        # # =========================================== 
        logger.info('proc 2.) Start of Main Process. ')
        
        try: 
            # Main try ======================================================================
            # ----------------------------------------------------------------------
            # # Load mart datasets 
            # if STATUS_ == 'TEST': 
            
            # Load Prerequisite ANA_LIB Table 
            MART_KCB_INDEX_base_ym = ANA_LIB.load_MART_KCB_INDEX_base_ym(base_ym, ANA_LIB_PATH, STATUS_) 

            # *****************************************************************************
            # * 기준월의 JDC_월고객현황(JDC022T00)으로 고객기본정보 생성                  *
            # *****************************************************************************
            #------------------------------------------TASK 2
            # *** No proper utils for 'add_month' functions: used customized functions. 
            # =====================================================================================================================================
            # 1. extract 'MM' -> calculate "int('MM') - int(5)"                 | ex.1) base_ym = '202011'   | ex.1) base_ym = '202003'          | 
            # 2. if 'MM-5' <= 0 , then 'MM-5' + int(12) & int('YYYY') - int(1)  |       11-5 = 6 (:ie. > 0)  |       3-5 = -2 (:ie. <= 0)        | 
            #    else           , then 'MM-5'           & int('YYYY')           |                            |       (-2)+12 = 10 & 2020-1 = 2019| 
            # 3. add 'YYYY'+'MM'                                                |       '202006'             |       '201910'                    | 
            # =====================================================================================================================================
            # ===========================================================
            def add_month(base_ym, _interval):
                _YYYY = int(str(base_ym)[:4])
                _MM_minus = int(str(base_ym)[-2:])+_interval

                if _MM_minus <= 0:
                    _MM_minus += 12
                    _YYYY -= 1

                _MM_minus = str('0'+str(_MM_minus)) if len(str(_MM_minus))==1 else _MM_minus
                deducted_YM = str(_YYYY)+str(_MM_minus)

                return(deducted_YM)
            # ===========================================================

            base_ym_6m = add_month(base_ym, -5)    # '202007' = add_month('202012', -5)

            #--------------------------------------
            # # PROC SQL; CUST MASTER 생성     
            #--------------------------------------
            query = f"""
            """

            CUST_MASTER = read_oracle(query, CONNECTION_INFO)
            
            # For data integrity
            CUST_MASTER = CUST_MASTER.drop_duplicates('GRP_MD_NO').reset_index(drop=True)


            # *****************************************************************************
            # * 기준월 고객마스터 생성                                                    *
            # *****************************************************************************
            MART_CUST_MASTER_base_ym = CUST_MASTER[['BASE_YM','GRP_MD_NO','PERS_YN','CORP_YN','SHIC_SRVC_TOPS_CODE',
                                                    'SHIC_PRFMC_TOPS_CODE','INTRGRT_TOPS_SRVC_CODE','INTRGRT_TOPS_PFRMC_CODE',
                                                    'ACT_GRD_CODE_MAX','ACT_GRD_CODE_MIN','CONTN_TRD_CUS_YN','BANK_INTRD_YN',
                                                    'GEND_CODE','AGE','JOIN_YMD','JOIN_MCNT','FRGN_YN','FTR_OPT_TRD_CUS_YN',
                                                    'MANG_CUST_YN','MJTSHOL_YN','SHI_EMPL_YN','SHI_EMPL_FML_YN','EXP_FMLY_YN',
                                                    'RPST_VISOR_MEMB','RPST_MANG_DBRN_NM','BRAN_TP_CODE','REGN_HEADQ_NM',
                                                    'NEW_CUST_YN','EXCLUDE_YN']]  

            uniq_cols = list(set(MART_KCB_INDEX_base_ym.columns) - set(['BASE_YM']))
            MART_CUST_MASTER_base_ym = pd.merge(MART_CUST_MASTER_base_ym, MART_KCB_INDEX_base_ym[uniq_cols],
                                                 left_on='GRP_MD_NO', right_on='GRP_MD_NO', how='left')

            # columns 'KCB_DATA_EXIST_YN'
            kcb_exist_list = MART_KCB_INDEX_base_ym['GRP_MD_NO'].dropna().unique()

            MART_CUST_MASTER_base_ym['KCB_DATA_EXIST_YN'] = MART_CUST_MASTER_base_ym['GRP_MD_NO'].isin(kcb_exist_list)
            MART_CUST_MASTER_base_ym.loc[MART_CUST_MASTER_base_ym['KCB_DATA_EXIST_YN']==True, 'KCB_DATA_EXIST_YN'] = 'Y'
            MART_CUST_MASTER_base_ym.loc[MART_CUST_MASTER_base_ym['KCB_DATA_EXIST_YN']==False,'KCB_DATA_EXIST_YN'] = 'N'

            # %MEND CUST;  
            # %CUST(BASE_YM=&base_ym.);  
            # ----------------------------------------------------------------------
            # =========================================== 
            # proc 3.) Export 
            # =========================================== 
            # /* 작업종료 상태 체크 */
            logger.debug("Proc 3.) Export")
            
            # Save Analysis Mart Table to Local Dir. 
            MART_CUST_MASTER_base_ym.to_csv(ANA_LIB_PATH+f'MART_CUST_MASTER_{base_ym}.csv', index=False, encoding='utf-8') 

            # Main Proc ----------------------------------------------------------------------
            logger.info('End of Process')
            
            return(MART_CUST_MASTER_base_ym)

        # Main try ======================================================================
        except Exception as ex: 
            logger.error(ex)