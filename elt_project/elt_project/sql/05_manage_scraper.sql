UPDATE elt_pipeline_configs
SET
    -- 1. Set the parser to use the Selenium logic
    parser_function = 'generic_selenium_scraper',
    
    -- 2. Define the scraping workflow JSON
    scraper_config = '{
        "driver_options": {
            "headless": false
        },
        "login_url": "https://ssologin.cuny.edu/oam/server/obrareq.cgi?encquery%3DbuEvSU77NNhATGCa2aFF%2FElzz%2BcPLV%2FTLPDCW%2BOrakV3N4DNjQSxhO%2FHJQiXyN46oMRHCZ57q3vkNRaucLJL81U32Gyi6QJQfThLlj9a4vzCZwBDG5n02cMuKbb4ms3SRYFuD3Md660uvlgGkdxUaB2u%2BewBT5rEyNp%2Bb3NZQm5gBDikRf%2BdB4j5PxyeRaycwxp1KYrA8OftVIQTVRpXyB2N7E1S4%2Bcq2tG1cDtdiZT%2BootBgPB%2BZL4c8SRBv8XPPntPNwxfz3RDTsUNH4fdA%2B1iUPSzd%2BZFCyBAum86AzWfi90Fktckt7lWn1fErHM6YCJ5iLBcrobyORgaVjsnpH8eEHAbyOMOYReH3EfIy3yYQoPON9NEQ8%2FRJevFiRF9J1Roy3%2FQs7Y8SIKDLJSmzs4WugREMr%2Bxodoc3q%2B8Llf5EYuKHKQ3%2BpjFNt0vvq9HMoBosJsAXljHO2RrNECNTw%3D%3D%20agentid%3DPeopleSoft%20ver%3D1%20crmethod%3D0%26cksum%3D16a9831a8918eb1aff9d98bddc68867bad4af07e&ECID-Context=1.006IFdqoBUf7q205rzx0iY006Pue00062Q%3BkXjE",
        "actions": [
            {
                "type": "find_and_fill",
                "selector": "id",
                "selector_value": "CUNYLoginUsernameDisplay",
                "value_env_var": "vladimir.nimchenko97@login.cuny.edu"
            },
            {
                "type": "find_and_fill",
                "selector": "id",
                "selector_value": "CUNYLoginPassword",
                "value_env_var": "Coolster@4445"
            },
			 {
                "type": "click",
                "selector": "id",
                "selector_value": "submit"
            },
            {
                "type": "find_and_fill_totp",
                "selector": "id",
                "selector_value": "[[$current.data.fieldId]]",
                "totp_secret_env_var": "MY_SITE_TOTP_SECRET"
            },
			 {
                "type": "click",
                "selector": "Verify",
                "selector_value": "Verify"
            },
           "login_url": "https://fscm.cunyfirst.cuny.edu/psp/cnyfsprd/EMPLOYEE/ERP/c/QUERY_MANAGER.SCHED_QUERY.GBL?FolderPath=PORTAL_ROOT_OBJECT.PT_REPORTING_TOOLS.PT_QUERY.PT_SCHED_QUERY_GBL&IsFolder=false&IgnoreParamTempl=FolderPath%2cIsFolder
          ",

		    {
                "type": "find_and_fill",
                "selector": "id",
                "selector_value": "QUERY_RUN_CNTRL_RUN_CNTL_ID",
                "value_env_var": "IMPORT_NAME"
            },
			 {
                "type": "click",
                "selector": "id",
                "selector_value": "PRCSRQSTDLG_WRK_LOADPRCSRQSTDLGPB"
            },

			
     "actions": [
    {
      "type": "switch_to_frame",
      "selector": "id", 
      "selector_value": "ptModFrame_0" 
    },
    {
      "type": "click",
      "selector": "xpath",
      "selector_value": "/html/body/form/div[4]/div[2]/span/a[1]"
    },
    {
      "type": "switch_to_default_content"
    }
  ],
  {
                "type": "click",
                "selector": "id",
                "selector_value": "PRCSRQSTDLG_WRK_LOADPRCSMONITORPB"
   },
   {
  "type": "wait",
  "duration_seconds": 10
  },
    {
                "type": "click",
                "selector": "id",
                "selector_value": "PRCSDETAIL_BTN$0"
   },
    {
                "type": "click",
                "selector": "id",
                "selector_value": "PMN_DERIVED_INDEX_BTN"
   },
     {
                "type": "click",
                "selector": "id",
                "selector_value": "URL$1"
   },
     {
                "type": "click",
                "selector": "id",
                "selector_value": "PSPUSHBUTTONRETURN"
   },
   {
     "type": "select_radio_by_value",
     "group_name": "PMN_DERIVED_DELETEREQUEST",
     "value": "PMN_DERIVED_DELETEREQUEST"
   },
   {
                "type": "click",
                "selector": "id",
                "selector_value": "#ICSave"
   },


     
    }'
WHERE import_name = 'BKL_BUDGET_ALL_VER_1_3'; -- Replace with the specific import you are configuring
