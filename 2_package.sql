----------------------------------------------------
-- Export file for user json                    --
-- Created by lyf on 2016-3-3, 14:02:35 --
----------------------------------------------------

spool JSON.log

prompt
prompt Creating package JSON
prompt =====================
prompt
CREATE OR REPLACE PACKAGE JSON IS

  -- Author  : lyf
  -- Created : 2016-3-2 15:24:38
  -- Purpose : json处理

  -- Public type declarations

  TYPE JSON_TYPE IS TABLE OF VARCHAR2(1000) INDEX BY VARCHAR2(1000); --定义json解析格式
  
    FUNCTION FN_RETRUN(I_RETRUN_NO IN VARCHAR2, I_RETRUN_DATA IN VARCHAR2)
    RETURN VARCHAR2; --日志返回拼写json格式

  FUNCTION FN_SPLIT(P_STR IN VARCHAR2, P_DELIMITER IN VARCHAR2)
    RETURN TY_TBL_STR_SPLIT; --json解析用
	
  FUNCTION PARSEJSON(P_JSONSTR VARCHAR2, P_KEY VARCHAR2) RETURN VARCHAR2;

  FUNCTION CHECK_JSON(P_STR IN JSON_TYPE, P_KEY IN VARCHAR2) RETURN VARCHAR2;

  FUNCTION T_PARSEJSON(P_JSONSTR VARCHAR2 /*,p_key varchar2*/)
    RETURN JSON_TYPE; --json处理

  PROCEDURE SP13_NOTIFY_LOG_TRADE_SHOP(SP_ETL_DATE   IN VARCHAR2, --数据日期
                                       BATCH_DATA_NO IN VARCHAR2 DEFAULT '01', --数据批次
                                       JOB_ID        IN VARCHAR2, --调度的jobid
                                       RESULT1       OUT VARCHAR2 --返回结果
                                       );


END JSON;
/

prompt
prompt Creating package body JSON
prompt ==========================
prompt
CREATE OR REPLACE PACKAGE BODY JSON IS
 
 FUNCTION FN_RETRUN(I_RETRUN_NO IN VARCHAR2, I_RETRUN_DATA IN VARCHAR2)
    RETURN VARCHAR2 AS
    ALL_RETRUN_DATA VARCHAR2(10000);
    --程序说明
    /*--------------------------------------------------------------------------------------
    [Developer]:
    [Developed date]:
    [ETL Frequency]:Daily
    功能: 返回数据拼接,发生错误返回255，以及相关错误数据
    */ --------------------------------------------------------------------------------------
  BEGIN
    --
    SELECT '{"RTNCODE":' || '"' || I_RETRUN_NO || '","RTNMSG":' || '"' ||
           I_RETRUN_DATA || '"}'
      INTO ALL_RETRUN_DATA
      FROM DUAL;
  
    RETURN ALL_RETRUN_DATA;
  EXCEPTION
    WHEN OTHERS THEN
      RETURN '{"RTNCODE":' || '"' || '255' || '","RTNMSG":' || '"' || SQLERRM || '"}';
    
  END;


  FUNCTION FN_SPLIT(P_STR IN VARCHAR2, P_DELIMITER IN VARCHAR2)
    RETURN TY_TBL_STR_SPLIT IS
    J         INT := 0;
    I         INT := 1;
    LEN       INT := 0;
    LEN1      INT := 0;
    STR       VARCHAR2(4000);
    STR_SPLIT TY_TBL_STR_SPLIT := TY_TBL_STR_SPLIT();
  BEGIN
    LEN  := LENGTH(P_STR);
    LEN1 := LENGTH(P_DELIMITER);
  
    WHILE J < LEN LOOP
      J := INSTR(P_STR, P_DELIMITER, I);
    
      IF J = 0 THEN
        J   := LEN;
        STR := SUBSTR(P_STR, I);
        STR_SPLIT.EXTEND;
        STR_SPLIT(STR_SPLIT.COUNT) := TY_ROW_STR_SPLIT(STRVALUE => STR);
      
        IF I >= LEN THEN
          EXIT;
        END IF;
      ELSE
        STR := SUBSTR(P_STR, I, J - I);
        I   := J + LEN1;
        STR_SPLIT.EXTEND;
        STR_SPLIT(STR_SPLIT.COUNT) := TY_ROW_STR_SPLIT(STRVALUE => STR);
      END IF;
    END LOOP;
  
    RETURN STR_SPLIT;
  END FN_SPLIT;
 
  FUNCTION PARSEJSON(P_JSONSTR VARCHAR2, P_KEY VARCHAR2) RETURN VARCHAR2 IS
    RTNVAL    VARCHAR2(1000);
    I         NUMBER(2);
    JSONKEY   VARCHAR2(500);
    JSONVALUE VARCHAR2(1000);
    JSON      VARCHAR2(3000);
  BEGIN
    IF P_JSONSTR IS NOT NULL THEN
      JSON := REPLACE(P_JSONSTR, '{"', '');
      JSON := REPLACE(JSON, '"}', '');

      FOR TEMPROW IN (SELECT STRVALUE AS VALUE
                        FROM TABLE(FN_SPLIT(JSON, ','))) LOOP
        IF TEMPROW.VALUE IS NOT NULL THEN
          I             := 0;
          JSONKEY       := '';
          JSONVALUE     := '';
          TEMPROW.VALUE := RTRIM(TEMPROW.VALUE, '"');
          TEMPROW.VALUE := LTRIM(TEMPROW.VALUE, '"');
          FOR TEM2 IN (SELECT STRVALUE AS VALUE
                         FROM TABLE(FN_SPLIT(TEMPROW.VALUE, '":'))) LOOP
            IF I = 0 THEN
              JSONKEY := TEM2.VALUE;
            END IF;
            IF I = 1 THEN
              JSONVALUE := LTRIM(TEM2.VALUE, '"');
            END IF;
          
            I := I + 1;
          END LOOP;
        
          IF (JSONKEY = P_KEY) THEN
            RTNVAL := JSONVALUE;
          END IF;
        END IF;
      END LOOP;
    END IF;
    RETURN RTNVAL;
  END PARSEJSON;

  FUNCTION CHECK_JSON(P_STR IN JSON_TYPE, P_KEY IN VARCHAR2) RETURN VARCHAR2 IS
    P_RETRUN VARCHAR2(30000);
  BEGIN
    P_RETRUN := P_STR(P_KEY);
    RETURN P_RETRUN;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RETURN NULL;
  END CHECK_JSON;

  FUNCTION T_PARSEJSON(P_JSONSTR VARCHAR2 )
    RETURN JSON_TYPE IS
    I         NUMBER(2);
    JSONKEY   VARCHAR2(500);
    JSONVALUE VARCHAR2(1000);
    JSON      VARCHAR2(3000);
    JSON_ALL  JSON_TYPE;
  BEGIN
    IF P_JSONSTR IS NOT NULL THEN
      JSON := REPLACE(P_JSONSTR, '{"', '');
      JSON := REPLACE(JSON, '"}', '');
      FOR TEMPROW IN (SELECT STRVALUE AS VALUE
                        FROM TABLE(FN_SPLIT(JSON, ','))) LOOP
        IF TEMPROW.VALUE IS NOT NULL THEN
          I             := 0;
          JSONKEY       := '';
          JSONVALUE     := '';
          TEMPROW.VALUE := RTRIM(TEMPROW.VALUE, '"');
          TEMPROW.VALUE := LTRIM(TEMPROW.VALUE, '"');
          FOR TEM2 IN (SELECT STRVALUE AS VALUE
                         FROM TABLE(FN_SPLIT(TEMPROW.VALUE, '":'))) LOOP
            IF I = 0 THEN
              JSONKEY := UPPER(TEM2.VALUE); --????
            END IF;
            IF I = 1 THEN
              JSONVALUE := LTRIM(TEM2.VALUE, '"');
            END IF;
          
            I := I + 1;
          END LOOP;
        
          JSON_ALL(JSONKEY) := JSONVALUE;
        
        END IF;
      END LOOP;
    END IF;
  
    RETURN JSON_ALL;
  END T_PARSEJSON;


  PROCEDURE SP13_NOTIFY_LOG_TRADE_SHOP(SP_ETL_DATE   IN VARCHAR2, 
                                       BATCH_DATA_NO IN VARCHAR2 DEFAULT '01',
                                       RESULT1       OUT VARCHAR2 
                                       ) IS

    /*--------------------------------------------------------------------------------------
    [Developer]:liyf
    [Developed date]:2015/12/14
    [ETL Frequency]:Daily
    */ --------------------------------------------------------------------------------------
  
    V_DATE      DATE; 
    RTNMSG      VARCHAR2(1000); 
    RTNCODE     NUMBER(10, 0); 
    SP_ETL_ID   VARCHAR2(100); 
    CUR_MESSAGE VARCHAR2(4000); 
  
    JSON_ALL JSON_TYPE;
  
  BEGIN
    RTNCODE := 0;
    RTNMSG  := 'SUCCESS';
  
    SELECT TO_DATE(SP_ETL_DATE, 'YYYYMMDD') INTO V_DATE FROM DUAL;
  
    SP_ETL_ID := SP_ETL_DATE || BATCH_DATA_NO;
    BEGIN

      DELETE FROM SHOP; --WHERE DATA_BATCH_ID = SP_ETL_ID; 
    
      BEGIN
        DBMS_OUTPUT.PUT_LINE(SYSDATE);
        --------------------------------------------------------------------
        DECLARE
          CURSOR CUR_MESSAGE IS
            SELECT "message" AS MESSAGE
              FROM SYNC_MESSAGE_501 --@DC
             WHERE ROWNUM <= 100; --"taskID" = JOB_ID;
          C_MESSAGE CUR_MESSAGE%ROWTYPE;
        
        BEGIN
          FOR C_MESSAGE IN CUR_MESSAGE LOOP
          
            JSON_ALL := T_PARSEJSON(C_MESSAGE.MESSAGE);
            DECLARE
              LOGGUID VARCHAR2(64) := NULL;
              --LOGTIME            TIMESTAMP(6) := NULL;
              IDCSTGUID     NUMBER(22) := NULL;
              IDMYGUID      VARCHAR2(64) := NULL;
              IDOPENID      VARCHAR2(32) := NULL;
              IDUNIONOPENID VARCHAR2(32) := NULL;
              IDMEMBERCARD  VARCHAR2(32) := NULL;
              IDMOBILE      VARCHAR2(16) := NULL;
              IDEMAIL       VARCHAR2(64) := NULL;
              IDCARDID      VARCHAR2(32) := NULL;
              IDCARCODE     VARCHAR2(32) := NULL;
              --ACTIONTIME         DATE := NULL;
              SOURCEAPPBIZ       NUMBER(22) := NULL;
              SOURCEAPP          NUMBER(22) := NULL;
              SOURCECHANNEL      VARCHAR2(64) := NULL;
              LONGITUDE          FLOAT := NULL;
              LATITUDE           FLOAT := NULL;
              PROJGUID           VARCHAR2(64) := NULL;
              SHOPGUID           VARCHAR2(64) := NULL;
              TRADEGUID          VARCHAR2(64) := NULL;
              PRODUCTCODE        VARCHAR2(64) := NULL;
              PRODUCTPRICE       FLOAT := NULL;
              PRODUCTCOUNT       FLOAT := NULL;
              PRODUCTCOST        FLOAT := NULL;
              TRADEDISCOUNTPRICE FLOAT := NULL;
              TRADEDISCOUNT      FLOAT := NULL;
              TRADEDISCOUNTMONEY FLOAT := NULL;
              TRADETOTAL         FLOAT := NULL;
              PAYMENTTYPE        NUMBER(22) := NULL;
              REMARK             VARCHAR2(256) := NULL;
              UUID               VARCHAR2(64) := NULL;
              BALANCE            VARCHAR2(64) := NULL;
              BEGINDATE          DATE := NULL;
              --ENDDATE            DATE := NULL;
              BIZSTATE        NUMBER(22) := NULL;
              BPMINSTANCE     VARCHAR2(64) := NULL;
              BPMMESSAGE      VARCHAR2(64) := NULL;
              BPMSTATE        NUMBER(22) := NULL;
              DISCHANGE       FLOAT := NULL;
              INPUTTYPE       NUMBER(22) := NULL;
              MAXDISCHANGE    FLOAT := NULL;
              PERMGROUPID     VARCHAR2(64) := NULL;
              PERMGROUPTITLE  VARCHAR2(64) := NULL;
              RECEIVER        VARCHAR2(64) := NULL;
              SHOPTYPE        VARCHAR2(64) := NULL;
              DM_CREATED_TIME DATE := NULL;
              DATA_BATCH_ID   VARCHAR2(10) := NULL;
            BEGIN
              LOGGUID := CHECK_JSON(JSON_ALL, 'LOGGUID');
              --LOGTIME            := CHECK_JSON(JSON_ALL, 'LOGTIME');
              IDCSTGUID     := CHECK_JSON(JSON_ALL, 'IDCSTGUID');
              IDMYGUID      := CHECK_JSON(JSON_ALL, 'IDMYGUID');
              IDOPENID      := CHECK_JSON(JSON_ALL, 'IDOPENID');
              IDUNIONOPENID := CHECK_JSON(JSON_ALL, 'IDUNIONOPENID');
              IDMEMBERCARD  := CHECK_JSON(JSON_ALL, 'IDMEMBERCARD');
              IDMOBILE      := CHECK_JSON(JSON_ALL, 'IDMOBILE');
              IDEMAIL       := CHECK_JSON(JSON_ALL, 'IDEMAIL');
              IDCARDID      := CHECK_JSON(JSON_ALL, 'IDCARDID');
              IDCARCODE     := CHECK_JSON(JSON_ALL, 'IDCARCODE');
              --ACTIONTIME         := CHECK_JSON(JSON_ALL, 'ACTIONTIME');
              SOURCEAPPBIZ       := CHECK_JSON(JSON_ALL, 'SOURCEAPPBIZ');
              SOURCEAPP          := CHECK_JSON(JSON_ALL, 'SOURCEAPP');
              SOURCECHANNEL      := CHECK_JSON(JSON_ALL, 'SOURCECHANNEL');
              LONGITUDE          := CHECK_JSON(JSON_ALL, 'LONGITUDE');
              LATITUDE           := CHECK_JSON(JSON_ALL, 'LATITUDE');
              PROJGUID           := CHECK_JSON(JSON_ALL, 'PROJGUID');
              SHOPGUID           := CHECK_JSON(JSON_ALL, 'SHOPGUID');
              TRADEGUID          := CHECK_JSON(JSON_ALL, 'TRADEGUID');
              PRODUCTCODE        := CHECK_JSON(JSON_ALL, 'PRODUCTCODE');
              PRODUCTPRICE       := CHECK_JSON(JSON_ALL, 'PRODUCTPRICE');
              PRODUCTCOUNT       := CHECK_JSON(JSON_ALL, 'PRODUCTCOUNT');
              PRODUCTCOST        := CHECK_JSON(JSON_ALL, 'PRODUCTCOST');
              TRADEDISCOUNTPRICE := CHECK_JSON(JSON_ALL,
                                               'TRADEDISCOUNTPRICE');
              TRADEDISCOUNT      := CHECK_JSON(JSON_ALL, 'TRADEDISCOUNT');
              TRADEDISCOUNTMONEY := CHECK_JSON(JSON_ALL,
                                               'TRADEDISCOUNTMONEY');
              TRADETOTAL         := CHECK_JSON(JSON_ALL, 'TRADETOTAL');
              PAYMENTTYPE        := CHECK_JSON(JSON_ALL, 'PAYMENTTYPE');
              REMARK             := CHECK_JSON(JSON_ALL, 'REMARK');
              UUID               := CHECK_JSON(JSON_ALL, 'UUID');
              BALANCE            := CHECK_JSON(JSON_ALL, 'BALANCE');
              --BEGINDATE          := CHECK_JSON(JSON_ALL, 'BEGINDATE');
              BEGINDATE := TO_DATE('20111111 11:11:52',
                                   'yyyymmdd hh24:mi:ss');
              --ENDDATE            := CHECK_JSON(JSON_ALL, 'ENDDATE');
              BIZSTATE        := CHECK_JSON(JSON_ALL, 'BIZSTATE');
              BPMINSTANCE     := CHECK_JSON(JSON_ALL, 'BPMINSTANCE');
              BPMMESSAGE      := CHECK_JSON(JSON_ALL, 'BPMMESSAGE');
              BPMSTATE        := CHECK_JSON(JSON_ALL, 'BPMSTATE');
              DISCHANGE       := CHECK_JSON(JSON_ALL, 'DISCHANGE');
              INPUTTYPE       := CHECK_JSON(JSON_ALL, 'INPUTTYPE');
              MAXDISCHANGE    := CHECK_JSON(JSON_ALL, 'MAXDISCHANGE');
              PERMGROUPID     := CHECK_JSON(JSON_ALL, 'PERMGROUPID');
              PERMGROUPTITLE  := CHECK_JSON(JSON_ALL, 'PERMGROUPTITLE');
              RECEIVER        := CHECK_JSON(JSON_ALL, 'RECEIVER');
              SHOPTYPE        := CHECK_JSON(JSON_ALL, 'SHOPTYPE');
              DM_CREATED_TIME := SYSDATE;
              DATA_BATCH_ID   := TO_CHAR(SYSDATE, 'yyyymmdd');
            
              INSERT INTO SHOP
                (LOGGUID,
                 --LOGTIME,
                 IDCSTGUID,
                 IDMYGUID,
                 IDOPENID,
                 IDUNIONOPENID,
                 IDMEMBERCARD,
                 IDMOBILE,
                 IDEMAIL,
                 IDCARDID,
                 IDCARCODE,
                 -- ACTIONTIME,
                 SOURCEAPPBIZ,
                 SOURCEAPP,
                 SOURCECHANNEL,
                 LONGITUDE,
                 LATITUDE,
                 PROJGUID,
                 SHOPGUID,
                 TRADEGUID,
                 PRODUCTCODE,
                 PRODUCTPRICE,
                 PRODUCTCOUNT,
                 PRODUCTCOST,
                 TRADEDISCOUNTPRICE,
                 TRADEDISCOUNT,
                 TRADEDISCOUNTMONEY,
                 TRADETOTAL,
                 PAYMENTTYPE,
                 REMARK,
                 UUID,
                 BALANCE,
                 BEGINDATE,
                 --ENDDATE,
                 BIZSTATE,
                 BPMINSTANCE,
                 BPMMESSAGE,
                 BPMSTATE,
                 DISCHANGE,
                 INPUTTYPE,
                 MAXDISCHANGE,
                 PERMGROUPID,
                 PERMGROUPTITLE,
                 RECEIVER,
                 SHOPTYPE,
                 DATA_BATCH_ID,
                 DM_CREATED_TIME)
                SELECT LOGGUID,
                       --LOGTIME,
                       IDCSTGUID,
                       IDMYGUID,
                       IDOPENID,
                       IDUNIONOPENID,
                       IDMEMBERCARD,
                       IDMOBILE,
                       IDEMAIL,
                       IDCARDID,
                       IDCARCODE,
                       -- ACTIONTIME,
                       SOURCEAPPBIZ,
                       SOURCEAPP,
                       SOURCECHANNEL,
                       LONGITUDE,
                       LATITUDE,
                       PROJGUID,
                       SHOPGUID,
                       TRADEGUID,
                       PRODUCTCODE,
                       PRODUCTPRICE,
                       PRODUCTCOUNT,
                       PRODUCTCOST,
                       TRADEDISCOUNTPRICE,
                       TRADEDISCOUNT,
                       TRADEDISCOUNTMONEY,
                       TRADETOTAL,
                       PAYMENTTYPE,
                       REMARK,
                       UUID,
                       BALANCE,
                       BEGINDATE,
                       --ENDDATE,
                       BIZSTATE,
                       BPMINSTANCE,
                       BPMMESSAGE,
                       BPMSTATE,
                       DISCHANGE,
                       INPUTTYPE,
                       MAXDISCHANGE,
                       PERMGROUPID,
                       PERMGROUPTITLE,
                       RECEIVER,
                       SHOPTYPE,
                       DATA_BATCH_ID,
                       DM_CREATED_TIME
                  FROM DUAL;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                NULL;
            END;
          END LOOP;
        END;
      END;
      DBMS_OUTPUT.PUT_LINE(SYSDATE);
    END; --END OF BEGIN
  
    COMMIT;
    RESULT1 := FN_RETRUN(RTNCODE, RTNMSG);
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RTNCODE := -1;
      RTNMSG  := SQLERRM;
      RESULT1 := FN_RETRUN(RTNCODE, RTNMSG);
  END SP13_NOTIFY_LOG_TRADE_SHOP;

END;
/


spool off
