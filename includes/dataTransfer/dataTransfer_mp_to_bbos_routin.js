// global constant
var query_days = 1;
var query_minutes = 120;


function transfer_Bank() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Bank
  SELECT
    id,
    name AS name,
    abbr AS abbr,
    enable AS enable,
    metadata_op AS flag,
    metadata_binlog_time AS  original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
    ${project_name}.XBB_pineapple_final.bank
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))`
  return statements;
}


function transfer_Cash() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Cash
  SELECT
    user_id,
    currency,
    CAST(balance as NUMERIC) as balance,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
    ${project_name}.XBB_pineapple_final.cash

  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Cash_Entry() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Cash_Entry
  SELECT
    domain as domain_id,
    id as INTEGER,
    user_id as user_id,
    parent_id as parent_id,
    currency  as currency,
    opcode as  opcode,
    CAST(amount as NUMERIC) as amount,
    CAST(balance as NUMERIC) as balance,
    trans_id as trans_id,
    ref_id  as ref_id,
    DATE_SUB(CAST(CAST(created_at as STRING)  AS DATETIME FORMAT 'YYYYMMDDHH24MIss'), INTERVAL 12 HOUR) as created_at,
    operator as operator,
    memo as memo,
    internal_memo as internal_memo,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.cash_entry
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Currency() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Currency
SELECT
id as id,
currency as currency,
CAST(exchange_rate as NUMERIC) as exchange,
effective_time  AS datetime,
metadata_op as flag,
metadata_binlog_time AS  original_time,
CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
  \`${project_name}.XBB_pineapple_hoster_final.exchange_rate\`
  `
  return statements;
}

function transfer_Deposit() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Deposit
SELECT
  domain as domain_id,
  id as id,
  user_id as user_id,
  submit_at ,
  DATE_SUB(CAST(CAST(\`at\` as string)  AS DATETIME FORMAT 'YYYYMMDDHH24MIss'), INTERVAL 12 HOUR),
  confirm_at,
  parent_id as parent_id,
  status as status,
  first as first,
  unfinished as unfinished,
  invoice_id as invoice_id,
  entry_id as entry_id,
  CAST(amount as NUMERIC) as amount,
  CAST(fee as NUMERIC) as fee,
  ref_id as ref_id,
  opcode as opcode,
  metadata_op as flag,
  metadata_binlog_time AS original_time,
  CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_final.invoice_entry
WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Domain() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Domain
  SELECT
    CAST(id as INTEGER) as id,
    name as name ,
    code as code ,
    enable as enable,
    test as test,
    currency as currency,
    CAST(TIMESTAMP_MILLIS(CAST(CAST(created_at AS STRING) AS bigint) - 12 * 3600000) AS datetime) as created_at,
    metadata_binlog_time as original_time,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_hoster_final.domain
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Domain_Config() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Domain_Config
SELECT
  domain AS domain_id,
  ingroup_transfer AS ingroup_transfer,
  metadata_op AS flag,
  metadata_binlog_time AS original_time,
  CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.domain_config
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Domain_Vip_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Domain_Vip_Level
  SELECT
    id as id,
    domain as domain_id,
    config_id as config_id,
    seq as seq,
    metadata_op as flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.vip2_level
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Domain_Vip_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Domain_Vip_Level
  SELECT
    id as id,
    domain as domain_id,
    config_id as config_id,
    seq as seq,
    metadata_op as flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.vip2_level
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Execution_Log() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Execution_Log
  SELECT
    domain as domain_id,
    id as id,
    table_name as table_name,
    major_key as major_key,
    method as method,
    \`at\` AS datetime,
    message as message,
    operator as operator,
    item_id as item_id,
    ip as  ip,
    country as country,
    city as city,
    CAST(city_id as INTEGER) as city_id,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.execution_log
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Geoip_Country() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Geoip_Country
  SELECT
    CAST(id as INTEGER) as id,
    country as country,
    en_name as en_name,
    zh_tw_name as zh_tw_name,
    zh_cn_name as zh_cn_name,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_hoster_final.geoip_country
WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Geoip_Region() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Geoip_Region
  SELECT
    CAST(id as INTEGER) as id,
    CAST(country_id as INTEGER) as country_id,
    country as country, 
    region as region,
    en_name as en_name,
    zh_tw_name as zh_tw_name, 
    zh_cn_name as zh_cn_name,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_hoster_final.geoip_region
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Invoice_Detail() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Invoice_Detail
  SELECT 
    CAST(invoice_id AS INTEGER) AS invoice_id, 
    data,
    dirty_data,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.invoice_detail
WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Invoice() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Invoice
  SELECT
    domain as domain_id,
    id as id,
    user_id  as user_id,
    CAST(amount as NUMERIC) as amount,
    opcode as opcode,
    created_at as created_at ,
    ref_id as ref_id,
    metadata_op as metadata_op,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_final.invoice
WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Jackpots() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Jackpots
  SELECT
    domain as domain_id,
    CAST(id as string) as id,
    DATE_SUB(CAST(CAST(\`at\` as string)  AS DATETIME FORMAT 'YYYYMMDDHH24MIss'), INTERVAL 12 HOUR),
    vendor as vendor,
    CAST(kind as STRING) as kind,
    game_code as game_code,
    grade as grade,
    ref_id as ref_id,
    deal as deal ,
    parent_id as parent_id,
    user_id as user_id,
    CAST(bet as NUMERIC) as bet,
    CAST(payoff as NUMERIC) as payoff,
    CAST(currency as STRING) as kind,
    CAST(exchange_rate as NUMERIC) as exchange_rate,
    account_at AS account_at,
    modified_at AS modified_at,
    CAST(version as INTEGER) as version,
    metadata_op as flag ,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_Wagers_final.jackpot_all
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Level
  SELECT
    id as level_id,
    domain as domain_id,
    name as level_name,
    role as role,
    enable as enable,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.level
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Login() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Login
  SELECT
    CAST(domain AS INTEGER) AS domain_id,
    CAST(id AS INTEGER) AS id,
    CAST(user_id AS INTEGER) AS id,
    CAST(role AS INTEGER) AS role,
    username AS username,
    ip AS ip,
    ipv6 AS ipv6,
    host AS host,
    client_os AS client_os,
    client_browser AS client_browser,
    country AS country,
    city AS city,
    CAST(city_id AS INTEGER) AS city_id,
    CAST(result AS INTEGER) AS result,
    CAST(ingress AS INTEGER) AS ingress,
    CAST(entrance AS INTEGER) AS entrance,
    region AS region,
    user_agent AS user_agent,
    \`at\`  AS datetime,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.login_log
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Oauth2() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Oauth2
  SELECT
    CAST(id AS INTEGER) AS id,
    name AS name,
    client_id AS client_id,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_hoster_final.oauth2
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Opcode() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Opcode
  SELECT
    opcode AS opcode,
    name as name,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
    \`${project_name}.XBB_pineapple_final.opcode\`
  where
   created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}



function transfer_Promotion() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Promotion
  SELECT
    code AS code,
    domain AS domain_id,
    user_id AS user_id,
    url as url,
    enable AS enable,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.promotion
  WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Rebate_Dispatch() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Rebate_Dispatch
  SELECT
    id AS id,
    rebate_at AS rebate_at,
    event_id AS event_id,
    level_id AS level_id,
    vip_config_id AS vip_config_id,
    vip_id AS vip_id,
    group_id AS group_id,
    version AS version,
    stat AS stat,
    done AS done,
    canceled AS canceled,
    restat AS restat,
    restat_version AS restat_version,
    stat_at AS stat_at,
    done_at AS done_at,
    start_canceled_at AS start_canceled_at,
    canceled_at AS canceled_at,
    created_at AS created_at,
    modified_at AS  modified_at,
    operator AS operator,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.rebate_dispatch
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Rebate_Entry_List() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Rebate_Entry_List
  SELECT
    id AS id,
    event_id AS event_id,
    user_id  AS user_id,
    rebate_at AS rebate_at,
    level_id AS level_id,
    CAST(amount AS NUMERIC) AS amount,
    CAST(original_amount AS NUMERIC) AS original_amount,
    canceled AS canceled,
    vip_id AS vip_id,
    vip_config_id AS vip_config_id,
    group_id AS group_id,
    self AS self,
    version AS version,
    CAST(valid_bet AS NUMERIC) AS valid_bet,
    CAST(sport AS NUMERIC) AS sport,
    CAST(live AS NUMERIC) AS live,
    CAST(slots AS NUMERIC) AS slots,
    CAST(lottery AS NUMERIC) AS lottery,
    CAST(card AS NUMERIC) AS card,
    CAST(mahjong AS NUMERIC) AS mahjong,
    created_at AS created_at,
    modified_at AS modified_at,
    rebate_done_at AS rebate_done_at,
    canceled_at AS canceled_at,
    metadata_op as flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.rebate_entry
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_Rebate_Entry_List_Detail() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data' ;  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Rebate_Entry_List_Detail
  SELECT
    id AS id,
    entry_id AS entry_id,
    kind AS kind,
    vendor AS vendor_id,
    type AS type,
    CAST(valid_bet AS NUMERIC) AS valid_bet,
    CAST(amount AS NUMERIC) AS rebate,
    CAST(original_amount AS NUMERIC) AS original_amount,
    exception AS exception,
    metadata_op as flag,
    metadata_binlog_time AS metadata_binlog_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.rebate_detail
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Rebate_Event() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data' ;    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Rebate_Event
  SELECT
    domain AS domain_id,
    id AS id,
    name AS event_name,
    frequency AS frequency,
    self AS self,
    vip AS vip,
    end_at AS end_at,
    metadata_op AS flag,
    metadata_binlog_time  AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_final.rebate_event
WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User
  SELECT
    domain AS  domain_id,
    id AS user_id,
    username,
    name,
    upper_id AS upper_id,
    role as role,
    enable as enable ,
    bankrupt as bankrupt,
    tied as tied,
    locked as locked,
    last_login AS last_login,
    last_ip as last_ip ,
    last_online AS last_online ,
    blacklist_modified_at AS blacklist_modified_at,
    parent_id as locked,
    checked as checked,
    failed as failed,
    metadata_op as flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM 
${project_name}.XBB_pineapple_final.user
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_User2_Vip_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'  
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User2_Vip_Level
  SELECT  
    domain as domain_id ,
    user_id as user_id,
    config_id as config_id,
    vip_id as vip_id,
    own as own,
    CAST(locked as STRING) as locked,
    metadata_op,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM 
  ${project_name}.XBB_pineapple_final.vip2_user
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_User_Address() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Address
  SELECT 
    id as id,
    user_id as user_id,
    domain as domain_id,
    is_default as is_default,
    name as name,
    phone as phone ,
    address as address  ,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM 
${project_name}.XBB_pineapple_final.user_address
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Bank() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Address
  SELECT 
    id as id,
    user_id as user_id,
    domain as domain_id,
    is_default as is_default,
    name as name,
    phone as phone ,
    address as address  ,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM 
${project_name}.XBB_pineapple_final.user_address
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_User_Blacklist() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Blacklist
  SELECT 
    CAST(user_id as INTEGER) as user_id  ,
    CAST(blacklist_id as INTEGER) as blacklist_id ,
    created_at as created_at,
    metadata_op as flag,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM 
${project_name}.XBB_pineapple_final.user_blacklist
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_User_Config() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data'    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Config
  SELECT
    user_id AS user_id,
    ingroup_transfer AS ingroup_transfer,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_final.user_config
WHERE
  metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Contact() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Contact
  SELECT
    CAST(id AS INTEGER) AS id,
    CAST(user_id AS INTEGER) AS user_id,
    CAST(contact_id AS INTEGER) AS contact_id,
    value as value,
    modified_at AS modified_at,
    metadata_op,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_contact
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Created() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Created
  SELECT
    CAST(user_id AS INTEGER) AS user_id,
    at AS datetime,
    ip as ip ,
    country as country,
    region as region,
    city as city,
    created_by AS created_by,
    register_channel AS register_channel,
    metadata_op,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_created
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Detail() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Detail
  SELECT
    user_id AS user_id,
    alias as alias,
    birthday AS birthday,
    gender AS gender,
    image_id AS image_id,
    custom_image_id AS custom_image_id,
    custom AS custom ,
    content_rating AS content_rating,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_detail
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_User_Email() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';  

  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Email
  SELECT
    CAST(user_id AS INTEGER) AS user_id,
    email as email,
    CAST(confirm AS INTEGER) AS confirm,
    confirm_at AS confirm_at,
    metadata_op as flag ,
    metadata_binlog_time as original_time ,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
FROM
${project_name}.XBB_pineapple_final.user_email
WHERE
  metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Level
  SELECT
    user_id AS user_id,
    level_id AS level_id,
    metadata_op as flag ,
    metadata_binlog_time as original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_level
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}


function transfer_User_Phone() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Phone
  SELECT
    user_id AS user_id,
    phone As phone,
    confirm AS confirm,
    confirm_at as confirm_at  ,
    metadata_op as flag ,
    metadata_binlog_time As original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_phone
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}



function transfer_User_Stat() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.User_Stat
  SELECT
    CAST(user_id AS INTEGER) AS user_id,
    last_deposit_at AS last_deposit_at,
    metadata_op as flag ,
    metadata_binlog_time As original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.user_stat
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes}  minute ))
  `
  return statements;
}


function transfer_User_Vip_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Vip_Level
  SELECT
    domain As domain_id,
    id AS user_id,
    complex As complex,
    name as name ,
    enable As enable,
    display AS display,
    modified_at As modified_at,
    metadata_op As flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.vip2_config
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}

function transfer_Vip_Level() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Vip_Level
  SELECT
  domain As domain_id,
  id AS user_id,
  complex As complex,
  name as name ,
  enable As enable,
  display AS display,
  modified_at As modified_at,
  metadata_op As flag,
  metadata_binlog_time AS original_time,
  CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.vip2_config
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
  `
  return statements;
}



function transfer_Wager() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Wager
  SELECT
    domain AS domain_id,
    id AS id,
    DATE_SUB(CAST(CAST(\`at\` AS STRING) AS DATETIME FORMAT 'YYYYMMDDHH24MIss'), INTERVAL 12 HOUR) As \`at\`,
    vendor AS vendor,
    kind AS kind,
    game_code as game_code,
    ref_id as ref_id ,
    deal As deal,
    CAST(parent_id AS INTEGER) As parent_id,
    CAST(user_id AS INTEGER) As user_id,
    CAST(device AS INTEGER) As device,
    os AS os,
    CAST(bet AS NUMERIC) As bet,
    CAST(payoff AS NUMERIC) As payoff,
    CAST(valid_bet AS NUMERIC) As valid_bet,
    CAST(commision AS NUMERIC) As commision,
    CAST(currency AS INTEGER) As currency,
    CAST(exchange_rate AS NUMERIC) As exchange_rate,
    CAST(result AS INTEGER) As result,
    settle_at AS settle_at,
    account_at AS account_at,
    modified_at AS modified_at,
    first_at AS first_at,
    version AS version,
    metadata_op AS flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_Wagers_final.wager_all
  WHERE
    metadata_binlog_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
   `
  return statements;
}


function transfer_Withdraw() {
  var statements = "";
  var project_name = global_constant.ai_dev_pid;
  var dataset_name = 'bbos_data';    
  statements = `
  INSERT INTO
    ${project_name}.${dataset_name}.Withdraw
  SELECT
    CAST(domain AS INTEGER) As domain_id,
    CAST(id AS INTEGER) As id,
    DATE_SUB(CAST(CAST(\`at\` AS STRING) AS DATETIME FORMAT 'YYYYMMDDHH24MIss'), INTERVAL 12 HOUR) As \`at\`,
    user_id As user_id,
    parent_id AS parent_id,
    kind AS kind,
    CAST(amount AS NUMERIC) as amount,
    CAST(offer AS NUMERIC) as offer,
    CAST(real_amount AS NUMERIC) as real_amount,
    currency as currency,
    process as process,
    confirm as confirm,
    cancel as cancel,
    reject as reject,
    locked as locked,
    risk_auto as risk_auto,
    risk_confirm as risk_confirm,
    risk_cancel as risk_cancel,
    risk_reject as risk_reject,
    holding as holding,
    operator as operator,
    holding_operator as holding_operator,
    account as account,
    address as address,
    first as first,
    CAST(fee as NUMERIC) as fee,
    CAST(administrative_amount as NUMERIC) as administrative_amount,
    CAST(offer_deduction as NUMERIC) as offer_deduction,
    user_virtual_bank_id as user_virtual_bank_id,
    user_bank_id as  user_bank_id,
    confirm_at AS confirm_at,
    client_id as client_id ,
    metadata_op as flag,
    metadata_binlog_time AS original_time,
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time
  FROM
  ${project_name}.XBB_pineapple_final.withdraw_entry
  WHERE
    metadata_binlog_time  >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute ))
   `
  return statements;
}


module.exports = {
    transfer_Bank,
    transfer_Cash,
    transfer_Cash_Entry,
    transfer_Currency,
    transfer_Deposit,
    transfer_Domain,
    transfer_Domain_Config,
    transfer_Domain_Vip_Level,
    transfer_Execution_Log,
    transfer_Geoip_Country,
    transfer_Geoip_Region,
    transfer_Invoice_Detail,
    transfer_Invoice,
    transfer_Jackpots,
    transfer_Level,
    transfer_Login,
    transfer_Oauth2,
    transfer_Opcode,
    transfer_Promotion,
    transfer_Rebate_Dispatch,
    transfer_Rebate_Entry_List,
    transfer_Rebate_Entry_List_Detail,
    transfer_Rebate_Event,
    transfer_User,
    transfer_User2_Vip_Level,
    transfer_User_Address,
    transfer_User_Bank,
    transfer_User_Blacklist,
    transfer_User_Config,
    transfer_User_Contact,
    transfer_User_Created,
    transfer_User_Detail,
    transfer_User_Email,
    transfer_User_Level,
    transfer_User_Phone,
    transfer_User_Stat,
    transfer_User_Vip_Level,
    transfer_Vip_Level,
    transfer_Wager,
    transfer_Withdraw
    };
