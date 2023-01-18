function insert_bbin_general_info(){
    var project_name = global_constant.project_name;
    var statements = '';
    statements = `
    DELETE ${project_name}.general_information_bbin.Bank_Currency
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Bank_Currency
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM ${project_name}.bbin_data.Bank_Currency ;
    
    DELETE ${project_name}.general_information_bbin.Bank_Info
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Bank_Info
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM ${project_name}.bbin_data.Bank_Info ;

    
    DELETE ${project_name}.general_information_bbin.Currency
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Currency
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM ${project_name}.bbin_data.Currency ;
    
    DELETE ${project_name}.general_information_bbin.Game_Code
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Game_Code
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM ${project_name}.bbin_data.Game_Code;

    DELETE ${project_name}.general_information_bbin.Game_Commission_Group
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Game_Commission_Group
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM ${project_name}.bbin_data.Game_Commission_Group ;


    DELETE ${project_name}.general_information_bbin.Opcode
    Where True ;

    INSERT INTO ${project_name}.general_information_bbin.Opcode
    SELECT
    * EXCEPT(created_time),
    CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
    created_time as original_create_time
    FROM${project_name}.bbin_data.Opcode ;


    INSERT INTO
    ${project_name}.bbin_data.Wager_At
    SELECT
    wager.*
    FROM
    ${project_name}.bbin_data.Wager wager
    LEFT JOIN ${project_name}.bbin_data.game_code_dict game_code_dict ON CASE WHEN wager.lobby in (121,122,1221,1222) THEN 12 ELSE wager.lobby END = game_code_dict.lobby AND wager.game_code = game_code_dict.game_code
    where
    game_code_dict.lobby_group_name not in ('sport', 'lottery')
    and created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} MINUTE));


    INSERT INTO
    ${project_name}.bbin_data.Wager_Settle_At
    SELECT
    wager.*
    FROM
    ${project_name}.bbin_data.Wager wager
    LEFT JOIN ${project_name}.bbin_data.game_code_dict game_code_dict ON CASE WHEN wager.lobby in (121,122,1221,1222) THEN 12 ELSE wager.lobby END = game_code_dict.lobby AND wager.game_code = game_code_dict.game_code
    where
    game_code_dict.lobby_group_name in ('sport', 'lottery')
    and created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} MINUTE));
    `        
    return statements;
} 


function insert_Bank(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempBank As
    SELECT
        *
    FROM ${project_name}.bbin_data.Bank 
        WHERE  created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ;
    `
    for(dataset in global_constant.bbin_datasets_config) { 
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Bank
        SELECT
        bank.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        bank.created_time as original_create_time
        FROM TempBank bank
        WHERE user_id in ( Select user_id from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']} ) 
        or user_id in 
        (Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        ; \n`
        } 
    statements += 'Drop table TempBank ;'        
    return statements;
} 
 

function insert_Cash(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempCash As
    SELECT
        *
    FROM ${project_name}.bbin_data.Cash 
        WHERE  created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ;
    `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Cash
        SELECT
        cash.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        cash.created_time as original_create_time
        FROM
        TempCash cash
        WHERE user_id in (
        Select user_id from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        or user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        ; \n`}
    statements += 'Drop table TempCash ;' 
    return statements;
}


function insert_Cash_Entry(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempCash_Entry As
    SELECT
        *
    FROM ${project_name}.bbin_data.Cash_Entry 
        WHERE  created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ;
    `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Cash_Entry
        SELECT
        Cash_Entry.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Cash_Entry.created_time as original_create_time
        FROM TempCash_Entry Cash_Entry
        WHERE hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempCash_Entry ;' 
    return statements;
}

function insert_Deposit(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempDeposit As
    SELECT
        *
    FROM ${project_name}.bbin_data.Deposit 
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ;
    `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Deposit
        SELECT
        Deposit.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Deposit.created_time as original_create_time
        FROM
        TempDeposit Deposit
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}

    statements += 'Drop table TempDeposit ;' 
    return statements;
}

function insert_Last_Login(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempLast_Login As
    SELECT
        *
    FROM ${project_name}.bbin_data.Last_Login
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `

    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Last_Login
        SELECT
        last_login.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        last_login.created_time as original_create_time
        FROM
        TempLast_Login last_login
        WHERE 
        user_id in (
        Select user_id from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        or
        user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        ; \n`}

    statements += 'Drop table TempLast_Login ;' 
    return statements;
}


function insert_Level(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempLast_Level As
    SELECT
        *
    FROM ${project_name}.bbin_data.Level
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `

    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Level
        SELECT
        Level.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Level.created_time as original_create_time
        FROM TempLast_Level Level
        where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempLast_Level ;' 
    return statements;
}



function insert_Lobby(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempLobby As
    SELECT
        *
    FROM ${project_name}.bbin_data.Lobby
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `

    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Lobby   
        SELECT
        Lobby.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Lobby.created_time as original_create_time
        FROM TempLobby Lobby
        where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}

    statements += 'Drop table TempLobby ;' 
    return statements;
}

function insert_Login(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempLogin As
    SELECT
        *
    FROM ${project_name}.bbin_data.Login
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Login
        SELECT
        Login.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Login.created_time as original_create_time
        FROM
        TempLogin Login
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempLogin ;' 
    return statements;
}

function insert_Removed_User(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempRemoved_User As
    SELECT
        *
    FROM ${project_name}.bbin_data.Removed_User
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Removed_User
        SELECT
        removed_user.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        removed_user.created_time as original_create_time
        FROM TempRemoved_User removed_user
        WHERE 
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempRemoved_User ;' 
    return statements;
}

function insert_Removed_User_Detail(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempRemoved_User_Detail As
    SELECT
        *
    FROM ${project_name}.bbin_data.Removed_User_Detail
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO 
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Removed_User_Detail
        SELECT
        removed_user_detail.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        removed_user_detail.created_time as original_create_time
        FROM
        TempRemoved_User_Detail removed_user_detail
        WHERE user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View 
        where 
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']} )
        ; \n`}

    statements += 'Drop table TempRemoved_User_Detail ;' 
    return statements;
}

function insert_Removed_User_Email(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempRemoved_User_Email As
    SELECT
        *
    FROM ${project_name}.bbin_data.Removed_User_Email
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Removed_User_Email
        SELECT
        removed_user_email.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        removed_user_email.created_time as original_create_time
        FROM
        TempRemoved_User_Email removed_user_email
        WHERE user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View 
        where 
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        ; \n`}
    
    statements += 'Drop table TempRemoved_User_Email ;' 
    return statements;
}

function insert_User(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempUser As
    SELECT
        *
    FROM ${project_name}.bbin_data.User
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.User
        SELECT
        user.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        user.created_time as original_create_time
        FROM TempUser user
        where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempUser ;' 
    return statements;
}

function insert_User_Detail(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempUser_Detail As
    SELECT
        *
    FROM ${project_name}.bbin_data.User_Detail
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.User_Detail
        SELECT
        user_detail.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        user_detail.created_time as original_create_time
        FROM
        TempUser_Detail user_detail
        WHERE 
        user_id in (
        Select user_id from ${project_name}.bbin_data.User_View 
        where 
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        or
        user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${query_minutes}
        )
        ; \n`}
    statements += 'Drop table TempUser_Detail ;' 
    return statements;
}


function insert_User_Email(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempUser_Email As
    SELECT
        *
    FROM ${project_name}.bbin_data.User_Email
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.User_Email
        SELECT
        user_email.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        user_email.created_time as original_create_time
        FROM
        TempUser_Email user_email
        WHERE 
        user_id in (
        Select user_id from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        )
        or
        user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        ; \n`}

    statements += 'Drop table TempUser_Email ;' 
    return statements;
}


function insert_User_Has_Deposit_Withdraw(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempUser_Has_Deposit_Withdraw As
    SELECT
        *
    FROM ${project_name}.bbin_data.User_Has_Deposit_Withdraw
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `

    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.User_Has_Deposit_Withdraw
        SELECT
        uhdw.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        uhdw.created_time as original_create_time
        FROM
        TempUser_Has_Deposit_Withdraw uhdw
        WHERE user_id in (
        Select user_id from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']})
        or
        user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}) 
        ; \n`}

     statements += 'Drop table TempUser_Has_Deposit_Withdraw ;' 
    return statements;
}


function insert_User_Level(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempUser_Level As
    SELECT
        *
    FROM ${project_name}.bbin_data.User_Level
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.User_Level
        SELECT
        user_level.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        user_level.created_time as original_create_time
        FROM
        TempUser_Level user_level
        WHERE user_id in (
        Select user_id 
        from ${project_name}.bbin_data.User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']} )
        or 
        user_id in (
        Select user_id from ${project_name}.bbin_data.Removed_User_View where hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']} )
        ; \n`}
    statements += 'Drop table TempUser_Level ;' 
    return statements;
}


function insert_Wager(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempWager As
    SELECT
        *
    FROM ${project_name}.bbin_data.Wager
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `    
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Wager
        SELECT
        Wager.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Wager.created_time as original_create_time
        FROM
        TempWager Wager
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    
    statements += 'Drop table TempWager ;' 
    return statements;
}

function insert_Wager_At(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempWager_At As
    SELECT
        *
    FROM ${project_name}.bbin_data.Wager_At
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `        
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Wager_At
        SELECT
        wager.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        created_time as original_create_time
        FROM
        TempWager_At wager
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempWager_At ;'
    return statements;
}

function insert_Wager_Settle_At(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempWager_Settle_At As
    SELECT
        *
    FROM ${project_name}.bbin_data.Wager_Settle_At
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `  
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Wager_Settle_At
        SELECT
        wager_settle.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        created_time as original_create_time
        FROM
        TempWager_Settle_At  wager_settle
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempWager_Settle_At ;'
    return statements;
}


function insert_Withdraw(){
    var statements = '';
    statements = `
    CREATE TEMP TABLE TempWithdraw As
    SELECT
        *
    FROM ${project_name}.bbin_data.Withdraw
    WHERE created_time >= (DATE_ADD(DATE_ADD(CURRENT_DATETIME(), INTERVAL 8 HOUR), INTERVAL -${query_minutes} minute)) ; `  
    for(dataset in global_constant.bbin_datasets_config) {
        statements += `
        INSERT INTO
        ${project_name}.${global_constant.bbin_datasets_config[dataset]['dw_dataset']}.Withdraw
        SELECT
        Withdraw.* EXCEPT(created_time),
        CAST(CONCAT(CURRENT_DATE('Asia/Taipei'),"T",CURRENT_TIME('Asia/Taipei')) AS DATETIME ) AS created_time,
        Withdraw.created_time as original_create_time
        FROM
        TempWithdraw  Withdraw
        WHERE
        hall_id = ${global_constant.bbin_datasets_config[dataset]['id_']}
        ; \n`}
    statements += 'Drop table TempWithdraw  ;'
    return statements;
}



module.exports = {
    insert_bbin_User_tag,
    insert_bbin_general_info,
    insert_Bank,
    insert_Cash,
    insert_Cash_Entry,
    insert_Deposit,
    insert_Last_Login,
    insert_Level,
    insert_Lobby,
    insert_Login,
    insert_Removed_User,
    insert_Removed_User_Detail,
    insert_Removed_User_Email ,
    insert_User,
    insert_User_Detail,
    insert_User_Email,
    insert_User_Has_Deposit_Withdraw,
    insert_User_Level,
    insert_Wager,
    insert_Wager_At,
    insert_Wager_Settle_At,
    insert_Withdraw};