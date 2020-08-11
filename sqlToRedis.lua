
local src_table = ast.from[1]['table']
 
-- Log function
local logtable = {}
local function logit(msg)
  logtable[#logtable+1] = msg
end

-- Table of supported sql functions
local sqlFuncTable = {}
sqlFuncTable["aggr_func_COUNT"] =  function(groupBlock, funcArgs)
    return #groupBlock;
end

sqlFuncTable["aggr_func_SUM"] =  function(groupBlock, funcArgs) 
    local sum = 0
    local column = funcArgs["expr"]["args"]["expr"]["column"]
    if funcArgs["expr"].args.expr.type ~= "column_ref" then  
        -- Error do not implement
        error("Error: expr.type = " .. funcArgs.expr.args.expr.type .. " did not implement yet.")
        return {} 
    end
    for rowKey, rowValue in pairs(groupBlock) do
        if rowValue[column] ~= nil then
            sum = sum + rowValue[column]
        end
    end 
    return sum;
end

sqlFuncTable["aggr_func_AVG"] =  function(groupBlock, funcArgs)
    local count = sqlFuncTable["aggr_func_COUNT"](groupBlock, funcArgs);
    if count == 0 then
        return 0
    end
    local sum = sqlFuncTable["aggr_func_SUM"](groupBlock, funcArgs);
    return sum/count
end

sqlFuncTable["aggr_func_MAX"] = function(groupBlock, funcArgs) 
    local max = nil
    local column = funcArgs["expr"]["args"]["expr"]["column"]
    if funcArgs["expr"].args.expr.type ~= "column_ref" then  
        -- Error do not implement
        error("Error: expr.type = " .. funcArgs.expr.args.expr.type .. " did not implement yet.")
        return {} 
    end
    for rowKey, rowValue in pairs(groupBlock) do
        if rowValue[column] ~= nil then
            if max == nil or max < rowValue[column] then
                max = rowValue[column]
            end 
        end
    end 
    return sum;
end

sqlFuncTable["aggr_func_MIN"] =  function(groupBlock, funcArgs) 
    local min = nil
    local column = funcArgs["expr"]["args"]["expr"]["column"]
    if funcArgs["expr"].args.expr.type ~= "column_ref" then  
        -- Error do not implement
        error("Error: expr.type = " .. funcArgs.expr.args.expr.type .. " did not implement yet.")
        return {} 
    end
    for rowKey, rowValue in pairs(groupBlock) do
        if rowValue[column] ~= nil then
            if min == nil or min > rowValue[column] then
                min = rowValue[column]
            end 
        end
    end 
    return sum;
end

-- Supported sql operators table
sqloperatorsTable["="] = function(left, right) 
    logit(" = " .. cjson.encode({left, right}))
    return left == right;
end
sqloperatorsTable[">"] = function(left, right) 
    logit(" > " .. cjson.encode({left, right}))
    return left > right;
end
sqloperatorsTable["<"] = function(left, right) 
    logit(" < " .. cjson.encode({left, right}))
    return left < right;
end

sqloperatorsTable["<>"] = function(left, right) 
    logit(" <> " .. cjson.encode({left, right}))
    return left ~= right;
end
sqloperatorsTable["OR"] = function(left, right) 
    logit(" or " .. cjson.encode({left, right}))
    return left or right;
end
sqloperatorsTable["IS"] = function(left, right) 
    logit(" is " .. cjson.encode({left, right}))
    return left == right;
end
sqloperatorsTable["IS NOT"] = function(left, right) 
    logit(" is not " .. cjson.encode({left, right}))
    return left ~= right;
end

sqloperatorsTable["NOT IN"] = function(left, right)   
    for rowKey, rowValue in pairs(right) do 
        if left == rowValue then 
            return false;
        end
    end 
    return true;
end
sqloperatorsTable["IN"] = function(left, right)   
    for rowKey, rowValue in pairs(right) do 
        if left == rowValue then 
            return true;
        end
    end 
    return false;
end

for rowKey, rowValue in pairs(sqloperatorsTableUsedKeys) do 
    if sqloperatorsTable[rowValue] == nil then 
        error("Error: operator in " .. rowValue .. " did not implement yet.")
    end
end 


-- Converts the table to the view specified by the Select directive
local function toSelectFormat(rawtable) 
    local tres = {} 
    for rowKey, rowValue in pairs(rawtable) do 
        local finalRltrow = {}
        for columnKey, columnAsName in pairs(columnsToNames) do
            -- Name substitution according to the AS directive
            finalRltrow[columnAsName] = rowValue[columnKey]  
        end
        table.insert( tres, finalRltrow)
    end 
    return tres
end

-- Returns the table that is truncated according to the limit section
local function catByLimit(rawtable)
    if queryRowLimit.start == 0 and queryRowLimit.limit == 0 then 
        return rawtable 
    end

    local tres = {} 
    local row_count = 0 
    for rowKey, rowValue in pairs(rawtable) do 
        row_count = row_count + 1

        if row_count > queryRowLimit.limit + queryRowLimit.start and queryRowLimit.limit > 0 then  
            return tres -- Do not select more than specified in the section limit
        end

        if row_count > queryRowLimit.start then  
            table.insert( tres, rowValue) 
        end
    end
    return tres
end



-- GROUP BY
local function getGroupByIndex(row)
    local keyValue = '_' 
    for key, value in ipairs(groupByKeys) do
        if row[value] ~= nil then
            keyValue = keyValue .. row[value]
        else
            keyValue = keyValue .. 'null'
        end
    end
    return keyValue
end

-- Takes all table rows
local function getRowsFromTable(tableName) 
    local matches = redis.call('KEYS', tableName .. '*') 
    for _,key in ipairs(matches) do
        local sJSON = redis.call('GET', key) 
        local tDecoded = cjson.decode(sJSON) 
         
        if whereCheck(tDecoded) then
            coroutine.yield(tDecoded)
        end 
    end 
end

local coroutineRowsFromTable = coroutine.create(getRowsFromTable)


local tPreBuild = {}
local lineNumber=1
while lineNumber<5000 do
    lineNumber = lineNumber + 1
    
    if coroutine.status(coroutineRowsFromTable) ~= "dead" then 
        local b,row = coroutine.resume(coroutineRowsFromTable, src_table)
        if row  ~= nil then  
            local rowIndex = getGroupByIndex(row)
            if tPreBuild[rowIndex]  == nil then 
                -- Creating a group for aggregation functions
                tPreBuild[rowIndex] = {} 
            end  
            table.insert( tPreBuild[rowIndex], row) 
        end  
    else
        break 
    end 
end
  
for groupKey, groupBlock in pairs(tPreBuild) do 
    -- loop for all groups
    local resultrow = {} 
    for rowKey, rowValue in pairs(groupBlock) do
        -- loop for all rows in groups  
        for key, value in pairs(rowValue) do 
            resultrow[key] = value  
        end 
        break -- It is enough to get the data of any row.
    end

    -- Calculating the values of the aggregation functions for the select block
    for _, funcArgs in pairs(aggrFuncsToNames) do
        if sqlFuncTable[funcArgs["func_name"]] ~= nil then
            resultrow[funcArgs.as] = sqlFuncTable[funcArgs["func_name"]](groupBlock, funcArgs)
        else
            -- Error function do not implement
            error("Error: function " .. funcArgs["func_name"] .. " did not implement yet.")
            return {} 
        end
    end
     
    table.insert( tFinal, resultrow) 
end 
