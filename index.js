const { Parser } = require('node-sql-parser'); 
 
let luaBody;
/**
 * Takes the main body of a lua script from a file
 */
function getLuaBody() {

  if(luaBody)
  {
    return luaBody
  }
  const fs = require('fs')
  const path = require('path') 
  luaBody = fs.readFileSync(path.resolve(__dirname, 'sqlToRedis.lua'), 'utf8')
  return luaBody
}

function sqlToLua(query, queryArgsValues, ast) {
  console.log(query, "\n" ,queryArgsValues, "\n" , JSON.stringify(ast)) 

  let groupByKeys = [];
  let columnsToNames = {};
  let aggrFuncsToNames = [];
  let orderByColumns = '';

  // Prepare data for select stamet
  if(ast.columns)
  {
      ast.columns.forEach(element => { 
          if(element.expr.type == 'column_ref')
          {
              if(!element.as)
              {
                  element.as = element.expr.column
              }
              columnsToNames[element.expr.column] = element.as
          }
          else if(element.expr.type == 'aggr_func')
          {
              if(!element.as)
              {
                  element.as = `${element.expr.name}(${element.expr.args.expr.value})`.toLowerCase()
              }
              element.func_name = `${element.expr.type}_${element.expr.name}`

              aggrFuncsToNames.push(element)
              columnsToNames[element.as] = element.as
          }
          else{
              throw `Not implement select expr type: ${element.expr.type}`
          }
      })
  }

  // Prepare data for WHERE stamet
  let sqloperatorsTableUsedKeys = [] 
  function whereCheckCode(arg){  
      if(arg.type == "column_ref") {
          return `rawRow["${arg.column}"]`
      }
      else if(arg.type == "null") {
          if(arg.value == "null"){
              return `nil`
          }
          return `"${arg.value}"`
      }
      else if(arg.type == "string") {
          if(/^queryArgsValues\[[0-9]+\]$/.test(arg.value)){ 
              let index = arg.value.match(/^queryArgsValues\[([0-9]+)\]$/)[1] - 1;
              let val = JSON.stringify(queryArgsValues[index])
              return `${val}`
          }
          return `"${arg.value}"`
      }
      else if(arg.type == "expr_list") {
          
          let code = [];
          arg.value.forEach(element => {
              code.push(`${whereCheckCode(element)}`)
          });

          return `{${code.join(',')}}`
      }
      else if(arg.type == "binary_expr") { 
          sqloperatorsTableUsedKeys.push(arg.operator)
          return `sqloperatorsTable["${arg.operator}"](${whereCheckCode(arg.left)}, ${whereCheckCode(arg.right)})`
      }
      else { 
          throw `"Not implement arg.type = "${arg.type}" in where section"` + JSON.stringify(arg)
      }
  }

  let whereCheck = `
  local function whereCheck(rawRow) 
      return true
  end`;
  if(ast.where) {
      whereCheck = `
      local function whereCheck(rawRow) 
          return ${whereCheckCode(ast.where)}
      end`;
  }


  // Prepare data for GROUP BY stamet 
  if(ast.groupby)
  {
      ast.groupby.forEach(element => { 
          let column = null;
          if(element.type == 'number'){
              let indexInSelect = element.value
              column = ast.columns[indexInSelect - 1]
          }
          else
          {
            throw "Not implement `group by` based on not numbers keys"
          }

          if(column.expr.type != 'column_ref') {
              throw "Not implement `group by` based on not column_ref"
          }

          groupByKeys.push(column.expr.column)
      });
  }

  // Prepare data for ORDER BY stamet  
  if(ast.orderby && ast.orderby.length)
  {
      ast.orderby.forEach(element =>{ 

          let column;
          let columnName;
          if(element.expr.type == 'number')
          { 
              column = ast.columns[element.expr.value - 1]
          }
          else
          {
              throw "Not implement `order by` based on not numbers keys"
          } 

          if(column.expr.type == "column_ref")
          {
              columnName = column.expr.column
          }
          else  
          {
              columnName = column.as
          }

          let compareFunc = '<'
          if(element.type == 'DESC')
          {
              compareFunc = '>'
          }
          orderByColumns += `
              if a["${columnName}"] ~= b["${columnName}"] then
                  return (a["${columnName}"] ${compareFunc} b["${columnName}"])  
              end
          `
      })

      orderByColumns = `table.sort(tFinal, function (a, b)
          ${orderByColumns} 
          return true
      end)`
  }

  // Prepare data for LIMIT stamet  
  let limit = {start:0, limit:0}
  if(ast.limit && ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value)
  {
      limit.start = ast.limit.value[0].value 
  }
  if(ast.limit && ast.limit.value && ast.limit.value[1] && ast.limit.value[1].value)
  {
      limit.limit = ast.limit.value[1].value 
  }
  if(!limit.limit && limit.start){
      limit.limit = limit.start 
      limit.start = 0
  }

  const body = `
  local tFinal = {}
  local sqloperatorsTable = {}
  local ast = cjson.decode('${JSON.stringify(ast)}')
  local sqloperatorsTableUsedKeys = cjson.decode('${JSON.stringify(sqloperatorsTableUsedKeys)}')
  local queryRowLimit = cjson.decode('${JSON.stringify(limit)}')
  local groupByKeys = cjson.decode('${JSON.stringify(groupByKeys)}')
  local columnsToNames = cjson.decode('${JSON.stringify(columnsToNames)}')
  local aggrFuncsToNames = cjson.decode('${JSON.stringify(aggrFuncsToNames)}')
  local queryArgsValues = cjson.decode('${JSON.stringify(queryArgsValues)}')


  -- Checking the condition in the where clause
  ${whereCheck}

  ${getLuaBody()}

  -- order by
  ${orderByColumns}

  tFinal = toSelectFormat(tFinal)
  tFinal = catByLimit(tFinal)

  return cjson.encode(tFinal) 
  ` 

  return body
}

class RedisSQL { 
  parse(sqlQuery, values){
    const parser = new Parser();
    
    values.forEach((element, index)=> {
      sqlQuery = sqlQuery.replace("?", "'queryArgsValues["+(index+1)+"]'") 
    });
    
    const {ast} = parser.parse(sqlQuery); 
    return sqlToLua(sqlQuery, values, ast)  
  } 
}

module.exports = {RedisSQL};