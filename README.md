# Redis sql

Example: 

```js
const redis = require('redis');
const { RedisSQL } = require('redis-sql'); 

const sqlQuery = 'SELECT\
      `donors`.`Donor City` `donors__donor_city`, count(*) `donors__count`, sum(`donors`.`Donor ID`) `donors__sum`\
    FROM\
      ab_api_test.donors AS `donors`\
  WHERE (`donors`.`Donor ID` NOT IN (?, ?) OR `donors`.`Donor ID` IS NULL) GROUP BY 1 ORDER BY 2 ASC, 3 ASC LIMIT 10000'

const values = [1, 2]

const parser = new RedisSQL()
const luaQuery = parser.parse(sqlQuery, values)
const conn = redis.createClient();
 
conn.eval(luaQuery, 0, function(err, res) {
    if(err){
        console.error("redis lua error", err, res); 
    }else{ 
        res = JSON.parse(res) 
        if(!Array.isArray(res))
        {
            res = []
        }
        
        console.log("redis answer", res);
        resolve(res)
    }
})
```

## Data set

Allows you to perform sql queries on keys that contain a serialized json object. 

```
set donors_pkey_1 '{"Donor City":"Appomattox","Donor State":"Illinois","Donor Is Teacher":"Yes","Donor Zip":"602","Donor ID":"1"}'
set donors_pkey_2 '{"Donor City":"Appomattox","Donor State":"Illinois","Donor Is Teacher":"Yes","Donor Zip":"602","Donor ID":"2"}'
set donors_pkey_3 '{"Donor City":"Appomattox","Donor State":"Illinois","Donor Is Teacher":"Yes","Donor Zip":"602","Donor ID":"3"}'
set donors_pkey_4 '{"Donor City":"Appomattox","Donor State":"Illinois","Donor Is Teacher":"Yes","Donor Zip":"602","Donor ID":"4"}'
```