const readline = require('readline');
const fs = require('fs');
const redis = require('redis');

let conn = redis.createClient();
 
const readInterface = readline.createInterface({
  input: fs.createReadStream('/home/victor/GIT/CubJS_test/Donors.csv'),
  output: process.stdout,
  console: false
});

let count = 0
readInterface.on('line', function(line) {
  count++;
  if(count <= 1) {
    return;
  }

  let datarow = line.split(",")
  if(!datarow || !datarow.length)
  {
    return;
  }
  let obj = {
    "Donor ID": datarow[0],
    "Donor City": datarow[1],
    "Donor State": datarow[2],
    "Donor Is Teacher": datarow[3],
    "Donor Zip": datarow[4],
  }

  console.log(datarow, obj);
  let key = 'donors_' + obj["Donor ID"] . replace(/[\n\t\r ]/, "_")

  conn.set(key, JSON.stringify(obj))
});