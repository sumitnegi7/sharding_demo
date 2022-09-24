const app = require("express")();
const crypto = require("crypto");
const {Client} = require("pg");
const HashRing = require("hashring");

const hr = new HashRing();
hr.add("5440")
hr.add("5441")
hr.add("5442")

const clients = {
    "5440":new Client({
        "host":"localhost",
        "port":"5440",
        "user":"postgres",
        "password":"sumit123",
        "database":"postgres"
    }),
    "5441":new Client({
        "host":"localhost",
        "port":"5441",
        "user":"postgres",
        "password":"sumit123",
        "database":"postgres"
    }),
    "5442":new Client({
        "host":"localhost",
        "port":"5442",
        "user":"postgres",
        "password":"sumit123",
        "database":"postgres"
    }),
}

async function  connect(){
    await  clients["5440"].connect();
    await  clients["5441"].connect();
    await  clients["5442"].connect();
}
connect()

app.get("/:urlId", async(req,res) =>{
console.log(req.params.urlId)
    const urlId = req.params.urlId;
    const server= hr.get(urlId)
    console.log(server,"🔥 🔥 🔥 ") 
    try {
        const result = await clients[server].query("SELECT * FROM URL_TABLE WHERE URL_ID = $1",[urlId]);
        console.log(result)
        if(result.rowCount >0){
            res.send({
                "urlId":urlId,
                "url": result.rows[0],
                "server":server
            })
        }
     } catch (error) {
         console.error(error)
     }
})

app.post("/", async (req,res) =>{

    const url = req.query.url;
    const hash = crypto.createHash("sha256").update(url).digest("base64")
    const urlId = hash.substring(0,5);

    const server = hr.get(urlId)
    try {
       const d= await clients[server].query("INSERT INTO URL_TABLE(URL, URL_ID) VALUES ($1,$2)",[url,urlId]);
       console.log(d)
    } catch (error) {
        console.error(error)
    }

   

    res.send({
        "urlId":urlId,
        "url": url,
        "server":server
    })

})

app.listen(8000,()=> console.log("listening to 8000"))