# DMON for Samsung 

## What is ?

DMON is a data monetization project at B2W for Samsung


## Is it ready?

Yup on PROD at : https://dmon-api.atlas.b2w/v1/dmon

For a compete follow up please check the project JIRA :

http://jira.b2w/secure/RapidBoard.jspa?rapidView=1238&projectKey=DMON&view=detail

## How to test it ?

For getting an authorization Token : 

curl -X POST http://dmon-api.atlas.b2w/v1/dmon/token -H "Authorization: Basic [Base64 encrypted samsung user password]"

and then do a sales check with initial and final dates using the Token returned in the previous step :

curl -X GET "http://dmon-api.atlas.b2w/v1/dmon/sale?initialDate=2017-10-10&finalDate=2017-12-12" -H "X-B2W-TOKEN: [encrypted token]"

On production be aware that a cache load is expected to run for about 10 minutes !
