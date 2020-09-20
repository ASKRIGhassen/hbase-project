HBASE Shell commands 
> status
   -- 1 active master, 0 backup masters, 1 servers, 0 dead, 3.0000 average load
> whoami
  root (auth:SIMPLE)
      groups: root
> list
TABLE                                                                                                
employee                                                                                             
1 row(s)
Took 0.6897 seconds                                                                                  
=> ["employee"]
> Table creation : create 'census', 'personal', 'professional' 

> describe 'census'
> count 'census'
> put 'census', 1 ,'personal:name', 'Ghassen ASKRI'
# Retrieve Data 
 - scan 'census' command : fetch all data
 - scan 'census', {COLUMNS => ['personal:name'],LIMIT => 3, STARTROW =>"2"} !
 - get 'census', key
 - get 'census', key ,'columnFamily:column'
 
 #Delete data from a record
-- delete 'census', 1 , 'personal:marital_status'
#  disable 'employee' ==> remove the indexes and the change logs!
# drop 'employee'



