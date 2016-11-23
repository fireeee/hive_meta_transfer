# hive_meta_transfer
Transfer the hive metadata from one cluster to another

I wrote this simple python script because I did not find anything to do the job on the net.
I use [pyhive](https://github.com/dropbox/PyHive)
The idea is simple 
- iterate through hive databases and create those on the remote hive.  
- iterate through the hive tables of every database and create those on the remote hive.
- repair the tables with 'msck repair table'
