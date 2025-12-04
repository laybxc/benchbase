SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

DROP TABLE IF EXISTS usertable_w;
DROP TABLE IF EXISTS usertable_r;
CREATE TABLE usertable_w (
    ycsb_key int PRIMARY KEY,
    field1   varchar(100),
    field2   varchar(100),
    field3   varchar(100),
    field4   varchar(100),
    field5   varchar(100),
    field6   varchar(100),
    field7   varchar(100),
    field8   varchar(100),
    field9   varchar(100),
    field10  varchar(100)
)ENGINE=rocksdb;
-- DATA DIRECTORY = '/mnt/hdd_data/ycsb_w'

CREATE TABLE usertable_r (
   ycsb_key int PRIMARY KEY,
   field1   varchar(100),
   field2   varchar(100),
   field3   varchar(100),
   field4   varchar(100),
   field5   varchar(100),
   field6   varchar(100),
   field7   varchar(100),
   field8   varchar(100),
   field9   varchar(100),
   field10  varchar(100)
)ENGINE=innodb;
-- DATA DIRECTORY = '/mnt/hdd_data/ycsb_r';

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;